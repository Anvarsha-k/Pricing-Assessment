// Package feed maintains the upstream market data session: connect,
// authenticate, subscribe, and forward ticks to the engine, reconnecting with
// jittered exponential backoff whenever the session fails or goes silent.
package feed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"pricing-engine/internal/metrics"
	"pricing-engine/internal/model"
)

type Config struct {
	URL             string
	APIKey          string
	Secret          string
	Symbols         []string
	DialTimeout     time.Duration
	AuthTimeout     time.Duration
	IdleTimeout     time.Duration // no frame (data or pong) for this long => reconnect
	PingInterval    time.Duration
	BackoffMin      time.Duration
	BackoffMax      time.Duration
	MaxMessageBytes int64
}

// A session that stayed up this long resets the backoff.
const stableSession = time.Minute

var errAuth = errors.New("upstream rejected credentials")

// closeSessionActive is the upstream close code for "already have an active
// session": one session per API key, and a dropped one lingers for a while.
const closeSessionActive = 4003

type Client struct {
	cfg      Config
	out      chan<- model.Tick
	onStatus func(up bool)
	log      *slog.Logger
	dialer   websocket.Dialer
}

// New creates a feed client that writes ticks to out and reports session
// state changes to onStatus.
func New(cfg Config, out chan<- model.Tick, onStatus func(up bool), log *slog.Logger) *Client {
	return &Client{
		cfg:      cfg,
		out:      out,
		onStatus: onStatus,
		log:      log,
		dialer: websocket.Dialer{
			Proxy:             http.ProxyFromEnvironment,
			HandshakeTimeout:  cfg.DialTimeout,
			EnableCompression: true,
			ReadBufferSize:    64 << 10,
		},
	}
}

// Run keeps a session alive until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	backoff := c.cfg.BackoffMin
	for {
		start := time.Now()
		err := c.session(ctx)
		metrics.UpstreamConnected.Set(0)
		c.onStatus(false)
		if ctx.Err() != nil {
			return
		}
		metrics.UpstreamReconnects.Inc()
		if time.Since(start) > stableSession {
			backoff = c.cfg.BackoffMin
		}
		// Jitter in [backoff/2, backoff) so a fleet of engines does not
		// reconnect in lockstep after an upstream outage.
		wait := backoff/2 + rand.N(backoff/2+1)
		if errors.Is(err, errAuth) {
			wait = c.cfg.BackoffMax // retrying bad credentials quickly helps no one
		}
		if ce := (*websocket.CloseError)(nil); errors.As(err, &ce) && ce.Code == closeSessionActive {
			c.log.Warn("upstream still holds a previous session for this API key; retrying until it expires")
		}
		c.log.Warn("upstream session ended", "err", err, "retry_in", wait.Round(time.Millisecond))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, c.cfg.BackoffMax)
	}
}

func (c *Client) session(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, c.cfg.DialTimeout)
	conn, _, err := c.dialer.DialContext(dialCtx, c.cfg.URL, nil)
	cancel()
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	// On shutdown, say goodbye with a close frame before dropping the socket:
	// upstream allows one session per API key and otherwise holds the old one
	// until it times out, rejecting the restarted engine with close 4003.
	// Closing the socket is also the only way to interrupt a blocked read.
	stop := context.AfterFunc(ctx, func() {
		conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"),
			time.Now().Add(time.Second))
		conn.Close()
	})
	defer stop()
	conn.SetReadLimit(c.cfg.MaxMessageBytes)

	if err := c.send(conn, map[string]any{"action": "auth", "api_key": c.cfg.APIKey, "secret": c.cfg.Secret}); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}
	if err := c.awaitAuth(ctx, conn); err != nil {
		return err
	}
	if err := c.send(conn, map[string]any{"action": "subscribeMarketPrice", "symbols": c.cfg.Symbols}); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}
	c.log.Info("upstream subscribed", "url", c.cfg.URL, "symbols", len(c.cfg.Symbols))
	metrics.UpstreamConnected.Set(1)
	c.onStatus(true)

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(c.cfg.IdleTimeout))
	})
	done := make(chan struct{})
	defer close(done)
	go c.keepalive(conn, done)

	for {
		if err := conn.SetReadDeadline(time.Now().Add(c.cfg.IdleTimeout)); err != nil {
			return err
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		c.handle(ctx, data, time.Now().UnixNano())
	}
}

func (c *Client) awaitAuth(ctx context.Context, conn *websocket.Conn) error {
	if err := conn.SetReadDeadline(time.Now().Add(c.cfg.AuthTimeout)); err != nil {
		return err
	}
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("awaiting auth response: %w", err)
		}
		msgs, err := decode(data)
		if err != nil {
			c.log.Warn("undecodable upstream message", "err", err)
			continue
		}
		for _, m := range msgs {
			if m.Event == "authenticated" || (m.Success != nil && m.Symbol == "") {
				metrics.UpstreamMessages.WithLabelValues("authenticated").Inc()
				if m.Success != nil && *m.Success {
					c.log.Info("upstream authenticated", "message", m.Message)
					return nil
				}
				return fmt.Errorf("%w: %s", errAuth, m.Message)
			}
		}
		c.handle(ctx, data, time.Now().UnixNano())
	}
}

func (c *Client) handle(ctx context.Context, data []byte, recvNs int64) {
	metrics.UpstreamBytes.Add(float64(len(data)))
	metrics.UpstreamMessageBytes.Observe(float64(len(data)))
	start := time.Now()
	msgs, err := decode(data)
	metrics.UpstreamDecodeSeconds.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.UpstreamMessages.WithLabelValues("undecodable").Inc()
		c.log.Warn("undecodable upstream message", "err", err, "bytes", len(data))
		return
	}
	for i := range msgs {
		m := &msgs[i]
		switch m.Event {
		case "marketPriceUpdate", "marketPriceSnapshot", "":
			t, ok := m.tick(recvNs)
			if !ok {
				metrics.UpstreamMessages.WithLabelValues("malformed").Inc()
				continue
			}
			metrics.UpstreamMessages.WithLabelValues(eventLabel(m.Event)).Inc()
			select {
			case c.out <- t:
			case <-ctx.Done():
				return
			}
		default:
			metrics.UpstreamMessages.WithLabelValues("other").Inc()
			c.log.Info("upstream event", "event", m.Event, "message", m.Message)
		}
	}
}

func eventLabel(ev string) string {
	if ev == "" {
		return "price"
	}
	return ev
}

func (c *Client) send(conn *websocket.Conn, v any) error {
	if err := conn.SetWriteDeadline(time.Now().Add(c.cfg.DialTimeout)); err != nil {
		return err
	}
	return conn.WriteJSON(v)
}

// keepalive pings upstream so a half-open TCP connection is detected by the
// read deadline even when the market is quiet.
func (c *Client) keepalive(conn *websocket.Conn, done <-chan struct{}) {
	t := time.NewTicker(c.cfg.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return
			}
		}
	}
}
