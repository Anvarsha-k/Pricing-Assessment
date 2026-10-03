package server

import (
	"encoding/json"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"

	"pricing-engine/internal/metrics"
	"pricing-engine/internal/model"
	"pricing-engine/pkg/protocol"
)

const (
	maxControlBytes      = 8 << 10
	maxSymbolsPerRequest = 256 // keeps the echoing ack well under the frame limit; "*" covers bulk use
	maxRateViolations    = 20
	ctrlQueue            = 256 // room for a chunked symbol table of a full registry plus replies
)

// subView is an immutable snapshot of a client's subscriptions, swapped
// atomically so the writer reads it without locking.
type subView struct {
	all bool
	ids []uint16
}

// Client is one downstream WebSocket connection. Exactly one goroutine reads
// (control messages) and one writes (control replies and update frames).
type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	enc    protocol.Encoder
	format string
	cfg    *Config
	log    *slog.Logger

	mu     sync.Mutex // guards subs, subAll, reset
	subs   map[uint16]bool
	subAll bool
	reset  []uint16 // ids that must be resent as full records
	view   atomic.Pointer[subView]

	sent []*model.Quote // writer only: last quote pointer sent per id

	wake      chan struct{} // subscription changes
	ctrl      chan []byte
	done      chan struct{}
	closeOnce sync.Once
	throttle  atomic.Int64 // minimum ns between update frames
	limiter   *rate.Limiter
}

func newClient(h *Hub, conn *websocket.Conn, format string, cfg *Config, log *slog.Logger) *Client {
	var enc protocol.Encoder = protocol.NewBinaryEncoder()
	if format == "json" {
		enc = protocol.NewJSONEncoder()
	}
	c := &Client{
		hub:     h,
		conn:    conn,
		enc:     enc,
		format:  format,
		cfg:     cfg,
		log:     log,
		subs:    make(map[uint16]bool),
		wake:    make(chan struct{}, 1),
		ctrl:    make(chan []byte, ctrlQueue),
		done:    make(chan struct{}),
		limiter: rate.NewLimiter(rate.Limit(cfg.ControlRate), cfg.ControlBurst),
	}
	c.view.Store(&subView{})
	return c
}

// sendCtrl queues a text message. A client that cannot keep up with the low
// volume of control messages is disconnected rather than buffered.
func (c *Client) sendCtrl(msg []byte) {
	select {
	case c.ctrl <- msg:
	default:
		metrics.ClientRejects.WithLabelValues("ctrl_overflow").Inc()
		c.close()
	}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		c.conn.Close()
	})
}

func (c *Client) writeLoop() {
	defer c.close()
	ping := time.NewTicker(c.cfg.PingInterval)
	defer ping.Stop()
	var (
		qs   []outQuote
		last time.Time
	)
	for {
		if d := time.Duration(c.throttle.Load()); d > 0 {
			if wait := d - time.Since(last); wait > 0 {
				t := time.NewTimer(wait)
				select {
				case <-t.C:
				case <-c.done:
					t.Stop()
					return
				}
			}
		}
		changed := c.hub.changed() // before scanning, so no update is missed
		// Control messages queued before this point (welcome, acks, symbol
		// announcements) must reach the client before the data they describe.
		if !c.flushCtrl() {
			return
		}
		qs = c.collect(qs[:0])
		if len(qs) > 0 {
			if c.writeQuotes(qs) != nil {
				return
			}
			last = time.Now()
		}
		select {
		case <-c.done:
			return
		case msg := <-c.ctrl:
			if c.writeText(msg) != nil {
				return
			}
		case <-ping.C:
			if c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(c.cfg.WriteTimeout)) != nil {
				return
			}
		case <-changed:
		case <-c.wake:
		}
	}
}

// outQuote is a quote due to be sent. snapshot marks the first record of a
// symbol on this connection: it carries a price received earlier, so its age
// is not delivery latency and is kept out of the latency metric.
type outQuote struct {
	model.Quote
	snapshot bool
}

// collect appends the subscribed quotes that changed since they were last
// sent. It runs on the writer goroutine.
func (c *Client) collect(dst []outQuote) []outQuote {
	c.mu.Lock()
	reset := c.reset
	c.reset = nil
	c.mu.Unlock()
	for _, id := range reset {
		if int(id) < len(c.sent) {
			c.sent[id] = nil
		}
		c.enc.Forget(id)
	}

	v := c.view.Load()
	if v.all {
		for id, n := 0, c.hub.reg.Len(); id < n; id++ {
			dst = c.check(id, dst)
		}
		return dst
	}
	for _, id := range v.ids {
		dst = c.check(int(id), dst)
	}
	return dst
}

func (c *Client) check(id int, dst []outQuote) []outQuote {
	q := c.hub.quote(id)
	if q == nil {
		return dst
	}
	if id >= len(c.sent) {
		c.sent = append(c.sent, make([]*model.Quote, id+1-len(c.sent))...)
	}
	if c.sent[id] == q {
		return dst
	}
	snapshot := c.sent[id] == nil
	c.sent[id] = q
	return append(dst, outQuote{Quote: *q, snapshot: snapshot})
}

func (c *Client) flushCtrl() bool {
	for {
		select {
		case msg := <-c.ctrl:
			if c.writeText(msg) != nil {
				return false
			}
		default:
			return true
		}
	}
}

func (c *Client) writeText(msg []byte) error {
	c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	return c.conn.WriteMessage(websocket.TextMessage, msg)
}

// writeQuotes encodes qs into as many frames as needed to keep every frame
// under MaxFrameBytes.
func (c *Client) writeQuotes(qs []outQuote) error {
	enc := c.enc
	enc.Begin()
	oldest := int64(math.MaxInt64)
	for i := range qs {
		if enc.Count() > 0 && enc.Len()+enc.MaxRecord() > c.cfg.MaxFrameBytes {
			if err := c.writeFrame(enc.Finish(), oldest); err != nil {
				return err
			}
			enc.Begin()
			oldest = math.MaxInt64
		}
		q := &qs[i]
		if enc.Append(&q.Quote.Quote, q.Symbol, q.IngestNs/int64(time.Microsecond)) && !q.snapshot {
			oldest = min(oldest, q.IngestNs)
		}
	}
	if enc.Count() == 0 {
		return nil
	}
	return c.writeFrame(enc.Finish(), oldest)
}

func (c *Client) writeFrame(b []byte, oldestNs int64) error {
	typ := websocket.TextMessage
	if c.enc.Binary() {
		typ = websocket.BinaryMessage
	}
	c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	if err := c.conn.WriteMessage(typ, b); err != nil {
		return err
	}
	metrics.FramesSent.WithLabelValues(c.format).Inc()
	metrics.BytesSent.WithLabelValues(c.format).Add(float64(len(b)))
	metrics.FrameBytes.WithLabelValues(c.format).Observe(float64(len(b)))
	// Frames made only of snapshot records, and throttled clients (which wait
	// on purpose), say nothing about delivery latency.
	if oldestNs != math.MaxInt64 && c.throttle.Load() == 0 {
		metrics.Latency.Observe(float64(time.Now().UnixNano()-oldestNs) / 1e9)
	}
	return nil
}

type controlMsg struct {
	Op         string   `json:"op"`
	Symbols    []string `json:"symbols"`
	ThrottleMs *int     `json:"throttle_ms"`
}

func (c *Client) readLoop() {
	c.conn.SetReadLimit(maxControlBytes)
	c.conn.SetReadDeadline(time.Now().Add(c.cfg.PongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(c.cfg.PongWait))
	})
	violations := 0
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		c.conn.SetReadDeadline(time.Now().Add(c.cfg.PongWait))
		if !c.limiter.Allow() {
			metrics.ClientRejects.WithLabelValues("rate_limited").Inc()
			if violations++; violations > maxRateViolations {
				c.conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "rate limit exceeded"),
					time.Now().Add(time.Second))
				return
			}
			c.sendError("rate limit exceeded")
			continue
		}
		c.handleControl(data)
	}
}

func (c *Client) handleControl(data []byte) {
	var m controlMsg
	if err := json.Unmarshal(data, &m); err != nil {
		c.sendError("invalid JSON")
		return
	}
	if len(m.Symbols) > maxSymbolsPerRequest {
		c.sendError("too many symbols in one request")
		return
	}
	switch m.Op {
	case "subscribe":
		c.subscribe(m.Symbols)
	case "unsubscribe":
		c.unsubscribe(m.Symbols)
	case "throttle":
		if m.ThrottleMs == nil || *m.ThrottleMs < 0 || *m.ThrottleMs > 60_000 {
			c.sendError("throttle_ms must be between 0 and 60000")
			return
		}
		c.setThrottle(*m.ThrottleMs)
		c.sendCtrl(mustJSON(map[string]any{"t": "throttle", "throttle_ms": *m.ThrottleMs}))
	case "ping":
		c.sendCtrl([]byte(`{"t":"pong"}`))
	default:
		c.sendError("unknown op")
	}
}

// subscribe adds subscriptions; newly covered symbols are sent in full as a
// snapshot on the next write.
func (c *Client) subscribe(names []string) {
	var ok, unknown []string
	c.mu.Lock()
	for _, n := range names {
		if n == "*" {
			if !c.subAll {
				c.subAll = true
				for id, total := 0, c.hub.reg.Len(); id < total; id++ {
					if !c.subs[uint16(id)] {
						c.reset = append(c.reset, uint16(id))
					}
				}
			}
			ok = append(ok, n)
			continue
		}
		id, found := c.hub.reg.ID(n)
		if !found {
			unknown = append(unknown, n)
			continue
		}
		ok = append(ok, n)
		if !c.subs[id] {
			c.subs[id] = true
			if !c.subAll {
				c.reset = append(c.reset, id)
			}
		}
	}
	c.publishView()
	c.mu.Unlock()
	c.sendCtrl(mustJSON(map[string]any{"t": "subscribed", "symbols": ok, "unknown": unknown}))
	c.kick()
}

func (c *Client) unsubscribe(names []string) {
	var ok, unknown []string
	c.mu.Lock()
	for _, n := range names {
		if n == "*" {
			c.subAll = false
			clear(c.subs)
			ok = append(ok, n)
			continue
		}
		id, found := c.hub.reg.ID(n)
		if !found {
			unknown = append(unknown, n)
			continue
		}
		delete(c.subs, id)
		ok = append(ok, n)
	}
	c.publishView()
	c.mu.Unlock()
	c.sendCtrl(mustJSON(map[string]any{"t": "unsubscribed", "symbols": ok, "unknown": unknown}))
}

// publishView must be called with c.mu held.
func (c *Client) publishView() {
	v := &subView{all: c.subAll, ids: make([]uint16, 0, len(c.subs))}
	for id := range c.subs {
		v.ids = append(v.ids, id)
	}
	slices.Sort(v.ids)
	c.view.Store(v)
}

func (c *Client) kick() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Client) setThrottle(ms int) {
	c.throttle.Store(int64(time.Duration(ms) * time.Millisecond))
}

func (c *Client) sendError(msg string) {
	c.sendCtrl(mustJSON(map[string]any{"t": "error", "msg": msg}))
}
