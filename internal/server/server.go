package server

import (
	"compress/flate"
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"pricing-engine/internal/metrics"
	"pricing-engine/internal/window"
)

// webFS holds the dashboard (index.html, app.js, app.css), compiled into the
// binary so the image needs no static files.
//
//go:embed web
var webFS embed.FS

type Config struct {
	MaxFrameBytes int
	WriteTimeout  time.Duration // a client that cannot take a frame this fast is dropped
	PingInterval  time.Duration
	PongWait      time.Duration
	MaxClients    int
	MaxConnsPerIP int
	ControlRate   float64 // control messages per second per client
	ControlBurst  int
	Resolution    time.Duration  // base candle resolution
	Info          map[string]any // static fields added to the welcome message
}

// CandleSource serves OHLC candles (implemented by the engine).
type CandleSource interface {
	Candles(ctx context.Context, symbol string, res time.Duration, limit int) ([]window.Bucket, error)
}

type Server struct {
	cfg      Config
	hub      *Hub
	candles  CandleSource
	log      *slog.Logger
	upgrader websocket.Upgrader

	mu    sync.Mutex
	perIP map[string]int
	total int
}

func New(cfg Config, hub *Hub, candles CandleSource, log *slog.Logger) *Server {
	return &Server{
		cfg:     cfg,
		hub:     hub,
		candles: candles,
		log:     log,
		perIP:   make(map[string]int),
		upgrader: websocket.Upgrader{
			ReadBufferSize:    4 << 10,
			WriteBufferSize:   16 << 10,
			EnableCompression: true,
			// Read-only public market data: any origin may connect.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleWS)
	mux.HandleFunc("GET /api/snapshot", s.handleSnapshot)
	mux.HandleFunc("GET /api/candles", s.handleCandles)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", s.handleReady)
	static, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err) // the embedded directory is fixed at compile time
	}
	mux.Handle("GET /", http.FileServerFS(static))
	return mux
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	format := q.Get("format")
	switch format {
	case "":
		format = "binary"
	case "binary", "json":
	default:
		http.Error(w, "format must be binary or json", http.StatusBadRequest)
		return
	}
	throttle := 0
	if v := q.Get("throttle_ms"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 60_000 {
			http.Error(w, "throttle_ms must be between 0 and 60000", http.StatusBadRequest)
			return
		}
		throttle = n
	}
	ip := clientIP(r)
	if reason := s.admit(ip); reason != "" {
		metrics.ClientRejects.WithLabelValues(reason).Inc()
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return
	}
	defer s.release(ip)

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already replied
	}
	// Deflate pays off for verbose JSON; binary frames are already dense and
	// tiny, so compressing them would mostly add latency.
	conn.EnableWriteCompression(format == "json")
	conn.SetCompressionLevel(flate.BestSpeed)

	c := newClient(s.hub, conn, format, &s.cfg, s.log)
	c.setThrottle(throttle)
	welcome := map[string]any{"t": "welcome", "format": format, "max_frame": s.cfg.MaxFrameBytes}
	for k, v := range s.cfg.Info {
		welcome[k] = v
	}
	s.hub.register(c, welcome)
	if syms := q.Get("symbols"); syms != "" {
		c.subscribe(strings.Split(syms, ","))
	}
	metrics.Clients.Inc()
	defer metrics.Clients.Dec()

	go c.writeLoop()
	c.readLoop()
	c.close()
	s.hub.unregister(c)
}

// admit enforces the global and per-IP connection limits.
func (s *Server) admit(ip string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.total >= s.cfg.MaxClients {
		return "max_clients"
	}
	if s.perIP[ip] >= s.cfg.MaxConnsPerIP {
		return "max_conns_per_ip"
	}
	s.total++
	s.perIP[ip]++
	return ""
}

func (s *Server) release(ip string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.total--
	if s.perIP[ip]--; s.perIP[ip] <= 0 {
		delete(s.perIP, ip)
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type quoteJSON struct {
	Symbol    string  `json:"symbol"`
	Buy       float64 `json:"buy"`
	Sell      float64 `json:"sell"`
	High24h   float64 `json:"high_24h"`
	Low24h    float64 `json:"low_24h"`
	Change24h float64 `json:"change_24h"`
	Decimals  uint8   `json:"decimals"`
	Timestamp int64   `json:"timestamp"`
}

func (s *Server) handleSnapshot(w http.ResponseWriter, _ *http.Request) {
	qs := s.hub.Snapshot()
	out := make([]quoteJSON, len(qs))
	for i, q := range qs {
		out[i] = quoteJSON{
			Symbol: q.Symbol, Buy: q.Ask, Sell: q.Bid, High24h: q.High, Low24h: q.Low,
			Change24h: q.ChangePct, Decimals: q.Decimals, Timestamp: q.TimeMs,
		}
	}
	writeJSON(w, http.StatusOK, out)
}

type candleJSON struct {
	Time  int64   `json:"t"`
	Open  float64 `json:"o"`
	High  float64 `json:"h"`
	Low   float64 `json:"l"`
	Close float64 `json:"c"`
	Ticks uint32  `json:"n"`
}

func (s *Server) handleCandles(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	symbol := q.Get("symbol")
	if symbol == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "symbol is required"})
		return
	}
	res := s.cfg.Resolution
	if v := q.Get("interval"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < s.cfg.Resolution || d%s.cfg.Resolution != 0 || d > 24*time.Hour {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "interval must be a multiple of " + s.cfg.Resolution.String() + " up to 24h",
			})
			return
		}
		res = d
	}
	limit := 500
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 5000 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be 1..5000"})
			return
		}
		limit = n
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	bs, err := s.candles.Candles(ctx, symbol, res, limit)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	out := make([]candleJSON, len(bs))
	for i, b := range bs {
		out[i] = candleJSON{Time: b.Start, Open: b.Open, High: b.High, Low: b.Low, Close: b.Close, Ticks: b.Count}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	up := s.hub.Upstream()
	status := http.StatusOK
	if !up {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]bool{"upstream": up})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
