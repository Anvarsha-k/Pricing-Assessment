package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"pricing-engine/internal/model"
	"pricing-engine/internal/window"
	"pricing-engine/pkg/protocol"
)

type noCandles struct{}

func (noCandles) Candles(context.Context, string, time.Duration, int) ([]window.Bucket, error) {
	return nil, nil
}

func setup(t *testing.T, symbols []string, mod func(*Config)) (*Hub, *model.Registry, string) {
	t.Helper()
	reg := model.NewRegistry(symbols)
	cfg := Config{
		MaxFrameBytes: 8192, WriteTimeout: time.Second, PingInterval: time.Minute, PongWait: time.Minute,
		MaxClients: 100, MaxConnsPerIP: 100, ControlRate: 100, ControlBurst: 100, Resolution: time.Minute,
	}
	if mod != nil {
		mod(&cfg)
	}
	hub := NewHub(reg, cfg.MaxFrameBytes, 0)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go hub.Run(ctx)
	srv := httptest.NewServer(New(cfg, hub, noCandles{}, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	t.Cleanup(srv.Close)
	return hub, reg, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

func quote(reg *model.Registry, sym string, bid float64) model.Quote {
	id, _ := reg.ID(sym)
	return model.Quote{
		Quote:  protocol.Quote{ID: id, Decimals: 5, Bid: bid, Ask: bid + 0.0002, High: bid + 0.01, Low: bid - 0.01, TimeMs: 1},
		Symbol: sym, IngestNs: time.Now().UnixNano(),
	}
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	return c
}

func readText(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	typ, b, err := c.ReadMessage()
	if err != nil || typ != websocket.TextMessage {
		t.Fatalf("read text: type=%d err=%v", typ, err)
	}
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

// expect skips text messages until one of type typ arrives.
func expect(t *testing.T, c *websocket.Conn, typ string) map[string]any {
	t.Helper()
	for {
		if m := readText(t, c); m["t"] == typ {
			return m
		}
	}
}

func readFrame(t *testing.T, c *websocket.Conn, dec *protocol.BinaryDecoder) protocol.Frame {
	t.Helper()
	for {
		typ, b, err := c.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if typ == websocket.TextMessage {
			continue
		}
		var f protocol.Frame
		if err := dec.Decode(b, &f); err != nil {
			t.Fatal(err)
		}
		return f
	}
}

func TestBinarySnapshotThenDeltas(t *testing.T) {
	hub, reg, url := setup(t, []string{"EURUSD", "GBPUSD"}, nil)
	hub.Publish(quote(reg, "EURUSD", 1.1))
	hub.Publish(quote(reg, "GBPUSD", 1.3))

	c := dial(t, url+"?symbols=EURUSD")
	if m := readText(t, c); m["t"] != "welcome" || m["format"] != "binary" {
		t.Fatalf("bad welcome %v", m)
	}
	if m := readText(t, c); m["t"] != "symbols" || len(m["symbols"].([]any)) != 2 {
		t.Fatalf("bad symbol table %v", m)
	}
	if m := readText(t, c); m["t"] != "subscribed" {
		t.Fatalf("bad ack %v", m)
	}
	dec := &protocol.BinaryDecoder{}
	f := readFrame(t, c, dec)
	if len(f.Quotes) != 1 || f.Quotes[0].Bid != 1.1 || f.Seq != 1 {
		t.Fatalf("snapshot frame %+v", f)
	}

	hub.Publish(quote(reg, "GBPUSD", 1.31)) // not subscribed
	hub.Publish(quote(reg, "EURUSD", 1.10001))
	f = readFrame(t, c, dec)
	if len(f.Quotes) != 1 || f.Quotes[0].Bid != 1.10001 || f.Seq != 2 {
		t.Fatalf("delta frame %+v", f)
	}
}

func TestFramesStayUnderLimit(t *testing.T) {
	var syms []string
	for i := 0; i < 2000; i++ {
		syms = append(syms, fmt.Sprintf("SYM%04d", i))
	}
	hub, reg, url := setup(t, syms, func(c *Config) { c.MaxFrameBytes = 1024 })
	for i, s := range syms {
		hub.Publish(quote(reg, s, 1+float64(i)))
	}
	for _, format := range []string{"binary", "json"} {
		c := dial(t, url+"?format="+format+"&symbols=*")
		got := 0
		for got < len(syms) {
			typ, b, err := c.ReadMessage()
			if err != nil {
				t.Fatalf("%s: %v after %d records", format, err, got)
			}
			if len(b) > 1024 {
				t.Fatalf("%s frame of %d bytes exceeds the 1024 limit", format, len(b))
			}
			if typ == websocket.BinaryMessage {
				var f protocol.Frame
				(&protocol.BinaryDecoder{}).Decode(b, &f) // fresh decoder: only counting full records
				got += len(f.Quotes)
			} else if strings.HasPrefix(string(b), `{"t":"u"`) {
				var f struct{ D []any }
				json.Unmarshal(b, &f)
				got += len(f.D)
			}
		}
	}
}

func TestJSONFormatAndUnknownSymbol(t *testing.T) {
	hub, reg, url := setup(t, []string{"EURUSD"}, nil)
	hub.Publish(quote(reg, "EURUSD", 1.1))
	c := dial(t, url+"?format=json")
	expect(t, c, "symbols")
	c.WriteJSON(map[string]any{"op": "subscribe", "symbols": []string{"EURUSD", "NOPE"}})
	ack := expect(t, c, "subscribed")
	if fmt.Sprint(ack["unknown"]) != "[NOPE]" {
		t.Fatalf("ack %v", ack)
	}
	u := readText(t, c)
	d := u["d"].([]any)[0].(map[string]any)
	if u["t"] != "u" || d["s"] != "EURUSD" || d["b"] != 1.1 {
		t.Fatalf("update %v", u)
	}
}

func TestControlRateLimitDisconnects(t *testing.T) {
	_, _, url := setup(t, []string{"EURUSD"}, func(c *Config) { c.ControlRate, c.ControlBurst = 1, 1 })
	c := dial(t, url)
	for i := 0; i < maxRateViolations+5; i++ {
		if c.WriteJSON(map[string]string{"op": "ping"}) != nil {
			break
		}
	}
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			if !websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
				t.Fatalf("expected policy-violation close, got %v", err)
			}
			return
		}
	}
}

func TestServesDashboard(t *testing.T) {
	_, _, url := setup(t, []string{"EURUSD"}, nil)
	base := "http" + strings.TrimSuffix(strings.TrimPrefix(url, "ws"), "/ws")
	for path, want := range map[string]string{
		"/":        "<title>Pricing Engine</title>",
		"/app.js":  "makeDecoder",
		"/app.css": "--accent",
	} {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), want) {
			t.Errorf("GET %s: status %d, missing %q", path, resp.StatusCode, want)
		}
	}
}

func TestStatusAndNewSymbolAnnouncements(t *testing.T) {
	hub, reg, url := setup(t, []string{"EURUSD"}, nil)
	c := dial(t, url+"?symbols=*")
	expect(t, c, "subscribed")
	hub.SetUpstream(true)
	if m := readText(t, c); m["t"] != "status" || m["upstream"] != true {
		t.Fatalf("status %v", m)
	}
	reg.Intern("NEWSYM")
	hub.Publish(quote(reg, "NEWSYM", 5))
	m := readText(t, c)
	if m["t"] != "symbols" || !strings.Contains(fmt.Sprint(m["symbols"]), "NEWSYM") {
		t.Fatalf("announcement %v", m)
	}
	if f := readFrame(t, c, &protocol.BinaryDecoder{}); f.Quotes[0].Bid != 5 {
		t.Fatalf("frame %+v", f)
	}
}
