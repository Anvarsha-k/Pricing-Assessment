package feed

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"pricing-engine/internal/model"
)

func TestDecodeShapes(t *testing.T) {
	cases := map[string]int{
		`{"event":"marketPriceUpdate","symbol":"EURUSD","bid":1.1,"ask":1.2}`:              1,
		`[{"symbol":"A","bid":1,"ask":2},{"symbol":"B","bid":1,"ask":2}]`:                  2,
		`{"data":[{"symbol":"A","buy":2,"sell":1},{"symbol":"B","buy":2,"sell":1}]}`:       2,
		`{"event":"authenticated","success":true,"message":"connected auth successfully"}`: 1,
	}
	for in, want := range cases {
		ms, err := decode([]byte(in))
		if err != nil || len(ms) != want {
			t.Errorf("decode(%s) = %d msgs, %v; want %d", in, len(ms), err, want)
		}
	}
}

func TestTickConversion(t *testing.T) {
	ms, _ := decode([]byte(`{"event":"marketPriceUpdate","symbol":"BTCUSD","bid":84727.43000,"ask":84848.67,
		"high":1,"low":1,"percentage":99,"low_24h":1,"high_24h":1,"change_24h":1,"timestamp":1791045400856}`))
	tk, ok := ms[0].tick(123)
	if !ok {
		t.Fatal("tick rejected")
	}
	want := model.Tick{Symbol: "BTCUSD", Bid: 84727.43, Ask: 84848.67, Decimals: 5, SrcMs: 1791045400856, RecvNs: 123}
	if tk != want {
		t.Fatalf("got %+v, want %+v", tk, want)
	}
	// buy/sell aliases: buy is the ask side.
	ms, _ = decode([]byte(`{"symbol":"X","buy":2.5,"sell":2.25}`))
	if tk, _ := ms[0].tick(0); tk.Bid != 2.25 || tk.Ask != 2.5 {
		t.Fatalf("buy/sell mapping wrong: %+v", tk)
	}
	if ms, err := decode([]byte(`{"symbol":"X","bid":"oops","ask":1}`)); err == nil {
		t.Fatalf("non-numeric price decoded: %+v", ms)
	}
	ms, _ = decode([]byte(`{"symbol":"X","bid":1}`))
	if _, ok := ms[0].tick(0); ok {
		t.Fatal("tick without ask accepted")
	}
}

func TestParseTimestamp(t *testing.T) {
	const ms = int64(1791045400856)
	for in, want := range map[string]int64{
		`1791045400856`:              ms,
		`1791045400.856`:             ms,
		`1791045400856000`:           ms,
		`1791045400856000000`:        ms,
		`"1791045400856"`:            ms,
		`"2026-10-03T16:36:40.856Z"`: time.Date(2026, 10, 3, 16, 36, 40, 856e6, time.UTC).UnixMilli(),
		`null`:                       0,
		`"garbage"`:                  0,
	} {
		if got := parseTimestamp(json.RawMessage(in)); got != want {
			t.Errorf("parseTimestamp(%s) = %d, want %d", in, got, want)
		}
	}
}

// fakeUpstream authenticates, sends one price per session, then drops the
// connection to force a reconnect.
func fakeUpstream(t *testing.T, sessions *atomic.Int32, reject bool) *httptest.Server {
	up := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		n := sessions.Add(1)
		var auth map[string]string
		if c.ReadJSON(&auth) != nil || auth["action"] != "auth" {
			return
		}
		if reject || auth["api_key"] != "k" {
			c.WriteJSON(map[string]any{"event": "authenticated", "success": false, "message": "bad key"})
			return
		}
		c.WriteJSON(map[string]any{"event": "authenticated", "success": true})
		var sub map[string]any
		if c.ReadJSON(&sub) != nil || sub["action"] != "subscribeMarketPrice" {
			return
		}
		c.WriteMessage(websocket.TextMessage, []byte(
			`{"event":"marketPriceSnapshot","symbol":"EURUSD","bid":1.1,"ask":1.2}`))
		if n == 1 {
			return // drop the first session abruptly
		}
		time.Sleep(time.Second)
	}))
}

func testClient(url string, out chan model.Tick, status func(bool)) *Client {
	return New(Config{
		URL: "ws" + strings.TrimPrefix(url, "http"), APIKey: "k", Secret: "s", Symbols: []string{"EURUSD"},
		DialTimeout: time.Second, AuthTimeout: time.Second, IdleTimeout: 2 * time.Second, PingInterval: time.Second,
		BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond, MaxMessageBytes: 1 << 20,
	}, out, status, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestClientReconnectsAndResubscribes(t *testing.T) {
	var sessions atomic.Int32
	srv := fakeUpstream(t, &sessions, false)
	defer srv.Close()
	out := make(chan model.Tick, 8)
	var ups atomic.Int32
	c := testClient(srv.URL, out, func(up bool) {
		if up {
			ups.Add(1)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go c.Run(ctx)
	for i := 0; i < 2; i++ {
		select {
		case tk := <-out:
			if tk.Symbol != "EURUSD" || tk.Bid != 1.1 {
				t.Fatalf("bad tick %+v", tk)
			}
		case <-ctx.Done():
			t.Fatalf("got %d ticks before timeout", i)
		}
	}
	if sessions.Load() < 2 || ups.Load() < 2 {
		t.Fatalf("sessions=%d subscribed=%d, want a reconnect", sessions.Load(), ups.Load())
	}
}

func TestClientAuthFailureBacksOff(t *testing.T) {
	var sessions atomic.Int32
	srv := fakeUpstream(t, &sessions, true)
	defer srv.Close()
	c := testClient(srv.URL, make(chan model.Tick, 1), func(bool) {})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	c.Run(ctx)
	// With BackoffMax=50ms applied on every auth failure, 300ms allows ~6
	// attempts, not the dozens a tight retry loop would make.
	if n := sessions.Load(); n < 2 || n > 8 {
		t.Fatalf("made %d auth attempts in 300ms", n)
	}
}
