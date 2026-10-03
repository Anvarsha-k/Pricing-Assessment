// Command mockfeed is a synthetic upstream that speaks the live feed's
// protocol (auth, subscribeMarketPrice, marketPriceSnapshot/Update) and can
// inject the faults the engine must tolerate: high tick rates, large batched
// payloads, duplicates, out-of-order timestamps, price spikes and abrupt
// disconnects. It is used for load and fault-tolerance testing, since the live
// feed's rate and behaviour cannot be controlled.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"pricing-engine/internal/config"
)

var (
	addr       = flag.String("addr", ":9000", "listen address")
	apiKey     = flag.String("api-key", "", "required api_key (empty accepts any)")
	rate       = flag.Int("rate", 2000, "ticks per second across all symbols")
	batch      = flag.Int("batch", 1, "ticks per WebSocket message (JSON array when > 1)")
	dupRate    = flag.Float64("dup", 0.2, "probability a tick is re-sent unchanged")
	oooRate    = flag.Float64("ooo", 0.01, "probability a tick carries a timestamp up to 2s in the past")
	spikeRate  = flag.Float64("spike", 0.0005, "probability of a +-30% bad tick")
	dropEvery  = flag.Duration("drop-every", 0, "abruptly close each session after this long (0 = never)")
	timestamps = flag.Bool("timestamps", true, "include a timestamp field (ms)")
)

type instrument struct {
	symbol string
	px     float64
	spread float64
	dec    int
}

func instruments(symbols []string) []*instrument {
	out := make([]*instrument, 0, len(symbols))
	for _, s := range symbols {
		in := &instrument{symbol: s, px: 1.1, dec: 5}
		switch {
		case s == "XAUUSD":
			in.px, in.dec = 4100, 2
		case s == "XAGUSD":
			in.px, in.dec = 48, 3
		case s == "BTCUSD":
			in.px, in.dec = 84000, 2
		case s == "ETHUSD":
			in.px, in.dec = 3200, 2
		case strings.HasSuffix(s, "USD") && !strings.HasPrefix(s, "EUR") && !strings.HasPrefix(s, "GBP") &&
			!strings.HasPrefix(s, "AUD") && !strings.HasPrefix(s, "NZD"):
			in.px, in.dec = 1+rand.Float64()*500, 4
		case strings.HasSuffix(s, "JPY"):
			in.px, in.dec = 100+rand.Float64()*90, 3
		}
		in.spread = in.px * 0.0002
		out = append(out, in)
	}
	return out
}

type wireTick struct {
	Event     string  `json:"event"`
	Symbol    string  `json:"symbol"`
	Bid       rawNum  `json:"bid"`
	Ask       rawNum  `json:"ask"`
	High      float64 `json:"high"` // junk: the engine must ignore these
	Low       float64 `json:"low"`
	Pct       float64 `json:"percentage"`
	Timestamp int64   `json:"timestamp,omitempty"`
}

// rawNum renders a price with the instrument's fixed decimals, like the live feed.
type rawNum string

func (r rawNum) MarshalJSON() ([]byte, error) { return []byte(r), nil }

func price(v float64, dec int) rawNum { return rawNum(strconv.FormatFloat(v, 'f', dec, 64)) }

func (in *instrument) tick(event string, rng *rand.Rand) wireTick {
	px := in.px
	if event == "marketPriceUpdate" && rng.Float64() < *spikeRate {
		px *= 1 + 0.3*float64(1-2*rng.IntN(2)) // bad tick; the walk itself is unaffected
	}
	t := wireTick{
		Event: event, Symbol: in.symbol,
		Bid: price(px, in.dec), Ask: price(px+in.spread, in.dec),
		High: 999999, Low: 0, Pct: 99.99,
	}
	if *timestamps {
		t.Timestamp = time.Now().UnixMilli()
		if rng.Float64() < *oooRate {
			t.Timestamp -= rng.Int64N(2000)
		}
	}
	return t
}

func (in *instrument) step(rng *rand.Rand) {
	in.px *= 1 + rng.NormFloat64()*0.0001
	in.px = math.Max(in.px, math.Pow(10, -float64(in.dec)))
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func serve(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	var auth struct {
		Action string `json:"action"`
		APIKey string `json:"api_key"`
	}
	if conn.ReadJSON(&auth) != nil || auth.Action != "auth" {
		return
	}
	ok := *apiKey == "" || auth.APIKey == *apiKey
	conn.WriteJSON(map[string]any{"event": "authenticated", "success": ok, "message": map[bool]string{true: "ok", false: "invalid credentials"}[ok]})
	if !ok {
		return
	}
	var sub struct {
		Action  string   `json:"action"`
		Symbols []string `json:"symbols"`
	}
	if conn.ReadJSON(&sub) != nil || sub.Action != "subscribeMarketPrice" {
		return
	}
	if len(sub.Symbols) == 0 {
		sub.Symbols = config.DefaultSymbols
	}
	log.Printf("session from %s: %d symbols, rate=%d/s batch=%d", r.RemoteAddr, len(sub.Symbols), *rate, *batch)
	go func() { // discard client frames, answer pings
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
	ins := instruments(sub.Symbols)
	for _, in := range ins {
		if conn.WriteJSON(in.tick("marketPriceSnapshot", rng)) != nil {
			return
		}
	}

	const interval = 10 * time.Millisecond
	perInterval := max(1, *rate/int(time.Second/interval))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var deadline <-chan time.Time
	if *dropEvery > 0 {
		deadline = time.After(*dropEvery)
	}
	var pending []wireTick
	var last = map[string]wireTick{}
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		var b []byte
		if *batch > 1 {
			b, _ = json.Marshal(pending)
		} else {
			b, _ = json.Marshal(pending[0])
		}
		pending = pending[:0]
		return conn.WriteMessage(websocket.TextMessage, b)
	}
	for {
		select {
		case <-deadline:
			log.Printf("dropping session from %s (fault injection)", r.RemoteAddr)
			return
		case <-ticker.C:
		}
		for i := 0; i < perInterval; i++ {
			in := ins[rng.IntN(len(ins))]
			var t wireTick
			if prev, ok := last[in.symbol]; ok && rng.Float64() < *dupRate {
				t = prev
			} else {
				in.step(rng)
				t = in.tick("marketPriceUpdate", rng)
				last[in.symbol] = t
			}
			pending = append(pending, t)
			if len(pending) >= *batch {
				if flush() != nil {
					return
				}
			}
		}
	}
}

func main() {
	flag.Parse()
	http.HandleFunc("/ws/algo", serve)
	http.HandleFunc("/ws", serve)
	log.Printf("mock feed on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
