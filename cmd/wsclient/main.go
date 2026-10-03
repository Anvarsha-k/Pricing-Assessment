// Command wsclient connects one or more clients to the pricing engine, decodes
// the stream and reports frame sizes, throughput and end-to-end latency.
//
//	wsclient -url ws://localhost:8080/ws -symbols '*' -duration 30s
//	wsclient -clients 500 -duration 60s            # fan-out load test
//	wsclient -v -symbols EURUSD,BTCUSD             # print decoded updates
//
// Latency is measured against the ingest timestamp in each frame header, so it
// is only meaningful when client and server share a clock (same host).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"pricing-engine/pkg/protocol"
)

type stats struct {
	frames, records, bytes, maxFrame atomic.Int64
	mu                               sync.Mutex
	latUs                            []int64
}

// observe records one update frame. The first frame of a connection is the
// subscription snapshot of prices received earlier, so its age is not latency.
func (s *stats) observe(size int, records int, seq uint64, latUs int64) {
	s.frames.Add(1)
	s.records.Add(int64(records))
	s.bytes.Add(int64(size))
	for {
		m := s.maxFrame.Load()
		if int64(size) <= m || s.maxFrame.CompareAndSwap(m, int64(size)) {
			break
		}
	}
	if seq == 1 {
		return
	}
	s.mu.Lock()
	s.latUs = append(s.latUs, latUs)
	s.mu.Unlock()
}

func main() {
	addr := flag.String("url", "ws://localhost:8080/ws", "engine WebSocket URL")
	format := flag.String("format", "binary", "binary or json")
	symbols := flag.String("symbols", "*", "comma-separated symbols, or *")
	clients := flag.Int("clients", 1, "number of concurrent connections")
	duration := flag.Duration("duration", 30*time.Second, "how long to run (0 = until interrupted)")
	verbose := flag.Bool("v", false, "print every decoded update (first client only)")
	flag.Parse()

	u, err := url.Parse(*addr)
	if err != nil {
		log.Fatal(err)
	}
	q := u.Query()
	q.Set("format", *format)
	q.Set("symbols", *symbols)
	u.RawQuery = q.Encode()

	st := &stats{}
	var connected atomic.Int64
	for i := 0; i < *clients; i++ {
		go func(i int) {
			if err := runClient(u.String(), st, &connected, *verbose && i == 0); err != nil {
				log.Printf("client %d: %v", i, err)
			}
		}(i)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	var deadline <-chan time.Time
	if *duration > 0 {
		deadline = time.After(*duration)
	}
	tick := time.NewTicker(time.Second)
	start := time.Now()
	var lastFrames, lastBytes int64
loop:
	for {
		select {
		case <-tick.C:
			f, b := st.frames.Load(), st.bytes.Load()
			if !*verbose {
				fmt.Fprintf(os.Stderr, "clients=%d frames/s=%d KB/s=%.1f max_frame=%dB\n",
					connected.Load(), f-lastFrames, float64(b-lastBytes)/1024, st.maxFrame.Load())
			}
			lastFrames, lastBytes = f, b
		case <-deadline:
			break loop
		case <-stop:
			break loop
		}
	}
	report(st, time.Since(start))
}

func runClient(addr string, st *stats, connected *atomic.Int64, verbose bool) error {
	conn, _, err := websocket.DefaultDialer.Dial(addr, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	connected.Add(1)
	defer connected.Add(-1)

	names := map[int]string{}
	dec := &protocol.BinaryDecoder{}
	var frame protocol.Frame
	for {
		typ, b, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		now := time.Now().UnixMicro()
		if typ == websocket.BinaryMessage {
			if err := dec.Decode(b, &frame); err != nil {
				return err
			}
			st.observe(len(b), len(frame.Quotes), frame.Seq, now-frame.IngestUs)
			if verbose {
				for _, q := range frame.Quotes {
					fmt.Printf("%-8s bid=%.*f ask=%.*f high=%.*f low=%.*f chg=%+.4f%% t=%d\n",
						names[int(q.ID)], q.Decimals, q.Bid, q.Decimals, q.Ask, q.Decimals, q.High,
						q.Decimals, q.Low, q.ChangePct, q.TimeMs)
				}
			}
			continue
		}
		var m struct {
			T       string `json:"t"`
			Seq     uint64 `json:"seq"`
			Ts      int64  `json:"ts"`
			D       []json.RawMessage
			Symbols json.RawMessage `json:"symbols"` // id table in "symbols", names in acks
		}
		if err := json.Unmarshal(b, &m); err != nil {
			return err
		}
		switch m.T {
		case "u":
			st.observe(len(b), len(m.D), m.Seq, now-m.Ts)
			if verbose {
				fmt.Println(string(b))
			}
		case "symbols":
			var table []struct {
				I int    `json:"i"`
				S string `json:"s"`
			}
			if err := json.Unmarshal(m.Symbols, &table); err != nil {
				return err
			}
			for _, s := range table {
				names[s.I] = s.S
			}
		default:
			if verbose {
				fmt.Println(string(b))
			}
		}
	}
}

func report(st *stats, elapsed time.Duration) {
	st.mu.Lock()
	lat := slices.Clone(st.latUs)
	st.mu.Unlock()
	slices.Sort(lat)
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return float64(lat[min(len(lat)-1, int(p*float64(len(lat))))]) / 1000
	}
	frames := st.frames.Load()
	fmt.Printf("\n== %s ==\n", elapsed.Round(time.Millisecond))
	fmt.Printf("frames          %d (%.1f/s)\n", frames, float64(frames)/elapsed.Seconds())
	fmt.Printf("records         %d\n", st.records.Load())
	fmt.Printf("bytes           %d (%.1f KB/s)\n", st.bytes.Load(), float64(st.bytes.Load())/1024/elapsed.Seconds())
	if frames > 0 {
		fmt.Printf("frame size      avg %.0f B, max %d B\n", float64(st.bytes.Load())/float64(frames), st.maxFrame.Load())
		fmt.Printf("bytes/record    %.1f\n", float64(st.bytes.Load())/float64(max(st.records.Load(), 1)))
	}
	fmt.Printf("latency (ms)    p50 %.3f  p90 %.3f  p99 %.3f  p99.9 %.3f  max %.3f\n",
		pct(0.50), pct(0.90), pct(0.99), pct(0.999), pct(1))
}
