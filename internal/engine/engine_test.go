package engine

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"pricing-engine/internal/model"
	"pricing-engine/internal/store"
	"pricing-engine/internal/window"
)

type recorder struct{ quotes []model.Quote }

func (r *recorder) Publish(q model.Quote) { r.quotes = append(r.quotes, q) }

func (r *recorder) last(t *testing.T) model.Quote {
	t.Helper()
	if len(r.quotes) == 0 {
		t.Fatal("nothing published")
	}
	return r.quotes[len(r.quotes)-1]
}

type fixture struct {
	e     *Engine
	pub   *recorder
	now   time.Time
	flush chan store.Batch
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{pub: &recorder{}, now: time.UnixMilli(1_791_000_000_000), flush: make(chan store.Batch, 16)}
	f.e = New(Config{
		Window:            24 * time.Hour,
		Resolution:        time.Minute,
		Basis:             BasisMid,
		SpikeThreshold:    0.10,
		SpikeConfirm:      3,
		SpikeConfirmAfter: 10 * time.Second,
		MaxClockSkew:      5 * time.Second,
		FlushInterval:     time.Second,
		QueueSize:         16,
	}, model.NewRegistry([]string{"EURUSD", "BTCUSD"}), f.pub, f.flush, slog.New(slog.NewTextHandler(io.Discard, nil)))
	f.e.now = func() time.Time { return f.now }
	return f
}

// tick sends a tick received "now" and advances the clock by step.
func (f *fixture) tick(sym string, bid, ask float64, step time.Duration) {
	f.e.process(model.Tick{Symbol: sym, Bid: bid, Ask: ask, Decimals: 5, RecvNs: f.now.UnixNano()})
	f.now = f.now.Add(step)
}

func TestComputesStatsIgnoringUpstream(t *testing.T) {
	f := newFixture(t)
	f.tick("EURUSD", 1.0000, 1.0002, time.Minute) // mid 1.0001
	f.tick("EURUSD", 1.0100, 1.0102, time.Minute) // mid 1.0101
	f.tick("EURUSD", 0.9900, 0.9902, time.Minute) // mid 0.9901
	f.tick("EURUSD", 1.0050, 1.0052, 0)           // mid 1.0051
	q := f.pub.last(t)
	if q.Bid != 1.0050 || q.Ask != 1.0052 {
		t.Fatalf("bid/ask = %v/%v", q.Bid, q.Ask)
	}
	if q.High != 1.0101 || q.Low != 0.9901 {
		t.Fatalf("high/low = %v/%v, want 1.0101/0.9901", q.High, q.Low)
	}
	want := (1.0051 - 1.0001) / 1.0001 * 100
	if diff := q.ChangePct - want; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("change = %v, want %v", q.ChangePct, want)
	}
}

func TestDropsDuplicatesAndInvalid(t *testing.T) {
	f := newFixture(t)
	f.tick("EURUSD", 1.0, 1.0002, time.Second)
	f.tick("EURUSD", 1.0, 1.0002, time.Second) // duplicate
	f.tick("EURUSD", 0, 1.0002, time.Second)   // non-positive
	f.tick("EURUSD", 1.1, 1.0, time.Second)    // crossed
	f.tick("bad symbol!", 1, 2, time.Second)   // invalid name
	if n := len(f.pub.quotes); n != 1 {
		t.Fatalf("published %d quotes, want 1", n)
	}
}

func TestSpikeFilterRejectsOutlierThenAcceptsConfirmedLevel(t *testing.T) {
	f := newFixture(t)
	f.tick("BTCUSD", 100, 100, time.Second)
	f.tick("BTCUSD", 150, 150, time.Second) // +50%: single bad tick
	f.tick("BTCUSD", 101, 101, time.Second)
	if q := f.pub.last(t); q.Bid != 101 || q.High != 101 {
		t.Fatalf("outlier leaked: %+v", q.Quote)
	}
	// A sustained move is a real gap and is accepted on the third tick.
	f.tick("BTCUSD", 130, 130, time.Second)
	f.tick("BTCUSD", 131, 131, time.Second)
	if q := f.pub.last(t); q.Bid != 101 {
		t.Fatalf("accepted before confirmation: %+v", q.Quote)
	}
	f.tick("BTCUSD", 132, 132, time.Second)
	if q := f.pub.last(t); q.Bid != 132 {
		t.Fatalf("confirmed level not accepted: %+v", q.Quote)
	}
}

func TestReplayedBadTickCannotConfirmItself(t *testing.T) {
	f := newFixture(t)
	f.tick("BTCUSD", 100, 100, time.Millisecond)
	for i := 0; i < 5; i++ { // the same bad price re-sent
		f.tick("BTCUSD", 130, 130, time.Millisecond)
	}
	if q := f.pub.last(t); q.Bid != 100 {
		t.Fatalf("replayed outlier accepted: %+v", q.Quote)
	}
	// ...but a level that persists long enough is accepted even if flat.
	f.now = f.now.Add(10 * time.Second)
	f.tick("BTCUSD", 130, 130, 0)
	if q := f.pub.last(t); q.Bid != 130 {
		t.Fatalf("persistent level not accepted: %+v", q.Quote)
	}
}

func TestLateTickUpdatesStatsButNotPrice(t *testing.T) {
	f := newFixture(t)
	base := f.now.UnixMilli()
	send := func(src int64, bid float64) {
		f.e.process(model.Tick{Symbol: "EURUSD", Bid: bid, Ask: bid, Decimals: 5, SrcMs: src, RecvNs: f.now.UnixNano()})
	}
	send(base-2000, 1.0)
	send(base-1000, 1.01)
	send(base-1500, 1.05) // arrives after a newer tick
	q := f.pub.last(t)
	if q.Bid != 1.01 || q.TimeMs != base-1000 {
		t.Fatalf("late tick replaced the current price: %+v", q.Quote)
	}
	if q.High != 1.05 {
		t.Fatalf("late tick not reflected in the 24h high: %v", q.High)
	}
	send(base+60_000, 1.02) // further than MaxClockSkew in the future
	if q := f.pub.last(t); q.Bid != 1.01 {
		t.Fatalf("future-dated tick accepted: %+v", q.Quote)
	}
}

func TestHousekeepingExpiresOldExtremes(t *testing.T) {
	f := newFixture(t)
	f.tick("EURUSD", 1.20, 1.20, time.Hour) // the high
	f.tick("EURUSD", 1.10, 1.10, 0)
	if q := f.pub.last(t); q.High != 1.20 {
		t.Fatalf("high = %v", q.High)
	}
	f.now = f.now.Add(23*time.Hour + 2*time.Minute)
	n := len(f.pub.quotes)
	f.e.housekeep(f.now)
	if len(f.pub.quotes) != n+1 {
		t.Fatal("expiry did not publish an update")
	}
	if q := f.pub.last(t); q.High != 1.10 || q.ChangePct != 0 {
		t.Fatalf("after expiry high=%v change=%v, want 1.10 and 0", q.High, q.ChangePct)
	}
}

func TestPersistAndRestore(t *testing.T) {
	f := newFixture(t)
	f.tick("EURUSD", 1.00, 1.00, time.Minute)
	f.tick("EURUSD", 1.05, 1.05, time.Minute)
	f.tick("EURUSD", 1.02, 1.02, 0)
	f.e.housekeep(f.now)
	b := <-f.flush
	if len(b.Buckets) != 3 || len(b.Quotes) != 1 {
		t.Fatalf("flush batch has %d buckets, %d quotes", len(b.Buckets), len(b.Quotes))
	}
	f.e.housekeep(f.now)
	select {
	case b := <-f.flush:
		t.Fatalf("clean state flushed again: %+v", b)
	default:
	}

	// Feed the batch into a fresh engine as if it came back from disk.
	g := newFixture(t)
	g.now = f.now.Add(10 * time.Second)
	buckets := map[string][]window.Bucket{}
	for _, r := range b.Buckets {
		buckets[r.Symbol] = append(buckets[r.Symbol], r.Bucket)
	}
	quotes := map[string]store.QuoteRecord{}
	for _, q := range b.Quotes {
		quotes[q.Symbol] = q
	}
	if n := g.e.Restore(buckets, quotes); n != 1 {
		t.Fatalf("restored %d symbols", n)
	}
	q := g.pub.last(t)
	if q.Bid != 1.02 || q.High != 1.05 || q.Low != 1.00 {
		t.Fatalf("restored quote %+v", q.Quote)
	}
	// After a restart the first tick is not a spike relative to the restored price.
	g.tick("EURUSD", 1.03, 1.03, 0)
	if q := g.pub.last(t); q.Bid != 1.03 || q.High != 1.05 {
		t.Fatalf("post-restore tick %+v", q.Quote)
	}
}

func TestRunDrainsAndFlushesOnShutdown(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.e.Run(ctx); close(done) }()
	f.e.Input() <- model.Tick{Symbol: "EURUSD", Bid: 1, Ask: 1, RecvNs: f.now.UnixNano()}
	cancel()
	<-done
	var got store.Batch
	for b := range f.flush { // closed by Run
		got.Quotes = append(got.Quotes, b.Quotes...)
	}
	if len(got.Quotes) != 1 {
		t.Fatalf("final flush carried %d quotes, want 1", len(got.Quotes))
	}
}

func BenchmarkProcess(b *testing.B) {
	e := New(Config{
		Window: 24 * time.Hour, Resolution: time.Minute, SpikeThreshold: 0.1, SpikeConfirm: 3,
		MaxClockSkew: time.Second, FlushInterval: time.Second, QueueSize: 1,
	}, model.NewRegistry(nil), nopPub{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	syms := []string{"EURUSD", "GBPUSD", "BTCUSD", "XAUUSD"}
	now := time.UnixMilli(1_791_000_000_000)
	e.now = func() time.Time { return now }
	px := 1.0
	i := 0
	b.ReportAllocs()
	for b.Loop() {
		i++
		now = now.Add(10 * time.Millisecond)
		px += 0.00001
		e.process(model.Tick{Symbol: syms[i%4], Bid: px, Ask: px + 0.0002, Decimals: 5, RecvNs: now.UnixNano()})
	}
}

type nopPub struct{}

func (nopPub) Publish(model.Quote) {}
