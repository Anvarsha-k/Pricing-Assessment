package store

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"pricing-engine/internal/window"
)

func open(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path, 48*time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestWriteLoadPrune(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s := open(t, path)
	b := Batch{
		Buckets: []BucketRecord{
			{"EURUSD", window.Bucket{Start: 60_000, Open: 1, High: 2, Low: 0.5, Close: 1.5, Count: 3}},
			{"EURUSD", window.Bucket{Start: 120_000, Open: 1.5, High: 1.6, Low: 1.4, Close: 1.55, Count: 2}},
			{"BTCUSD", window.Bucket{Start: 120_000, Open: 100, High: 101, Low: 99, Close: 100, Count: 9}},
		},
		Quotes: []QuoteRecord{{Symbol: "EURUSD", Bid: 1.55, Ask: 1.56, TimeMs: 130_000, Decimals: 5}},
	}
	if err := s.Write(b); err != nil {
		t.Fatal(err)
	}
	// Overwrite a bucket: the later write wins.
	upd := b.Buckets[1]
	upd.Bucket.High = 1.7
	if err := s.Write(Batch{Buckets: []BucketRecord{upd}}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s = open(t, path) // survives reopen
	defer s.Close()
	buckets, quotes, err := s.Load(0)
	if err != nil {
		t.Fatal(err)
	}
	if got := buckets["EURUSD"]; len(got) != 2 || got[0] != b.Buckets[0].Bucket || got[1].High != 1.7 {
		t.Fatalf("EURUSD buckets = %+v", got)
	}
	if q := quotes["EURUSD"]; q != b.Quotes[0] {
		t.Fatalf("quote = %+v", q)
	}

	if buckets, _, _ := s.Load(100_000); len(buckets["EURUSD"]) != 1 {
		t.Fatalf("Load(since) returned %d EURUSD buckets, want 1", len(buckets["EURUSD"]))
	}
	n, err := s.Prune(100_000)
	if err != nil || n != 1 {
		t.Fatalf("pruned %d (err %v), want 1", n, err)
	}
	buckets, _, _ = s.Load(0)
	if len(buckets["EURUSD"]) != 1 || len(buckets["BTCUSD"]) != 1 {
		t.Fatalf("after prune: %+v", buckets)
	}
}

func TestRunWritesUntilClosed(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state.db"))
	defer s.Close()
	in := make(chan Batch, 1)
	done := make(chan struct{})
	go func() { s.Run(in); close(done) }()
	in <- Batch{Quotes: []QuoteRecord{{Symbol: "X", Bid: 1, Ask: 2}}}
	close(in)
	<-done
	_, quotes, _ := s.Load(0)
	if _, ok := quotes["X"]; !ok {
		t.Fatal("batch sent before close was not written")
	}
}
