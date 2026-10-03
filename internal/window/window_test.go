package window

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

const (
	minute = int64(time.Minute / time.Millisecond)
	day    = 24 * 60 * minute
)

type obs struct {
	ts int64
	p  float64
}

// reference computes high/low/open by brute force over every observation in
// buckets that overlap the window ending at now.
func reference(all []obs, now int64) (hi, lo, open float64, ok bool) {
	hi, lo = math.Inf(-1), math.Inf(1)
	firstBucket := int64(math.MaxInt64)
	for _, o := range all {
		start := o.ts - floorMod(o.ts, minute)
		if start+minute <= now-day {
			continue
		}
		ok = true
		hi, lo = max(hi, o.p), min(lo, o.p)
		if start < firstBucket {
			firstBucket, open = start, o.p
		}
	}
	return
}

func TestWindowMatchesBruteForce(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	w := New(24*time.Hour, time.Minute)
	var all []obs
	now := int64(1_700_000_000_000)
	p := 100.0
	for i := 0; i < 20000; i++ {
		now += rng.Int64N(30 * 1000) // up to 30s between ticks: covers ~3.5 days
		p *= 1 + (rng.Float64()-0.5)*0.01
		ts := now
		if rng.IntN(20) == 0 { // 5% late ticks, up to 10 minutes old
			ts -= rng.Int64N(10 * minute)
		}
		w.Advance(now)
		if w.Add(ts, p, now) {
			all = append(all, obs{ts, p})
		}
		if i%97 == 0 {
			hi, lo, _, ok := reference(all, now)
			gotHi, ok1 := w.High()
			gotLo, ok2 := w.Low()
			if ok != ok1 || ok != ok2 || gotHi != hi || gotLo != lo {
				t.Fatalf("step %d: got hi=%v lo=%v, want hi=%v lo=%v", i, gotHi, gotLo, hi, lo)
			}
		}
	}
	if w.Len() > 24*60+1 {
		t.Fatalf("window holds %d buckets, want <= %d", w.Len(), 24*60+1)
	}
}

func TestWindowOpenIsOldestPriceInWindow(t *testing.T) {
	w := New(24*time.Hour, time.Minute)
	base := int64(1_700_000_000_000) - floorMod(1_700_000_000_000, minute)
	w.Add(base, 10, base)
	w.Add(base+minute, 11, base+minute)
	w.Add(base+day, 12, base+day)
	if o, _ := w.Open(); o != 10 {
		t.Fatalf("open = %v, want 10", o)
	}
	w.Advance(base + day + minute) // first bucket now entirely outside the window
	if o, _ := w.Open(); o != 11 {
		t.Fatalf("open after expiry = %v, want 11", o)
	}
}

func TestWindowExpiryDropsOldExtremes(t *testing.T) {
	w := New(24*time.Hour, time.Minute)
	t0 := int64(0)
	w.Add(t0, 200, t0) // spike-high early
	w.Add(t0+minute, 100, t0+minute)
	w.Add(t0+2*minute, 50, t0+2*minute)
	if h, _ := w.High(); h != 200 {
		t.Fatalf("high = %v, want 200", h)
	}
	w.Advance(t0 + day + minute)
	if h, _ := w.High(); h != 100 {
		t.Fatalf("high after first bucket expired = %v, want 100", h)
	}
	if l, _ := w.Low(); l != 50 {
		t.Fatalf("low = %v, want 50", l)
	}
	w.Advance(t0 + 3*day)
	if _, ok := w.High(); ok || w.Len() != 0 {
		t.Fatalf("window should be empty, len=%d", w.Len())
	}
	// A fresh tick after a full expiry starts a new window.
	w.Add(t0+3*day, 7, t0+3*day)
	if h, _ := w.High(); h != 7 {
		t.Fatalf("high = %v, want 7", h)
	}
}

func TestWindowRejectsTicksOlderThanWindow(t *testing.T) {
	w := New(24*time.Hour, time.Minute)
	now := 10 * day
	if w.Add(now-day-2*minute, 1, now) {
		t.Fatal("tick older than the window was accepted")
	}
}

func TestLateTickInsertsMissingBucket(t *testing.T) {
	w := New(24*time.Hour, time.Minute)
	w.Add(0, 10, 0)
	w.Add(5*minute, 12, 5*minute)
	w.Add(2*minute, 99, 5*minute) // late, into an empty minute
	if w.Len() != 3 {
		t.Fatalf("len = %d, want 3", w.Len())
	}
	if h, _ := w.High(); h != 99 {
		t.Fatalf("high = %v, want 99", h)
	}
	bs := w.Buckets()
	for i := 1; i < len(bs); i++ {
		if bs[i-1].Start >= bs[i].Start {
			t.Fatalf("buckets not sorted: %+v", bs)
		}
	}
}

func TestTakeDirtyAndRestore(t *testing.T) {
	w := New(24*time.Hour, time.Minute)
	for i := int64(0); i < 5; i++ {
		w.Add(i*minute, float64(10+i), i*minute)
	}
	if got := len(w.TakeDirty()); got != 5 {
		t.Fatalf("dirty = %d, want 5", got)
	}
	if got := w.TakeDirty(); got != nil {
		t.Fatalf("second TakeDirty = %v, want nil", got)
	}
	w.Add(4*minute+1, 20, 4*minute+1)
	dirty := w.TakeDirty()
	if len(dirty) != 1 || dirty[0].High != 20 {
		t.Fatalf("dirty = %+v, want only the last bucket", dirty)
	}

	r := New(24*time.Hour, time.Minute)
	r.Restore(w.Buckets(), 5*minute)
	h1, _ := w.High()
	h2, _ := r.High()
	l1, _ := w.Low()
	l2, _ := r.Low()
	if h1 != h2 || l1 != l2 || r.Len() != w.Len() {
		t.Fatalf("restored window differs: hi %v/%v lo %v/%v len %d/%d", h1, h2, l1, l2, w.Len(), r.Len())
	}
}

func TestAggregate(t *testing.T) {
	var bs []Bucket
	for i := int64(0); i < 10; i++ {
		p := float64(i)
		bs = append(bs, Bucket{Start: i * minute, Open: p, High: p + 1, Low: p - 1, Close: p, Count: 1})
	}
	out := Aggregate(bs, 5*time.Minute)
	if len(out) != 2 {
		t.Fatalf("got %d candles, want 2", len(out))
	}
	c := out[0]
	if c.Open != 0 || c.Close != 4 || c.High != 5 || c.Low != -1 || c.Count != 5 {
		t.Fatalf("bad candle %+v", c)
	}
}

func BenchmarkWindowAdd(b *testing.B) {
	w := New(24*time.Hour, time.Minute)
	now := int64(1_700_000_000_000)
	p := 100.0
	rng := rand.New(rand.NewPCG(1, 2))
	b.ReportAllocs()
	for b.Loop() {
		now += 50
		p *= 1 + (rng.Float64()-0.5)*0.001
		w.Advance(now)
		w.Add(now, p, now)
		w.High()
		w.Low()
	}
}
