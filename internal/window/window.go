// Package window maintains a rolling time window of OHLC buckets with O(1)
// amortised high/low queries.
//
// Prices are aggregated into fixed-resolution buckets (1 minute by default).
// Two monotonic deques over bucket indexes keep the running max of bucket highs
// and min of bucket lows, so High/Low are O(1) and each bucket is pushed and
// evicted at most once. Memory is bounded by span/resolution buckets per symbol
// (1440 for 24h at 1m) regardless of tick rate. The buckets double as 1-minute
// candles.
//
// The window holds every bucket that overlaps [now-span, now], so the oldest
// edge has bucket resolution: a 24h window reports statistics over 24h to
// 24h+1m. That is the precision/memory trade-off of bucketing.
package window

import (
	"cmp"
	"math"
	"slices"
	"time"
)

// Bucket is one OHLC aggregate. Start is the bucket start time in unix ms.
type Bucket struct {
	Start int64
	Open  float64
	High  float64
	Low   float64
	Close float64
	Count uint32
}

// Window is a rolling window of buckets. It is not safe for concurrent use.
type Window struct {
	span, res int64 // ms
	b         []Bucket
	off       int // b[off:] are live, sorted by Start, unique
	maxQ      deque
	minQ      deque
	dirtyFrom int64 // lowest bucket Start modified since TakeDirty
}

func New(span, resolution time.Duration) *Window {
	return &Window{
		span:      span.Milliseconds(),
		res:       resolution.Milliseconds(),
		dirtyFrom: math.MaxInt64,
	}
}

// Len is the number of live buckets.
func (w *Window) Len() int { return len(w.b) - w.off }

// Add records price p observed at ts (unix ms). It returns false, ignoring the
// price, if ts falls before the window that ends at now.
func (w *Window) Add(ts int64, p float64, now int64) bool {
	start := ts - floorMod(ts, w.res)
	if start+w.res <= now-w.span {
		return false
	}
	last := len(w.b) - 1
	switch {
	case w.Len() == 0 || start > w.b[last].Start:
		w.b = append(w.b, Bucket{Start: start, Open: p, High: p, Low: p, Close: p, Count: 1})
		w.pushMax(len(w.b) - 1)
		w.pushMin(len(w.b) - 1)
	case start == w.b[last].Start:
		bk := &w.b[last]
		bk.Close = p
		bk.Count++
		if p > bk.High {
			bk.High = p
			w.repush(&w.maxQ, last, w.pushMax)
		}
		if p < bk.Low {
			bk.Low = p
			w.repush(&w.minQ, last, w.pushMin)
		}
	default:
		w.addPast(start, p)
	}
	w.dirtyFrom = min(w.dirtyFrom, start)
	return true
}

// addPast folds a late price into an older bucket. Open and Close are kept,
// since the bucket's intra-bucket ordering is not tracked; High/Low are exact.
// This path is rare (out-of-order data), so it simply rebuilds the deques.
func (w *Window) addPast(start int64, p float64) {
	live := w.b[w.off:]
	i, found := slices.BinarySearchFunc(live, start, func(b Bucket, t int64) int { return cmp.Compare(b.Start, t) })
	if found {
		bk := &live[i]
		bk.Count++
		if p <= bk.High && p >= bk.Low {
			return
		}
		bk.High, bk.Low = max(bk.High, p), min(bk.Low, p)
	} else {
		w.b = slices.Insert(w.b, w.off+i, Bucket{Start: start, Open: p, High: p, Low: p, Close: p, Count: 1})
	}
	w.rebuild()
}

// Advance evicts buckets that no longer overlap the window ending at now and
// reports whether any were evicted.
func (w *Window) Advance(now int64) bool {
	horizon := now - w.span
	evicted := false
	for w.off < len(w.b) && w.b[w.off].Start+w.res <= horizon {
		w.off++
		evicted = true
	}
	if !evicted {
		return false
	}
	for !w.maxQ.empty() && w.maxQ.front() < w.off {
		w.maxQ.popFront()
	}
	for !w.minQ.empty() && w.minQ.front() < w.off {
		w.minQ.popFront()
	}
	if w.off >= 256 && w.off*2 >= len(w.b) {
		n := w.off
		w.b = append(w.b[:0], w.b[n:]...)
		w.off = 0
		w.maxQ.shift(n)
		w.minQ.shift(n)
	}
	return true
}

// High is the highest price in the window.
func (w *Window) High() (float64, bool) {
	if w.maxQ.empty() {
		return 0, false
	}
	return w.b[w.maxQ.front()].High, true
}

// Low is the lowest price in the window.
func (w *Window) Low() (float64, bool) {
	if w.minQ.empty() {
		return 0, false
	}
	return w.b[w.minQ.front()].Low, true
}

// Open is the first price in the window: the reference for the % change.
func (w *Window) Open() (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}
	return w.b[w.off].Open, true
}

// Buckets returns a copy of the live buckets, oldest first.
func (w *Window) Buckets() []Bucket {
	return slices.Clone(w.b[w.off:])
}

// TakeDirty returns copies of the buckets modified since the previous call.
func (w *Window) TakeDirty() []Bucket {
	if w.dirtyFrom == math.MaxInt64 {
		return nil
	}
	i := len(w.b)
	for i > w.off && w.b[i-1].Start >= w.dirtyFrom {
		i--
	}
	w.dirtyFrom = math.MaxInt64
	return slices.Clone(w.b[i:])
}

// Restore replaces the window contents with bs (any order) and evicts what is
// outside the window ending at now.
func (w *Window) Restore(bs []Bucket, now int64) {
	w.b = slices.Clone(bs)
	slices.SortFunc(w.b, func(a, b Bucket) int { return cmp.Compare(a.Start, b.Start) })
	w.b = slices.CompactFunc(w.b, func(a, b Bucket) bool { return a.Start == b.Start })
	w.off = 0
	w.rebuild()
	w.Advance(now)
	w.dirtyFrom = math.MaxInt64
}

func (w *Window) rebuild() {
	w.maxQ.reset()
	w.minQ.reset()
	for i := w.off; i < len(w.b); i++ {
		w.pushMax(i)
		w.pushMin(i)
	}
}

func (w *Window) pushMax(i int) {
	for !w.maxQ.empty() && w.b[w.maxQ.back()].High <= w.b[i].High {
		w.maxQ.popBack()
	}
	w.maxQ.push(i)
}

func (w *Window) pushMin(i int) {
	for !w.minQ.empty() && w.b[w.minQ.back()].Low >= w.b[i].Low {
		w.minQ.popBack()
	}
	w.minQ.push(i)
}

// repush re-inserts the newest bucket after its high/low moved outward.
func (w *Window) repush(q *deque, i int, push func(int)) {
	if !q.empty() && q.back() == i {
		q.popBack()
	}
	push(i)
}

func floorMod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// deque is a slice-backed double-ended queue of bucket indexes.
type deque struct {
	idx []int
	off int
}

func (d *deque) empty() bool { return d.off == len(d.idx) }
func (d *deque) front() int  { return d.idx[d.off] }
func (d *deque) back() int   { return d.idx[len(d.idx)-1] }
func (d *deque) push(i int)  { d.idx = append(d.idx, i) }
func (d *deque) popFront()   { d.off++ }
func (d *deque) popBack()    { d.idx = d.idx[:len(d.idx)-1] }
func (d *deque) reset()      { d.idx, d.off = d.idx[:0], 0 }

// shift compacts the deque and rebases indexes after the bucket slice moved by n.
func (d *deque) shift(n int) {
	live := d.idx[d.off:]
	for i := range live {
		live[i] -= n
	}
	d.idx = append(d.idx[:0], live...)
	d.off = 0
}

// Aggregate merges consecutive buckets into coarser candles of resolution res.
func Aggregate(bs []Bucket, res time.Duration) []Bucket {
	r := res.Milliseconds()
	var out []Bucket
	for _, b := range bs {
		start := b.Start - floorMod(b.Start, r)
		if n := len(out); n > 0 && out[n-1].Start == start {
			c := &out[n-1]
			c.High, c.Low = max(c.High, b.High), min(c.Low, b.Low)
			c.Close = b.Close
			c.Count += b.Count
			continue
		}
		b.Start = start
		out = append(out, b)
	}
	return out
}
