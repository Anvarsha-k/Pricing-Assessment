// Package engine turns raw upstream ticks into per-symbol quotes with rolling
// 24h statistics.
//
// The engine is a single goroutine that owns all symbol state (actor model):
// no locks on the hot path, deterministic ordering, and per-tick cost in the
// low microseconds. Ticks arrive on a bounded channel; reads that need engine
// state (candles) are executed on the engine goroutine through a query channel.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"pricing-engine/internal/metrics"
	"pricing-engine/internal/model"
	"pricing-engine/internal/store"
	"pricing-engine/internal/window"
)

// Basis selects the price the 24h statistics are computed on.
type Basis uint8

const (
	BasisMid Basis = iota
	BasisBid
	BasisAsk
)

func ParseBasis(s string) (Basis, error) {
	switch strings.ToLower(s) {
	case "mid", "":
		return BasisMid, nil
	case "bid", "sell":
		return BasisBid, nil
	case "ask", "buy":
		return BasisAsk, nil
	}
	return 0, fmt.Errorf("unknown price basis %q (want mid, bid or ask)", s)
}

func (b Basis) Price(bid, ask float64) float64 {
	switch b {
	case BasisBid:
		return bid
	case BasisAsk:
		return ask
	}
	return (bid + ask) / 2
}

type Config struct {
	Window            time.Duration // rolling statistics window (24h)
	Resolution        time.Duration // bucket size (1m)
	Basis             Basis
	SpikeThreshold    float64       // max relative move vs the last accepted price; <= 0 disables the filter
	SpikeConfirm      int           // distinct consecutive ticks at a new level needed to accept a jump
	SpikeConfirmAfter time.Duration // or: accept a new level that has held this long (0 = never)
	MaxClockSkew      time.Duration // how far upstream timestamps may be ahead of local time
	FlushInterval     time.Duration // expiry check and persistence cadence
	QueueSize         int
}

// Publisher receives every quote change.
type Publisher interface {
	Publish(q model.Quote)
}

var (
	ticksAccepted = metrics.Ticks.WithLabelValues("accepted")
	ticksDup      = metrics.Ticks.WithLabelValues("duplicate")
	ticksInvalid  = metrics.Ticks.WithLabelValues("invalid")
	ticksSpike    = metrics.Ticks.WithLabelValues("spike")
	ticksLate     = metrics.Ticks.WithLabelValues("late")
	ticksTooOld   = metrics.Ticks.WithLabelValues("too_old")
	ticksFuture   = metrics.Ticks.WithLabelValues("future")
	ticksUnknown  = metrics.Ticks.WithLabelValues("bad_symbol")
)

type Engine struct {
	cfg     Config
	reg     *model.Registry
	pub     Publisher
	flush   chan<- store.Batch
	in      chan model.Tick
	queries chan func()
	syms    []*symbol // by registry id
	pending store.Batch
	now     func() time.Time
	log     *slog.Logger
}

type symbol struct {
	name       string
	win        *window.Window
	q          model.Quote
	has        bool    // a price has been accepted
	px         float64 // basis price of the last accepted tick
	quoteDirty bool    // last price changed since the previous flush
	spikePx    float64 // candidate level during spike confirmation
	spikeN     int     // distinct confirming ticks seen at the candidate level
	spikeSince int64   // event time the candidate level was first seen
}

// New creates an engine. flush receives persistence batches and is closed when
// Run returns.
func New(cfg Config, reg *model.Registry, pub Publisher, flush chan<- store.Batch, log *slog.Logger) *Engine {
	return &Engine{
		cfg:     cfg,
		reg:     reg,
		pub:     pub,
		flush:   flush,
		in:      make(chan model.Tick, cfg.QueueSize),
		queries: make(chan func()),
		now:     time.Now,
		log:     log,
	}
}

// Input is the channel the feed writes ticks to. Sends block when the engine
// is saturated, which pushes back on the upstream TCP connection rather than
// silently dropping prices the 24h statistics depend on.
func (e *Engine) Input() chan<- model.Tick { return e.in }

// Run processes ticks until ctx is cancelled, then drains queued ticks,
// writes a final persistence batch and closes the flush channel.
func (e *Engine) Run(ctx context.Context) {
	defer close(e.flush)
	t := time.NewTicker(e.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case tk := <-e.in:
			e.timedProcess(tk)
		case fn := <-e.queries:
			fn()
		case now := <-t.C:
			e.housekeep(now)
		case <-ctx.Done():
			e.drain()
			e.collect()
			if !e.pending.Empty() {
				select {
				case e.flush <- e.pending:
				case <-time.After(5 * time.Second):
					e.log.Error("final state flush timed out")
				}
			}
			return
		}
	}
}

func (e *Engine) drain() {
	for {
		select {
		case tk := <-e.in:
			e.timedProcess(tk)
		default:
			return
		}
	}
}

func (e *Engine) timedProcess(t model.Tick) {
	start := time.Now()
	e.process(t)
	metrics.EngineTickSeconds.Observe(time.Since(start).Seconds())
}

func (e *Engine) process(t model.Tick) {
	if !validPrice(t.Bid) || !validPrice(t.Ask) || t.Bid > t.Ask {
		ticksInvalid.Inc()
		return
	}
	nowMs := e.now().UnixMilli()
	evt := t.RecvNs / int64(time.Millisecond)
	if t.SrcMs > 0 {
		if t.SrcMs > nowMs+e.cfg.MaxClockSkew.Milliseconds() {
			ticksFuture.Inc()
			return
		}
		evt = t.SrcMs
	}
	s, err := e.symbol(t.Symbol)
	if err != nil {
		ticksUnknown.Inc()
		return
	}
	if s.has && t.Bid == s.q.Bid && t.Ask == s.q.Ask {
		ticksDup.Inc()
		return
	}
	px := e.cfg.Basis.Price(t.Bid, t.Ask)

	if s.has && evt < s.q.TimeMs {
		// Out of order: the price belongs to the past. It still counts towards
		// the high/low of the bucket it falls in, but must not replace the
		// newer current price. A late outlier cannot be confirmed by later
		// ticks, so it is simply rejected.
		if thr := e.cfg.SpikeThreshold; thr > 0 && relDiff(px, s.px) > thr {
			ticksSpike.Inc()
			return
		}
		if !s.win.Add(evt, px, nowMs) {
			ticksTooOld.Inc()
			return
		}
		ticksLate.Inc()
		if e.refresh(s) {
			s.q.IngestNs = t.RecvNs
			e.pub.Publish(s.q)
		}
		return
	}

	if !e.spikeOK(s, px, evt) {
		ticksSpike.Inc()
		return
	}
	s.win.Advance(nowMs)
	if !s.win.Add(evt, px, nowMs) {
		ticksTooOld.Inc()
		return
	}
	ticksAccepted.Inc()
	s.q.Bid, s.q.Ask, s.q.TimeMs = t.Bid, t.Ask, evt
	s.q.Decimals = max(s.q.Decimals, t.Decimals)
	s.q.IngestNs = t.RecvNs
	s.px, s.has, s.quoteDirty = px, true, true
	e.refresh(s)
	e.pub.Publish(s.q)
}

// spikeOK rejects a price that jumps more than SpikeThreshold away from the
// last accepted one, unless the new level is confirmed (a genuine gap, e.g. a
// market re-open or a restart after downtime): either SpikeConfirm consecutive
// ticks with distinct prices agree on it, or it has held for SpikeConfirmAfter.
// Re-sent identical prices do not count, so a replayed bad tick cannot
// confirm itself.
func (e *Engine) spikeOK(s *symbol, px float64, evt int64) bool {
	thr := e.cfg.SpikeThreshold
	if !s.has || thr <= 0 || relDiff(px, s.px) <= thr {
		s.spikeN = 0
		return true
	}
	switch {
	case s.spikeN == 0 || relDiff(px, s.spikePx) > thr:
		s.spikeN, s.spikeSince = 1, evt
	case px != s.spikePx:
		s.spikeN++
	}
	s.spikePx = px
	held := evt-s.spikeSince >= e.cfg.SpikeConfirmAfter.Milliseconds()
	if s.spikeN >= e.cfg.SpikeConfirm || (held && e.cfg.SpikeConfirmAfter > 0) {
		e.log.Warn("accepting new price level after confirmation",
			"symbol", s.name, "from", s.px, "to", px, "ticks", s.spikeN)
		s.spikeN = 0
		return true
	}
	return false
}

// refresh recomputes the 24h statistics of s and reports whether they changed.
func (e *Engine) refresh(s *symbol) bool {
	hi, lo, chg := s.px, s.px, 0.0
	if h, ok := s.win.High(); ok {
		l, _ := s.win.Low()
		hi, lo = h, l
		if ref, _ := s.win.Open(); ref > 0 {
			chg = (s.px - ref) / ref * 100
		}
	}
	changed := hi != s.q.High || lo != s.q.Low || chg != s.q.ChangePct
	s.q.High, s.q.Low, s.q.ChangePct = hi, lo, chg
	return changed
}

// housekeep expires window buckets (publishing statistics that changed as
// old extremes age out) and hands dirty state to the store.
func (e *Engine) housekeep(now time.Time) {
	metrics.EngineQueue.Set(float64(len(e.in)))
	nowMs := now.UnixMilli()
	for _, s := range e.syms {
		if s == nil || !s.has || !s.win.Advance(nowMs) {
			continue
		}
		if e.refresh(s) {
			s.q.IngestNs = now.UnixNano()
			e.pub.Publish(s.q)
		}
	}
	e.collect()
	if e.pending.Empty() {
		return
	}
	select {
	case e.flush <- e.pending:
		e.pending = store.Batch{}
	default:
		// The store is still busy; keep accumulating and retry next interval.
		metrics.PersistBackpressure.Inc()
	}
}

func (e *Engine) collect() {
	for _, s := range e.syms {
		if s == nil {
			continue
		}
		for _, b := range s.win.TakeDirty() {
			e.pending.Buckets = append(e.pending.Buckets, store.BucketRecord{Symbol: s.name, Bucket: b})
		}
		if s.quoteDirty {
			s.quoteDirty = false
			e.pending.Quotes = append(e.pending.Quotes, store.QuoteRecord{
				Symbol: s.name, Bid: s.q.Bid, Ask: s.q.Ask, TimeMs: s.q.TimeMs, Decimals: s.q.Decimals,
			})
		}
	}
}

func (e *Engine) symbol(name string) (*symbol, error) {
	id, err := e.reg.Intern(name)
	if err != nil {
		return nil, err
	}
	if int(id) >= len(e.syms) {
		e.syms = append(e.syms, make([]*symbol, int(id)+1-len(e.syms))...)
	}
	s := e.syms[id]
	if s == nil {
		s = &symbol{name: name, win: window.New(e.cfg.Window, e.cfg.Resolution)}
		s.q.ID, s.q.Symbol = id, name
		e.syms[id] = s
	}
	return s, nil
}

// Restore loads persisted state. It must be called before Run.
func (e *Engine) Restore(buckets map[string][]window.Bucket, quotes map[string]store.QuoteRecord) int {
	now := e.now()
	n := 0
	for name, q := range quotes {
		s, err := e.symbol(name)
		if err != nil {
			continue
		}
		s.win.Restore(buckets[name], now.UnixMilli())
		s.q.Bid, s.q.Ask, s.q.TimeMs, s.q.Decimals = q.Bid, q.Ask, q.TimeMs, q.Decimals
		s.q.IngestNs = now.UnixNano()
		s.px, s.has = e.cfg.Basis.Price(q.Bid, q.Ask), true
		e.refresh(s)
		e.pub.Publish(s.q)
		n++
	}
	return n
}

// Candles returns up to limit candles of the given resolution for symbol,
// aggregated from the in-memory window buckets, oldest first.
func (e *Engine) Candles(ctx context.Context, name string, res time.Duration, limit int) ([]window.Bucket, error) {
	reply := make(chan []window.Bucket, 1)
	query := func() {
		id, ok := e.reg.ID(name)
		if !ok || int(id) >= len(e.syms) || e.syms[id] == nil {
			reply <- nil
			return
		}
		reply <- e.syms[id].win.Buckets()
	}
	select {
	case e.queries <- query:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var bs []window.Bucket
	select {
	case bs = <-reply:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if res != e.cfg.Resolution {
		bs = window.Aggregate(bs, res)
	}
	if limit > 0 && len(bs) > limit {
		bs = bs[len(bs)-limit:]
	}
	return bs, nil
}

func validPrice(v float64) bool {
	return v > 0 && !math.IsInf(v, 0) && !math.IsNaN(v)
}

func relDiff(a, ref float64) float64 {
	return math.Abs(a-ref) / ref
}
