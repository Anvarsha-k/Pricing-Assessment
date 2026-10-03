// Package server is the downstream distribution layer: a WebSocket server with
// per-client subscriptions, conflation and delta encoding, plus a small HTTP API.
package server

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"pricing-engine/internal/model"
)

// Hub holds the latest quote per symbol and wakes client writers when quotes
// change.
//
// Distribution is pull-based so the engine's cost per tick is O(1) no matter
// how many clients are connected:
//
//   - Publish stores the quote in the symbol's atomic slot (lock-free) and
//     nudges the dispatcher through a 1-slot channel.
//   - The dispatcher wakes every idle writer at once by closing a broadcast
//     channel. Ticks that arrive while it is busy coalesce into one wake-up.
//   - Each writer compares slot pointers with the ones it last sent and
//     encodes only the symbols that changed.
//
// This is conflation: a slow client skips intermediate prices rather than
// queueing them, so per-client memory is bounded by the number of symbols.
type Hub struct {
	reg    *model.Registry
	maxMsg int           // size cap for control messages carrying symbol tables
	minGap time.Duration // minimum spacing between wake-ups (0 = wake per change)

	slots [model.MaxSymbols]atomic.Pointer[model.Quote]
	kick  chan struct{}

	sigMu sync.Mutex
	sig   chan struct{}

	mu        sync.Mutex // guards clients, upstream and announcements
	clients   map[*Client]struct{}
	announced atomic.Int32 // registry symbols already announced to clients
	upstream  bool
}

// NewHub creates a hub. maxMsg bounds the size of symbol-table messages, which
// are split into several messages when the registry is large. minGap
// rate-limits wake-ups to trade a little latency for larger, fewer frames.
func NewHub(reg *model.Registry, maxMsg int, minGap time.Duration) *Hub {
	h := &Hub{
		reg:     reg,
		maxMsg:  maxMsg,
		minGap:  minGap,
		kick:    make(chan struct{}, 1),
		sig:     make(chan struct{}),
		clients: make(map[*Client]struct{}),
	}
	h.announced.Store(int32(reg.Len()))
	return h
}

// Publish implements engine.Publisher. It never blocks.
func (h *Hub) Publish(q model.Quote) {
	if int(q.ID) >= len(h.slots) {
		return
	}
	if int32(q.ID) >= h.announced.Load() {
		h.announce()
	}
	h.slots[q.ID].Store(&q)
	select {
	case h.kick <- struct{}{}:
	default:
	}
}

// Run is the dispatcher loop. It returns when ctx is cancelled.
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.kick:
		}
		h.sigMu.Lock()
		close(h.sig)
		h.sig = make(chan struct{})
		h.sigMu.Unlock()
		if h.minGap > 0 {
			select {
			case <-time.After(h.minGap):
			case <-ctx.Done():
				return
			}
		}
	}
}

// changed returns a channel that is closed on the next change. Writers must
// obtain it before scanning so no change can be missed.
func (h *Hub) changed() <-chan struct{} {
	h.sigMu.Lock()
	defer h.sigMu.Unlock()
	return h.sig
}

func (h *Hub) quote(id int) *model.Quote { return h.slots[id].Load() }

// announce tells clients about symbols that appeared on the feed after they
// connected, so binary clients can map the new ids to names. Clients receive
// it before any data for those ids because writers flush control messages
// before scanning slots.
func (h *Hub) announce() {
	h.mu.Lock()
	defer h.mu.Unlock()
	names := h.reg.Names()
	from := int(h.announced.Load())
	if len(names) <= from {
		return
	}
	msgs := h.symbolMessages(names, from)
	h.announced.Store(int32(len(names)))
	for c := range h.clients {
		for _, m := range msgs {
			c.sendCtrl(m)
		}
	}
}

// SetUpstream records the upstream session state and broadcasts changes.
func (h *Hub) SetUpstream(up bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.upstream == up {
		return
	}
	h.upstream = up
	msg := mustJSON(map[string]any{"t": "status", "upstream": up})
	for c := range h.clients {
		c.sendCtrl(msg)
	}
}

func (h *Hub) Upstream() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.upstream
}

// Snapshot returns all current quotes ordered by id.
func (h *Hub) Snapshot() []model.Quote {
	n := h.reg.Len()
	out := make([]model.Quote, 0, n)
	for id := 0; id < n; id++ {
		if q := h.quote(id); q != nil {
			out = append(out, *q)
		}
	}
	return out
}

func (h *Hub) register(c *Client, welcome map[string]any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
	welcome["upstream"] = h.upstream
	c.sendCtrl(mustJSON(welcome))
	for _, m := range h.symbolMessages(h.reg.Names(), 0) {
		c.sendCtrl(m)
	}
}

func (h *Hub) unregister(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
}

type symbolEntry struct {
	ID     int    `json:"i"`
	Symbol string `json:"s"`
}

// maxSymbolEntry bounds one encoded entry: {"i":4095,"s":"<24 chars>"},
const maxSymbolEntry = 40

// symbolMessages encodes names[from:] as id->name tables, split so that each
// message stays within maxMsg bytes.
func (h *Hub) symbolMessages(names []string, from int) [][]byte {
	per := max(1, (h.maxMsg-48)/maxSymbolEntry)
	var msgs [][]byte
	for lo := from; lo < len(names); lo += per {
		hi := min(lo+per, len(names))
		entries := make([]symbolEntry, 0, hi-lo)
		for i := lo; i < hi; i++ {
			entries = append(entries, symbolEntry{ID: i, Symbol: names[i]})
		}
		msgs = append(msgs, mustJSON(map[string]any{"t": "symbols", "symbols": entries}))
	}
	return msgs
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // only called with static, marshalable shapes
	}
	return b
}
