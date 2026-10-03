// Package model holds the types shared between the ingest, engine and
// distribution layers.
package model

import (
	"errors"
	"sync"

	"pricing-engine/pkg/protocol"
)

// Tick is one price observation received from upstream, before validation.
type Tick struct {
	Symbol   string
	Bid      float64
	Ask      float64
	Decimals uint8 // fractional digits present in the upstream text
	SrcMs    int64 // upstream event time, unix ms; 0 when the feed does not send one
	RecvNs   int64 // local receive time, unix ns
}

// Quote is the engine's computed state for one symbol.
type Quote struct {
	protocol.Quote
	Symbol   string
	IngestNs int64 // receive time of the event that produced this state, unix ns
}

// MaxSymbols caps the registry so a misbehaving feed cannot grow it unbounded.
const MaxSymbols = 4096

var (
	ErrInvalidSymbol = errors.New("invalid symbol")
	ErrRegistryFull  = errors.New("symbol registry full")
)

// Registry is the stable symbol <-> wire id mapping. Configured symbols get ids
// in configuration order; symbols first seen on the feed are appended.
type Registry struct {
	mu    sync.RWMutex
	ids   map[string]uint16
	names []string
}

func NewRegistry(symbols []string) *Registry {
	r := &Registry{ids: make(map[string]uint16, len(symbols))}
	for _, s := range symbols {
		_, _ = r.Intern(s)
	}
	return r
}

// ID returns the id of a known symbol.
func (r *Registry) ID(symbol string) (uint16, bool) {
	r.mu.RLock()
	id, ok := r.ids[symbol]
	r.mu.RUnlock()
	return id, ok
}

// Intern returns the id of symbol, registering it if needed.
func (r *Registry) Intern(symbol string) (uint16, error) {
	if id, ok := r.ID(symbol); ok {
		return id, nil
	}
	if !ValidSymbol(symbol) {
		return 0, ErrInvalidSymbol
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.ids[symbol]; ok {
		return id, nil
	}
	if len(r.names) >= MaxSymbols {
		return 0, ErrRegistryFull
	}
	id := uint16(len(r.names))
	r.ids[symbol] = id
	r.names = append(r.names, symbol)
	return id, nil
}

// Name returns the symbol for id, or "" if unknown.
func (r *Registry) Name(id uint16) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if int(id) < len(r.names) {
		return r.names[id]
	}
	return ""
}

// Names returns a copy of all symbols indexed by id.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string(nil), r.names...)
}

func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.names)
}

// ValidSymbol accepts 1-24 characters of [A-Za-z0-9._/-]. The restriction keeps
// symbols safe to embed in JSON without escaping.
func ValidSymbol(s string) bool {
	if len(s) == 0 || len(s) > 24 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '/', c == '-':
		default:
			return false
		}
	}
	return true
}
