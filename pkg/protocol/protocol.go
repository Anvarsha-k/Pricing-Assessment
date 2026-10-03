// Package protocol defines the downstream wire formats served by the pricing
// engine and the encoders/decoders for them.
//
// Two formats share one model: every record carries a symbol id plus a bit mask
// of the fields that changed since the last record this connection received for
// that symbol. The binary format additionally sends prices as zig-zag varint
// deltas of fixed-point integers, which typically costs 1-3 bytes per field.
//
// Binary frame layout (see docs/PROTOCOL.md for the full specification):
//
//	u8      frame type (FrameUpdate)
//	u64 LE  ingest time of the freshest tick in the frame, unix microseconds
//	uvarint frame sequence number (per connection, starts at 1)
//	record* until end of frame:
//	  uvarint symbol id
//	  u8      field mask (FlagFull | FlagBid | ...)
//	  FlagFull set:   u8 decimals, then all six fields as absolute varints
//	  FlagFull unset: one zig-zag varint delta per field flagged in the mask
package protocol

import "math"

// FrameUpdate is the only binary frame type; control messages are JSON text frames.
const FrameUpdate byte = 0x01

// Field mask bits.
const (
	FlagFull byte = 1 << iota
	FlagBid
	FlagAsk
	FlagHigh
	FlagLow
	FlagChange
	FlagTime
)

// MaxDecimals is the largest price precision carried on the wire.
const MaxDecimals = 8

// ChangeScale is the fixed-point scale of ChangePct (4 decimal places).
const ChangeScale = 1e4

// headerSize is the fixed part of a binary frame: type byte + u64 ingest time.
const headerSize = 9

var (
	pow10      = [MaxDecimals + 1]float64{1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8}
	fieldFlags = [numFields]byte{FlagBid, FlagAsk, FlagHigh, FlagLow, FlagChange, FlagTime}
	allFields  = FlagBid | FlagAsk | FlagHigh | FlagLow | FlagChange | FlagTime
)

const numFields = 6

// Quote is the per-symbol state streamed to clients.
type Quote struct {
	ID        uint16
	Decimals  uint8   // fractional digits of Bid/Ask/High/Low
	Bid       float64 // sell price
	Ask       float64 // buy price
	High      float64 // rolling 24h high
	Low       float64 // rolling 24h low
	ChangePct float64 // rolling 24h change, percent
	TimeMs    int64   // event time of the last accepted tick, unix ms
}

// wireState is a quote quantised to the integers that go on the wire. Change
// detection works on these integers so float noise never produces a record.
type wireState struct {
	valid bool
	dec   uint8
	f     [numFields]int64 // bid, ask, high, low, change, time
}

func toWire(q *Quote) wireState {
	dec := min(q.Decimals, MaxDecimals)
	p := pow10[dec]
	return wireState{valid: true, dec: dec, f: [numFields]int64{
		fixed(q.Bid, p), fixed(q.Ask, p), fixed(q.High, p), fixed(q.Low, p),
		fixed(q.ChangePct, ChangeScale), q.TimeMs,
	}}
}

func fixed(v, scale float64) int64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return int64(math.Round(v * scale))
}

func (w *wireState) quote(id uint16) Quote {
	p := pow10[w.dec]
	return Quote{
		ID: id, Decimals: w.dec,
		Bid: float64(w.f[0]) / p, Ask: float64(w.f[1]) / p,
		High: float64(w.f[2]) / p, Low: float64(w.f[3]) / p,
		ChangePct: float64(w.f[4]) / ChangeScale, TimeMs: w.f[5],
	}
}

// diff returns the mask of fields that differ between w and prev.
func (w *wireState) diff(prev *wireState) byte {
	var m byte
	for i := range w.f {
		if w.f[i] != prev.f[i] {
			m |= fieldFlags[i]
		}
	}
	return m
}

// states tracks the last state sent (or received) per symbol id.
type states struct{ last []wireState }

func (s *states) slot(id uint16) *wireState {
	if int(id) >= len(s.last) {
		s.last = append(s.last, make([]wireState, int(id)+1-len(s.last))...)
	}
	return &s.last[id]
}

// Forget makes the next record for id a full record.
func (s *states) Forget(id uint16) {
	if int(id) < len(s.last) {
		s.last[id].valid = false
	}
}

// Encoder builds frames for one connection. It is not safe for concurrent use.
type Encoder interface {
	// Begin starts a new frame, discarding any unfinished one.
	Begin()
	// Append adds q to the current frame unless nothing changed since the last
	// record sent for q.ID, and reports whether a record was written.
	Append(q *Quote, symbol string, ingestUs int64) bool
	// Len is the current frame size in bytes; Count the records in it.
	Len() int
	Count() int
	// MaxRecord bounds the bytes a single Append can add.
	MaxRecord() int
	// Finish completes the frame. The returned slice is valid until Begin.
	Finish() []byte
	// Forget makes the next record for id a full record.
	Forget(id uint16)
	// Binary reports whether frames are binary (true) or text (false).
	Binary() bool
}
