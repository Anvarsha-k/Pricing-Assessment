package feed

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"pricing-engine/internal/model"
)

// wireMsg is the union of the upstream message shapes we care about.
//
// The upstream 24h statistics (high/low/percentage, or low_24h/high_24h/
// change_24h) are deliberately not decoded: the engine computes its own.
// Both bid/ask (what the feed actually sends) and buy/sell (what the spec
// describes) are accepted; buy is the ask side and sell the bid side.
type wireMsg struct {
	Event     string          `json:"event"`
	Success   *bool           `json:"success"`
	Message   string          `json:"message"`
	Symbol    string          `json:"symbol"`
	Bid       json.Number     `json:"bid"`
	Ask       json.Number     `json:"ask"`
	Buy       json.Number     `json:"buy"`
	Sell      json.Number     `json:"sell"`
	Timestamp json.RawMessage `json:"timestamp"`
	Data      []wireMsg       `json:"data"`
}

// decode accepts a single object, an array of objects, or an object wrapping
// an array in "data" (batched payloads).
func decode(b []byte) ([]wireMsg, error) {
	b = bytes.TrimSpace(b)
	if len(b) > 0 && b[0] == '[' {
		var ms []wireMsg
		err := json.Unmarshal(b, &ms)
		return ms, err
	}
	var m wireMsg
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if len(m.Data) > 0 && m.Symbol == "" {
		return m.Data, nil
	}
	return []wireMsg{m}, nil
}

// tick converts a price message. ok is false if it carries no usable prices.
func (m *wireMsg) tick(recvNs int64) (t model.Tick, ok bool) {
	bidN, askN := m.Bid, m.Ask
	if bidN == "" {
		bidN = m.Sell
	}
	if askN == "" {
		askN = m.Buy
	}
	if m.Symbol == "" || bidN == "" || askN == "" {
		return t, false
	}
	bid, err1 := strconv.ParseFloat(string(bidN), 64)
	ask, err2 := strconv.ParseFloat(string(askN), 64)
	if err1 != nil || err2 != nil {
		return t, false
	}
	return model.Tick{
		Symbol:   m.Symbol,
		Bid:      bid,
		Ask:      ask,
		Decimals: max(decimals(string(bidN)), decimals(string(askN))),
		SrcMs:    parseTimestamp(m.Timestamp),
		RecvNs:   recvNs,
	}, true
}

// decimals counts fractional digits in a JSON number literal.
func decimals(s string) uint8 {
	if strings.ContainsAny(s, "eE") {
		return 8
	}
	i := strings.IndexByte(s, '.')
	if i < 0 {
		return 0
	}
	return uint8(min(len(s)-i-1, 8))
}

// parseTimestamp accepts unix seconds/ms/µs/ns (number or numeric string) or
// RFC 3339 and returns unix ms, or 0 if absent or unparseable.
func parseTimestamp(raw json.RawMessage) int64 {
	if len(raw) == 0 || string(raw) == "null" {
		return 0
	}
	s := string(raw)
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &s); err != nil {
			return 0
		}
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixMilli()
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f <= 0 {
		return 0
	}
	switch {
	case f >= 1e17:
		return int64(f / 1e6)
	case f >= 1e14:
		return int64(f / 1e3)
	case f >= 1e11:
		return int64(f)
	default:
		return int64(f * 1e3)
	}
}
