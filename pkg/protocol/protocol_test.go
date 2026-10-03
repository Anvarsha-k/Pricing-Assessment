package protocol

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"testing"
)

func randomWalk(rng *rand.Rand, qs []Quote) {
	for i := range qs {
		q := &qs[i]
		if rng.IntN(3) == 0 {
			continue
		}
		step := math.Pow(10, -float64(q.Decimals))
		q.Bid += float64(rng.IntN(7)-3) * step
		q.Ask = q.Bid + 2*step
		q.High = max(q.High, q.Ask)
		q.Low = min(q.Low, q.Bid)
		q.ChangePct = math.Round((rng.Float64()-0.5)*1e4) / 1e4
		q.TimeMs += int64(rng.IntN(1000))
	}
}

func roundTo(v float64, dec uint8) float64 {
	p := math.Pow(10, float64(dec))
	return math.Round(v*p) / p
}

func TestBinaryRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	qs := []Quote{
		{ID: 0, Decimals: 5, Bid: 1.12531, Ask: 1.12546, High: 1.13, Low: 1.12, ChangePct: 0.04, TimeMs: 1791045400570},
		{ID: 1, Decimals: 3, Bid: 109.8, Ask: 109.83, High: 110, Low: 109, ChangePct: 0.35, TimeMs: 1791045400570},
		{ID: 60, Decimals: 2, Bid: 84727.43, Ask: 84848.67, High: 85000, Low: 84000, ChangePct: -1.2, TimeMs: 1791045400570},
	}
	enc, dec := NewBinaryEncoder(), &BinaryDecoder{}
	var f Frame
	for round := 0; round < 500; round++ {
		randomWalk(rng, qs)
		if round == 250 {
			qs[1].Decimals = 4 // precision upgrade forces a full record
		}
		enc.Begin()
		for i := range qs {
			enc.Append(&qs[i], "", 42)
		}
		if enc.Count() == 0 {
			continue
		}
		if err := dec.Decode(enc.Finish(), &f); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if f.IngestUs != 42 {
			t.Fatalf("ingest = %d", f.IngestUs)
		}
		for _, got := range f.Quotes {
			var want Quote
			for _, q := range qs {
				if q.ID == got.ID {
					want = q
				}
			}
			d := want.Decimals
			if got.Bid != roundTo(want.Bid, d) || got.Ask != roundTo(want.Ask, d) ||
				got.High != roundTo(want.High, d) || got.Low != roundTo(want.Low, d) ||
				got.ChangePct != want.ChangePct || got.TimeMs != want.TimeMs || got.Decimals != d {
				t.Fatalf("round %d: got %+v want %+v", round, got, want)
			}
		}
	}
}

func TestBinarySkipsUnchangedAndDeltasAreSmall(t *testing.T) {
	enc := NewBinaryEncoder()
	q := Quote{ID: 3, Decimals: 5, Bid: 1.12531, Ask: 1.12546, High: 1.13, Low: 1.12, ChangePct: 0.04, TimeMs: 1791045400570}
	enc.Begin()
	enc.Append(&q, "", 0)
	full := enc.Len()

	enc.Begin()
	if enc.Append(&q, "", 0) {
		t.Fatal("unchanged quote produced a record")
	}
	q.Bid += 0.00001
	q.TimeMs += 120
	enc.Begin()
	before := enc.Len()
	enc.Append(&q, "", 0)
	delta := enc.Len() - before
	if delta > 6 {
		t.Fatalf("bid+time delta record is %d bytes, want <= 6 (full was %d)", delta, full)
	}
	enc.Forget(3)
	enc.Begin()
	before = enc.Len()
	enc.Append(&q, "", 0)
	if enc.Len()-before < full-headerSize-2 {
		t.Fatal("Forget did not force a full record")
	}
}

func TestDecodeRejectsDeltaWithoutBase(t *testing.T) {
	enc := NewBinaryEncoder()
	q := Quote{ID: 1, Decimals: 2, Bid: 1, Ask: 2}
	enc.Begin()
	enc.Append(&q, "", 0)
	enc.Finish()
	q.Bid = 1.5
	enc.Begin()
	enc.Append(&q, "", 0)
	if err := (&BinaryDecoder{}).Decode(enc.Finish(), &Frame{}); err == nil {
		t.Fatal("decoder accepted a delta record with no preceding full record")
	}
}

func TestJSONFrames(t *testing.T) {
	enc := NewJSONEncoder()
	q := Quote{ID: 16, Decimals: 5, Bid: 1.12531, Ask: 1.12546, High: 1.12863, Low: 1.12217, ChangePct: 0.0412, TimeMs: 1791045400570}
	enc.Begin()
	enc.Append(&q, "EURUSD", 99)
	var full struct {
		T   string           `json:"t"`
		Seq uint64           `json:"seq"`
		Ts  int64            `json:"ts"`
		D   []map[string]any `json:"d"`
	}
	if err := json.Unmarshal(enc.Finish(), &full); err != nil {
		t.Fatal(err)
	}
	if full.T != "u" || full.Seq != 1 || full.Ts != 99 || len(full.D) != 1 {
		t.Fatalf("bad frame %+v", full)
	}
	r := full.D[0]
	if r["s"] != "EURUSD" || r["b"] != 1.12531 || r["a"] != 1.12546 || r["c"] != 0.0412 || r["T"] != float64(1791045400570) {
		t.Fatalf("bad record %v", r)
	}

	q.Ask = 1.12547
	enc.Begin()
	enc.Append(&q, "EURUSD", 100)
	b := enc.Finish()
	var delta struct{ D []map[string]any }
	if err := json.Unmarshal(b, &delta); err != nil {
		t.Fatal(err)
	}
	if len(delta.D[0]) != 3 { // i, s, a
		t.Fatalf("delta record should carry only the changed field: %s", b)
	}
}

func BenchmarkBinaryEncode61(b *testing.B) {
	qs := make([]Quote, 61)
	for i := range qs {
		qs[i] = Quote{ID: uint16(i), Decimals: 5, Bid: 1.1 + float64(i), Ask: 1.1002 + float64(i), High: 2, Low: 1, TimeMs: 1791045400570}
	}
	enc := NewBinaryEncoder()
	b.ReportAllocs()
	for b.Loop() {
		enc.Begin()
		for i := range qs {
			qs[i].Bid += 0.00001
			qs[i].TimeMs++
			enc.Append(&qs[i], "", 1)
		}
		enc.Finish()
	}
}
