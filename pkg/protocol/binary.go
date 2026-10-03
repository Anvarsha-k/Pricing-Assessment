package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// BinaryEncoder encodes delta-compressed binary frames.
type BinaryEncoder struct {
	states
	buf    []byte
	seq    uint64
	count  int
	ingest int64
}

func NewBinaryEncoder() *BinaryEncoder {
	return &BinaryEncoder{buf: make([]byte, 0, 2048)}
}

func (e *BinaryEncoder) Binary() bool { return true }
func (e *BinaryEncoder) Len() int     { return len(e.buf) }
func (e *BinaryEncoder) Count() int   { return e.count }
func (e *BinaryEncoder) MaxRecord() int {
	return binary.MaxVarintLen16 + 2 + numFields*binary.MaxVarintLen64
}

func (e *BinaryEncoder) Begin() {
	e.buf = append(e.buf[:0], FrameUpdate, 0, 0, 0, 0, 0, 0, 0, 0)
	e.buf = binary.AppendUvarint(e.buf, e.seq+1)
	e.count, e.ingest = 0, 0
}

func (e *BinaryEncoder) Append(q *Quote, _ string, ingestUs int64) bool {
	prev := e.slot(q.ID)
	cur := toWire(q)
	mark := len(e.buf)
	e.buf = binary.AppendUvarint(e.buf, uint64(q.ID))
	if !prev.valid || prev.dec != cur.dec {
		e.buf = append(e.buf, FlagFull|allFields, cur.dec)
		for _, v := range cur.f {
			e.buf = binary.AppendVarint(e.buf, v)
		}
	} else {
		mask := cur.diff(prev)
		if mask == 0 {
			e.buf = e.buf[:mark]
			return false
		}
		e.buf = append(e.buf, mask)
		for i, fl := range fieldFlags {
			if mask&fl != 0 {
				e.buf = binary.AppendVarint(e.buf, cur.f[i]-prev.f[i])
			}
		}
	}
	*prev = cur
	e.count++
	e.ingest = max(e.ingest, ingestUs)
	return true
}

func (e *BinaryEncoder) Finish() []byte {
	binary.LittleEndian.PutUint64(e.buf[1:headerSize], uint64(e.ingest))
	e.seq++
	return e.buf
}

// Frame is a decoded update frame.
type Frame struct {
	Seq      uint64
	IngestUs int64
	Quotes   []Quote
}

var errShort = errors.New("protocol: truncated frame")

// BinaryDecoder reverses BinaryEncoder for one connection.
type BinaryDecoder struct{ states }

// Decode parses b, reusing f.Quotes' backing array.
func (d *BinaryDecoder) Decode(b []byte, f *Frame) error {
	if len(b) < headerSize || b[0] != FrameUpdate {
		return fmt.Errorf("protocol: bad frame header")
	}
	f.IngestUs = int64(binary.LittleEndian.Uint64(b[1:headerSize]))
	f.Quotes = f.Quotes[:0]
	p := b[headerSize:]
	seq, n := binary.Uvarint(p)
	if n <= 0 {
		return errShort
	}
	f.Seq, p = seq, p[n:]
	for len(p) > 0 {
		id, n := binary.Uvarint(p)
		if n <= 0 || id > 0xFFFF || len(p) < n+1 {
			return errShort
		}
		mask := p[n]
		p = p[n+1:]
		s := d.slot(uint16(id))
		if mask&FlagFull != 0 {
			if len(p) < 1 || p[0] > MaxDecimals {
				return errShort
			}
			s.valid, s.dec, p = true, p[0], p[1:]
			for i := range s.f {
				v, n := binary.Varint(p)
				if n <= 0 {
					return errShort
				}
				s.f[i], p = v, p[n:]
			}
		} else {
			if !s.valid {
				return fmt.Errorf("protocol: delta for symbol %d before full record", id)
			}
			for i, fl := range fieldFlags {
				if mask&fl == 0 {
					continue
				}
				v, n := binary.Varint(p)
				if n <= 0 {
					return errShort
				}
				s.f[i] += v
				p = p[n:]
			}
		}
		f.Quotes = append(f.Quotes, s.quote(uint16(id)))
	}
	return nil
}
