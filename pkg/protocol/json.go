package protocol

import "strconv"

// JSONEncoder encodes field-delta frames as compact JSON text:
//
//	{"t":"u","seq":7,"d":[{"i":16,"s":"EURUSD","b":1.12531,"a":1.12546}],"ts":1791045400856123}
//
// A record always carries "i" and "s"; the other fields appear only when they
// changed (all of them on the first record for a symbol). Values are absolute.
type JSONEncoder struct {
	states
	buf    []byte
	seq    uint64
	count  int
	ingest int64
}

func NewJSONEncoder() *JSONEncoder {
	return &JSONEncoder{buf: make([]byte, 0, 4096)}
}

var jsonKeys = [numFields]string{`"b":`, `"a":`, `"h":`, `"l":`, `"c":`, `"T":`}

func (e *JSONEncoder) Binary() bool   { return false }
func (e *JSONEncoder) Len() int       { return len(e.buf) }
func (e *JSONEncoder) Count() int     { return e.count }
func (e *JSONEncoder) MaxRecord() int { return 64 + numFields*32 }

func (e *JSONEncoder) Begin() {
	e.buf = append(e.buf[:0], `{"t":"u","seq":`...)
	e.buf = strconv.AppendUint(e.buf, e.seq+1, 10)
	e.buf = append(e.buf, `,"d":[`...)
	e.count, e.ingest = 0, 0
}

func (e *JSONEncoder) Append(q *Quote, symbol string, ingestUs int64) bool {
	prev := e.slot(q.ID)
	cur := toWire(q)
	mask := allFields
	if prev.valid && prev.dec == cur.dec {
		if mask = cur.diff(prev); mask == 0 {
			return false
		}
	}
	if e.count > 0 {
		e.buf = append(e.buf, ',')
	}
	e.buf = append(e.buf, `{"i":`...)
	e.buf = strconv.AppendUint(e.buf, uint64(q.ID), 10)
	// Symbols are validated to [A-Za-z0-9._/-] on registration, so no escaping is needed.
	e.buf = append(e.buf, `,"s":"`...)
	e.buf = append(e.buf, symbol...)
	e.buf = append(e.buf, '"')
	for i, fl := range fieldFlags {
		if mask&fl == 0 {
			continue
		}
		e.buf = append(e.buf, ',')
		e.buf = append(e.buf, jsonKeys[i]...)
		switch i {
		case 4:
			e.buf = strconv.AppendFloat(e.buf, float64(cur.f[i])/ChangeScale, 'f', -1, 64)
		case 5:
			e.buf = strconv.AppendInt(e.buf, cur.f[i], 10)
		default:
			e.buf = strconv.AppendFloat(e.buf, float64(cur.f[i])/pow10[cur.dec], 'f', int(cur.dec), 64)
		}
	}
	e.buf = append(e.buf, '}')
	*prev = cur
	e.count++
	e.ingest = max(e.ingest, ingestUs)
	return true
}

func (e *JSONEncoder) Finish() []byte {
	e.buf = append(e.buf, `],"ts":`...)
	e.buf = strconv.AppendInt(e.buf, e.ingest, 10)
	e.buf = append(e.buf, '}')
	e.seq++
	return e.buf
}
