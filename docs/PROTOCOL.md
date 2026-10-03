# Client protocol

Endpoint: `ws://<host>:8080/ws`

| Query parameter | Default | Meaning |
|---|---|---|
| `format` | `binary` | `binary` (delta-encoded) or `json` (field deltas, permessage-deflate) |
| `symbols` | none | Comma-separated symbols to subscribe to on connect, or `*` for all. |
| `throttle_ms` | `0` | Minimum interval between update frames. The client always gets the latest values (conflation), never a backlog. |

Control messages, in both directions, are JSON **text** frames. In binary mode, updates are **binary** frames; in JSON mode they are text frames with `"t":"u"`. Every frame, control or update, is at most `MAX_FRAME_BYTES` (default 8,192, hard cap 10,240).

## Client → server

```json
{"op":"subscribe",   "symbols":["EURUSD","BTCUSD"]}   // or ["*"]
{"op":"unsubscribe", "symbols":["EURUSD"]}            // or ["*"]
{"op":"throttle",    "throttle_ms":250}
{"op":"ping"}
```

Limits: 256 symbols per request, and 10 control messages per second (burst 20). After 20 rate-limit violations the server closes the connection with code `1008`. A newly subscribed symbol is first sent as a **full** record, which acts as the snapshot; after that only changes are sent.

## Server → client (text)

```json
{"t":"welcome","format":"binary","max_frame":8192,"upstream":true,"window":"24h0m0s","basis":"mid"}
{"t":"symbols","symbols":[{"i":0,"s":"AUDUSD"},{"i":1,"s":"AUDCAD"}, ...]}  // may arrive in several chunks
{"t":"subscribed","symbols":["EURUSD","NOPE"],"unknown":["NOPE"]}
{"t":"unsubscribed","symbols":["EURUSD"],"unknown":[]}
{"t":"status","upstream":false}      // upstream feed lost / regained; prices are last-known while false
{"t":"error","msg":"rate limit exceeded"}
{"t":"pong"}
```

`symbols` messages map the numeric ids used in binary frames to names. They are always sent before any update that references a new id, including symbols that first appear on the feed after the client connected.

## Binary update frame

All integers are little-endian. `varint` is a zig-zag signed LEB128 varint and `uvarint` an unsigned one, as in Go's `encoding/binary`.

```
u8       frame type = 0x01
u64      ingest time of the freshest tick in this frame, unix µs
uvarint  frame sequence number (1, 2, 3, … per connection; detects gaps)
record*  until the end of the frame:
  uvarint  symbol id
  u8       mask
             bit 0  FULL      bit 1  bid      bit 2  ask      bit 3  high_24h
             bit 4  low_24h   bit 5  change   bit 6  time
  if FULL:
    u8       decimals d
    varint × 6   bid, ask, high, low, change, time   (absolute)
  else:
    varint   per field whose bit is set, in the order above: the delta from
             the previous value this connection received for this symbol
```

Decoding the fields:

- `bid`, `ask`, `high`, `low` are fixed-point with `d` decimals: `price = value / 10^d`.
- `change` is percent × 10⁴: `change_pct = value / 10000`.
- `time` is the event time of the last accepted tick, in unix ms.
- `bid` is the **sell** price and `ask` the **buy** price.

A FULL record resets the decoder state for that symbol. One is sent on subscribe, and again whenever the symbol's precision `d` changes. A delta record arriving before any full record for the same id is a protocol error.

Typical sizes:

| | Size |
|---|---|
| Frame header | 10–11 bytes |
| One price change (bid + ask + time) | 5–8 bytes |
| Full record | about 30 bytes |
| Snapshot of all 61 symbols | under 2 KB |

Reference implementations: `pkg/protocol` (Go encoder and decoder). `cmd/wsclient` is a complete client.

### Minimal JavaScript decoder

```js
const state = new Map(); // id -> {d, f: [bid, ask, high, low, chg, time]}
function decode(buf) {
  const v = new DataView(buf); let p = 0;
  const uvar = () => { let x = 0n, s = 0n, b; do { b = v.getUint8(p++); x |= BigInt(b & 0x7f) << s; s += 7n; } while (b & 0x80); return x; };
  const svar = () => { const u = uvar(); return Number((u >> 1n) ^ -(u & 1n)); };
  if (v.getUint8(p++) !== 1) throw new Error('bad frame');
  const ingestUs = Number(v.getBigUint64(p, true)); p += 8;
  const seq = Number(uvar()); const out = [];
  while (p < buf.byteLength) {
    const id = Number(uvar()), mask = v.getUint8(p++);
    let s = state.get(id);
    if (mask & 1) { s = { d: v.getUint8(p++), f: [] }; for (let i = 0; i < 6; i++) s.f[i] = svar(); state.set(id, s); }
    else for (let i = 0; i < 6; i++) if (mask & (2 << i)) s.f[i] += svar();
    const k = 10 ** s.d;
    out.push({ id, bid: s.f[0] / k, ask: s.f[1] / k, high: s.f[2] / k, low: s.f[3] / k, change: s.f[4] / 1e4, time: s.f[5] });
  }
  return { seq, ingestUs, quotes: out };
}
```

## JSON update frame

```json
{"t":"u","seq":42,"d":[{"i":16,"s":"EURUSD","b":1.12531,"a":1.12546,"h":1.12863,"l":1.12217,"c":0.0412,"T":1791045400856}],"ts":1791045400856123}
```

`i` and `s` are always present. The other fields appear only when they changed, and on the first record for a symbol all of them appear. Keys:

| Key | Field |
|---|---|
| `b` | bid (sell) |
| `a` | ask (buy) |
| `h` | 24h high |
| `l` | 24h low |
| `c` | 24h change, % |
| `T` | event time, ms |
| `ts` | ingest time of the freshest tick in the frame, µs |

## HTTP

| Endpoint | Returns |
|---|---|
| `GET /api/snapshot` | All current quotes: `symbol, buy, sell, high_24h, low_24h, change_24h, decimals, timestamp` |
| `GET /api/candles?symbol=BTCUSD&interval=5m&limit=100` | OHLC candles `{t,o,h,l,c,n}` from the 24h window. `interval` is a multiple of `BUCKET` up to 24h. |
| `GET /metrics` | Prometheus metrics |
| `GET /healthz` | Liveness |
| `GET /readyz` | `200` when the upstream session is live, `503` otherwise |
| `GET /` | Live demo table (JSON mode) |
