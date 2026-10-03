# Performance

All numbers below were measured on one laptop (13th-gen Intel i5-13420H, 12 threads) running Docker Desktop. The engine, the feed (live or mock) and the measuring client all shared one Docker VM, so the client competes with the engine for CPU. Real deployments should do at least this well.

## How latency is measured

- **Server-side** (`pricing_e2e_latency_seconds`): from the moment the upstream WebSocket message was read, *before* JSON decode, to the moment the frame was written to the client socket. It measures the oldest update in each frame, so it is the worst case within the frame.
- **Client-side** (`wsclient`): client receive time minus the ingest timestamp carried in the frame header. This adds the network hop, the client's scheduling and its decode time. It is only valid because the client runs on the same host clock. The first frame of each connection is excluded, because it is the subscription snapshot of prices received earlier.

## Live feed (`wss://v3-prod.livefxhub.com/ws/algo`)

This was a Saturday, so the FX markets were closed. Snapshots arrived for 39 of the 61 symbols, and updates only for crypto.

| Metric (60 s, 1 binary client, all symbols) | Value |
|---|---|
| Upstream rate | ~34 msgs/s, most of them unchanged repeats |
| Frames delivered | 2.5/s (repeats are deduped) |
| Average / max frame | **24 B** / 896 B (max = 39-symbol snapshot) |
| Latency p50 / p90 / p99 | **0.41 ms** / 0.94 ms / 1.8 ms |
| Restart: state restored | 39 symbols in 0.85 ms; served before upstream reconnects |

## Load test (`docker-compose.mock.yml`)

Synthetic upstream:

- 20,000 ticks/s across 61 symbols;
- **2,200 ticks per message, averaging 259 KB**, which matches the spec's "~256 KB input";
- 20% duplicates, 1% out-of-order timestamps, 0.05% ±30% spikes;
- an abrupt upstream disconnect every 45 s.

| Scenario (30 s each) | Frames/s | Avg / max frame | Bytes / update | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|
| 1 binary client | 59 | 355 B / **1,477 B** | **7.2** | 3.8 ms | 5.6 ms | 6.0 ms | 6.2 ms |
| 1 JSON client | 57 | 3.6 KB / **6,147 B** | 73 | 3.9 ms | 6.0 ms | 6.9 ms | 7.1 ms |
| 1,000 binary clients | 1,260 total | 502 B / 1,485 B | 8.5 | 4.8 ms | 8.7 ms | 17.6 ms | 37.7 ms |

Server-side metrics for the whole run (about 1.86 million ticks):

| Metric | Value |
|---|---|
| Engine time per tick (validate → window → publish) | **154 ns** mean |
| Engine queue depth | 0 |
| JSON decode per 259 KB upstream message | 2.5 ms mean |
| Frames written within 10 ms / 25 ms / 50 ms of receipt | 99.5% / 99.8% / **99.99%** |
| Ticks: accepted / duplicate / late / spike | 1,436,277 / 406,071 / 17,929 / 1,106 |
| Upstream reconnects (injected) | 2, both recovered automatically |
| State flush | 3.3 ms per 1 s batch (bbolt, fsync) |
| Engine memory (RSS) | ~21 MB |

### Payload reduction

| | Input (upstream) | Output (binary) | Output (JSON) |
|---|---|---|---|
| Per message / frame | ~259 KB | 355 B avg, 1.5 KB max | 3.6 KB avg, 6.1 KB max |
| Per price update | ~118 B | ~7–8 B | ~73 B (before deflate) |

All output frames stay under the 8 KB limit, and the server enforces a hard cap of 10 KB. The reduction comes from four sources:

1. **Deduplication:** 22% of input ticks change nothing.
2. **Conflation:** a symbol updated several times between two writes is sent once.
3. **Field deltas:** only changed fields are sent; usually that's bid or ask plus time.
4. **Binary encoding:** 1–3-byte varint deltas of fixed-point integers instead of decimal strings and repeated keys.

### Where the latency goes

At p50 ≈ 4 ms with 259 KB input messages, the budget breaks down roughly as follows:

1. **~2.5 ms: decoding the whole 259 KB JSON message.** Every tick in it is stamped with the message's receive time, so the last tick decoded effectively waits for the whole decode.
2. **~0.3 ms: the engine**, at 2,200 ticks × 154 ns.
3. **The rest: wake-up, encode, socket write and Docker networking.**

With the live feed's small messages, the same path takes about 0.4 ms. The ≤ 50 ms requirement holds with an order-of-magnitude margin. The remaining tail (p99.9 of 17 ms with 1,000 clients) comes from CPU contention with the 1,000-client load generator sharing the VM.

Faster JSON parsing is the obvious next step if lower latency were needed for large payloads. One option is a streaming decoder that emits ticks while it is still parsing; another is a SIMD JSON library. I didn't do this, because the budget is already met and `encoding/json` keeps the dependency surface small.

## Micro-benchmarks

`scripts/go.sh test -run '^$' -bench . ./internal/window ./internal/engine ./pkg/protocol`

| Benchmark | ns/op | allocs/op |
|---|---|---|
| `BenchmarkWindowAdd`: advance, add, read high/low | 8.9 | 0 |
| `BenchmarkProcess`: full engine tick path (no fan-out) | 53 | 0 |
| `BenchmarkBinaryEncode61`: encode a 61-symbol delta frame | 1,605 | 0 |

The hot path makes no allocations apart from one immutable `Quote` per published change (about 100 B), which writers share by pointer.

## Reproducing

```sh
# live
docker compose up -d
docker run --rm --network pricingassessment_default --entrypoint wsclient pricing-engine -url ws://pricing-engine:8080/ws -duration 60s

# load + fault injection
docker compose -p pricing-mock -f docker-compose.yml -f docker-compose.mock.yml up -d --build
docker run --rm --network pricing-mock_default --entrypoint wsclient pricing-engine -url ws://pricing-engine:8080/ws -clients 1000 -duration 30s
curl -s localhost:8080/metrics | grep -E 'pricing_(e2e_latency|engine_tick|ticks_total|upstream_decode)'
```
