# Real-Time Pricing Engine

A low-latency, fault-tolerant market data engine in Go. It consumes the livefxhub WebSocket price feed and computes its **own** buy/sell, rolling 24h high/low and 24h % change for 61 instruments. The upstream statistics are ignored. Results are served to many WebSocket clients as compact binary or JSON deltas.

| Requirement | How it is met | Measured |
|---|---|---|
| Real-time processing | Single-writer engine; 1-minute OHLC buckets + monotonic deques give O(1) rolling 24h high/low | **154 ns**/tick |
| State management | Write-behind bbolt store (1 fsync'd tx/s); full restore on start; graceful drain on SIGTERM | restore < 1 ms |
| WebSocket output | Own server: multi-client, per-client subscriptions, snapshot-on-subscribe, conflation | 1,000 clients tested |
| Payload < 10 KB | Dedupe + conflation + field deltas + binary varint encoding; every frame split to ≤ 8 KB | 259 KB in → **1.5 KB max** out |
| Latency ≤ 50 ms | Lock-free publish, one broadcast wake-up per batch, no intermediate hops | p50 **0.4 ms** live; p99.99 < 50 ms at 20k ticks/s |
| Fault tolerance | Backoff + jitter reconnect, idle watchdog, dedupe, late-tick handling, spike filter, backpressure, slow-client eviction | see [Architecture](docs/ARCHITECTURE.md#fault-tolerance-summary) |
| Modular / scalable | `feed → engine → hub → clients`, each a package behind a small interface; shard-by-symbol path | |
| Bonus | OHLC candles API, Prometheus metrics, rate limiting, binary protocol, mock feed + load tester | |

## Quick start

```sh
cp .env.example .env        # put API_KEY / SECRET_KEY in .env
docker-compose up --build   # or: docker compose up --build
```

Then:

- **http://localhost:8080/** opens the trading dashboard. Use `HOST_PORT=18080 docker compose up` if port 8080 is taken.
- `curl localhost:8080/api/snapshot` returns all current quotes.
- `curl 'localhost:8080/api/candles?symbol=BTCUSD&interval=5m&limit=12'` returns OHLC candles.
- `curl localhost:8080/metrics` returns Prometheus metrics. `docker compose --profile monitoring up` also starts Prometheus on :9090.

To stream from the command line with the bundled client:

```sh
docker run --rm --network pricingassessment_default --entrypoint wsclient pricing-engine \
  -url ws://pricing-engine:8080/ws -symbols BTCUSD,ETHUSD -v
```

To load-test with fault injection (20k ticks/s, 259 KB messages, duplicates, late ticks, spikes, disconnects), without touching the live instance:

```sh
HOST_PORT=8081 docker compose -p pricing-mock -f docker-compose.yml -f docker-compose.mock.yml up --build
```

## Connecting a client

```
ws://localhost:8080/ws?format=binary&symbols=EURUSD,BTCUSD     # or format=json, symbols=*
→ {"op":"subscribe","symbols":["XAUUSD"]}
← {"t":"welcome",…}  {"t":"symbols",…}  {"t":"subscribed",…}  <binary update frames…>
```

Each subscribed symbol is sent first as a full record (the snapshot), and after that only fields that changed are sent. Full specification, including a 25-line JavaScript decoder: [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): **architecture diagram**, components, the life of a tick, design decisions and trade-offs, fault-tolerance table, scaling path.
- [docs/PROTOCOL.md](docs/PROTOCOL.md): client protocol (binary and JSON) and HTTP API.
- [docs/PERFORMANCE.md](docs/PERFORMANCE.md): methodology, live and load-test results, latency breakdown, benchmarks.
- [docs/CHALLENGES.md](docs/CHALLENGES.md): problems hit along the way and how each was solved.

## Configuration

Everything is set through environment variables (see `.env.example`).

| Variable | Default | |
|---|---|---|
| `API_KEY`, `SECRET_KEY` | (required) | upstream credentials |
| `FEED_URL` | `wss://v3-prod.livefxhub.com/ws/algo` | |
| `SYMBOLS` | the 61 from the assignment | comma-separated |
| `PRICE_BASIS` | `mid` | price the 24h statistics use: `mid`, `bid` or `ask` |
| `WINDOW` / `BUCKET` | `24h` / `1m` | rolling window and its resolution |
| `SPIKE_THRESHOLD_PCT` | `10` | larger moves need confirmation (`0` disables) |
| `SPIKE_CONFIRM_TICKS` / `SPIKE_CONFIRM_AFTER` | `3` / `10s` | distinct ticks, or hold time, that confirm a new level |
| `MAX_CLOCK_SKEW` | `5s` | reject upstream timestamps further in the future than this |
| `FLUSH_INTERVAL` / `RETENTION` | `1s` / `48h` | persistence cadence / candle retention on disk |
| `MAX_FRAME_BYTES` | `8192` | output frame cap (≤ 10240) |
| `FANOUT_INTERVAL` | `0` | minimum gap between client wake-ups (trade latency for fewer frames) |
| `MAX_CLIENTS` / `MAX_CONNS_PER_IP` | `10000` / `100` | connection limits |
| `CONTROL_RATE` / `CONTROL_BURST` | `10` / `20` | per-client control-message token bucket |
| `CLIENT_WRITE_TIMEOUT` | `5s` | slower clients are disconnected |
| `FEED_IDLE_TIMEOUT` | `30s` | reconnect if upstream is silent (pings every ⅓ of this) |
| `FEED_BACKOFF_MIN` / `FEED_BACKOFF_MAX` | `500ms` / `30s` | reconnect backoff |
| `LISTEN_ADDR` / `DATA_DIR` / `LOG_LEVEL` | `:8080` / `/data` / `info` | |

## Development

Go 1.26. No local Go install is needed; `scripts/go.sh` runs the toolchain in Docker:

```sh
scripts/go.sh test ./...                                  # unit + integration tests
scripts/go.sh test -run '^$' -bench . ./internal/... ./pkg/...
docker run --rm -v "$PWD":/src -w /src -e CGO_ENABLED=1 golang:1.26 go test -race ./...
```

The tests cover:

- the rolling window against a brute-force reference over 20k random ticks, including late ticks;
- protocol round-trips, delta sizes, and frames for a 2,000-symbol snapshot staying under the cap;
- engine dedupe, spike confirmation, late and future ticks, expiry, and persist → restore;
- bbolt round-trip and pruning;
- feed reconnect and auth backoff against a fake upstream;
- the server end to end: snapshot then deltas, JSON mode, rate-limit disconnect, status and symbol announcements.

```
cmd/
  pricing-engine/   service entrypoint (wiring, ordered shutdown, -healthcheck)
  mockfeed/         synthetic upstream with fault injection
  wsclient/         decoding client / load generator / latency meter
internal/
  feed/             upstream session, parsing
  engine/           validation, ordering, spike filter, statistics
  window/           rolling OHLC window, O(1) high/low
  server/           hub, WebSocket clients, HTTP API, demo page
  store/            bbolt persistence
  config/ metrics/ model/
pkg/protocol/       wire formats (encoder + decoder)
```
