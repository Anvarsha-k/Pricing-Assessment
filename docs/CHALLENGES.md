# Challenges faced

The problems below came up while building and testing the engine, roughly in the order I hit them.

## 1. The real feed doesn't match the spec

**Problem.** The assignment describes messages with `buy, sell, low_24h, high_24h, change_24h, timestamp`. When I probed the live endpoint before writing any code, it actually sends `{"event":"marketPriceUpdate","symbol":…,"bid":…,"ask":…,"high":…,"low":…,"percentage":…}`. That shape has **no timestamp and no sequence number**, there is no acknowledgement for `subscribeMarketPrice`, and only 39 of the 61 requested symbols produced a snapshot.

**Solution.** The parser accepts both vocabularies: `bid/ask`, and `buy/sell` with buy mapped to ask and sell to bid. It also accepts single objects, arrays, and `{"data":[…]}` batches. The upstream statistics fields aren't even decoded, so there's no way to use them by accident. An upstream timestamp is used when present, in any common unit; otherwise each tick is stamped with its local receive time. Unknown event types are logged and counted, never fatal.

## 2. Out-of-order handling without timestamps

**Problem.** Without upstream timestamps, "out-of-order" cannot be detected on the live feed: TCP delivers in order, and receive time is all there is. Even so, the engine has to behave correctly when timestamps *are* present.

**Solution.** Event time is the upstream timestamp if one is sent, otherwise the receive time. A tick older than the symbol's current event time is a **late** tick. Late ticks are folded into the high/low of the past bucket they belong to, so the 24h statistics stay correct, but they never overwrite the newer current price. A late outlier is rejected outright, since nothing can confirm it. Ticks older than the window, or too far in the future, are dropped. The mock feed sends 1% of ticks up to 2 s late to exercise this path: 17,929 late ticks were handled in the load test.

## 3. Duplicates dominate the feed

**Problem.** Most live updates are byte-identical repeats of the previous one. After a reconnect, the snapshot replays every symbol's current price.

**Solution.** Value-based dedupe against the current quote: the 22% of mock ticks that were repeats cost about 50 ns each and generate no output. That one decision does much of the payload optimisation on its own.

## 4. Fan-out made the engine collapse under load (largest issue)

**Problem.** The first hub design had the engine goroutine walk each symbol's subscriber set on every tick, under a hub-wide lock, marking each client dirty. Unit tests passed and the live feed was fine. The 20k ticks/s load test with 500 clients was not:

| | Measured |
|---|---|
| Engine time per tick | ~98 µs (fan-out included), so a maximum of about 10k ticks/s against 20k/s arriving |
| Engine queue | grew to 20,938 ticks |
| End-to-end latency | **11–14 seconds** |

The cost was O(subscribers) per tick on the single engine goroutine. It was made worse by contention on the hub lock, which 500 writer goroutines were reading at the same time.

**Solution.** I inverted the design to make it pull-based:

- The engine stores an immutable quote in that symbol's `atomic.Pointer` slot and does a non-blocking send to a dispatcher. That is O(1) and takes no lock.
- The dispatcher closes a broadcast channel, waking every idle writer at once. Ticks that arrive in the meantime coalesce into that single wake-up.
- Each writer compares slot pointers with the ones it last sent and encodes only what changed.

Result at the same load: **154 ns per tick (about 600× faster)**, queue depth 0, and server-side p99.99 under 50 ms with 1,000 clients. Conflation now falls out of the design rather than being bolted on.

**Lesson:** the bottleneck only showed up at realistic fan-out. A load generator (`cmd/mockfeed` plus `wsclient -clients N`) was worth building early.

## 5. A replayed bad tick could confirm itself

**Problem.** The spike filter rejected a jump above 10% unless three consecutive ticks agreed on the new level. In the load test, the logs showed `accepting new price level … from 180.785 to 126.553` followed immediately by `… to 180.799`. The mock re-sends the previous tick to simulate duplicates, and when that previous tick was a bad one, three identical copies of the bad price "confirmed" it. The engine then needed three more ticks to return to the real price.

**Solution.** Identical repeats of the candidate price no longer count as confirmation; it takes `SPIKE_CONFIRM_TICKS` **distinct** prices at the new level. So that a genuinely flat new level (an illiquid symbol, or a reopen) isn't rejected forever, a level that holds for `SPIKE_CONFIRM_AFTER` (10 s) is also accepted. A regression test covers both cases.

**Trade-off:** a real jump larger than 10% is delayed by up to three ticks, or up to 10 s. That's acceptable for a pricing display; the threshold is configurable per deployment, and 0 disables the filter.

## 6. Corrupted state from buffer reuse in bbolt

**Problem.** The persistence round-trip test read back a BTCUSD candle under EURUSD's key. I had reused one key buffer and one value buffer across all `Put` calls in a transaction. bbolt keeps references to those slices until commit, so every record ended up pointing at the last value written.

**Solution.** Each transaction now allocates one arena sized for the batch and hands each `Put` its own slice of it. That's one allocation per flush instead of one per record. Found by the store test before it ever reached the running service.

## 7. Keeping *every* frame under 10 KB, not just updates

**Problem.** The 2,000-symbol frame-limit test failed on the **welcome** message, which carried the whole symbol table: 48 KB. A full snapshot on subscribe, and a large subscribe acknowledgement, could both break the limit the same way.

**Solution.** The symbol table is sent as separate `symbols` messages, chunked to the frame limit. Update frames are split whenever the next record could cross `MAX_FRAME_BYTES`, using the encoder's worst-case record size. Subscribe requests are capped at 256 names (`*` covers bulk use). Tests check that every binary and JSON frame for a 2,000-symbol snapshot stays under a 1 KB limit.

## 8. Ordering control messages before data

**Problem.** Binary frames refer to symbols by id. If a new symbol appears on the feed mid-session, its first update must not reach a client before the `symbols` message that names it. On connect, the welcome must also come first.

**Solution.** Control messages go into a per-client queue. Before every scan for data, the writer drains that queue. The hub queues an announcement *before* storing the first quote for a new id, so the announcement is always written first.

## 9. Latency attribution with large payloads

**Problem.** With 259 KB upstream messages, p50 was about 4 ms. That's well within budget, but I wanted to understand it rather than guess.

**Solution.** I added `pricing_upstream_decode_seconds`, which showed 2.5 ms per message spent in `encoding/json`. Ticks are stamped with the message's receive time *before* decoding, which keeps the latency figure honest, and that decode step dominates. The engine accounts for about 0.3 ms per message. A streaming or SIMD JSON parser is the documented next step; it isn't needed to meet the target.

## 10. Testing with closed markets and no local Go toolchain

**Problem.** Development happened on a Saturday, when the FX markets are closed and only crypto ticks, at about 34 messages/s. That rate is far too low to exercise rate spikes, large payloads or disconnects. The development machine also had no Go installed.

**Solution.**

- `cmd/mockfeed` speaks the live protocol and injects every fault class on demand, at any rate or payload size.
- All Go commands run in the official `golang` image through `scripts/go.sh`, and the race detector runs in the Debian variant.
- The production image is a static binary on distroless, running as non-root.

## 11. Upstream allows one session per API key

**Problem.** After the engine was redeployed, the dashboard showed *Feed down*, and every reconnect failed with `close 4003: already have an active session`. On shutdown, the feed client closed its TCP socket without sending a WebSocket close frame. The upstream kept the dead session registered for about a minute and rejected the new container. The backoff loop did recover on its own, but a routine deploy shouldn't cause a minute of stale prices.

**Solution.** On shutdown, the feed client now sends a `1000 Normal Closure` frame before closing the socket. The upstream releases the session immediately, and a restart is back to `subscribed` in about 1 s, with no 4003 rejections, verified across repeated restarts. A 4003 rejection is still retried with backoff, which covers a crash where no close frame could be sent, and it is logged clearly as a session still held upstream.

## 12. The latency metric counted snapshots as latency

**Problem.** The dashboard's latency card showed 250 ms on a quiet market. Each time a client subscribed, the server sent the current prices as a snapshot, and those were received upstream earlier. The server measured their age as delivery latency.

**Solution.** The writer tracks the first record of each symbol on a connection. Those records are encoded and sent as usual but left out of `pricing_e2e_latency_seconds`, and a frame made only of snapshot records records no latency at all. The client-side `wsclient` already skipped the first frame for the same reason. With the fix, live server-side latency reads p50 96 µs, p99 247 µs.

## 13. Smaller things

- **Host port clash:** 8080 was already in use on the development machine. The compose file now publishes `${HOST_PORT:-8080}`.
- **Upstream's own high/low use ask prices**, as observed for wide-spread crypto where the bid sits below upstream's `low`. The engine defaults to mid (`PRICE_BASIS=mid|bid|ask`), so its numbers deliberately won't match upstream's for those symbols.
- **A 24h window can't start full:** on a cold start, statistics cover only the history available so far. Persistence means that happens once, not on every deploy.
