# Backpressure semantics

Scope: the shared-ring design, the lapping protocol that makes an undetectable dropped event impossible by construction, `Block` and `Drop` semantics, gap reporting via the global emit index, the drop-oldest overflow policy, subscriber lifecycle, and the `pacing_slip` gauge.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md).

## Why not a channel and a goroutine per subscriber

A design with one buffered channel and one goroutine per subscriber does not implement "Block blocks until the slowest subscriber accepts" — it implements "blocks until the slowest subscriber's *buffer* is full." Every subscriber gets `cap(channel)` events of slack before backpressure engages at all, the point where it engages depends on an arbitrary buffer-size choice rather than subscriber speed, and the merge stage can run up to `sum(cap)` events ahead of the slowest subscriber.

`internal/fanout.Ring` replaces this entirely with a single shared, fixed-capacity ring and one atomic cursor per subscriber (a disruptor-style design). One emit goroutine writes; any number of subscriber goroutines read through their own cursor. This is what makes Block mean exactly what it says: a real per-event barrier, not a per-buffer one.

## The lapping protocol

A reader may be copying a slot at the exact moment the writer overwrites it. The protocol that makes this safe:

1. The reader loads the slot's `seq`.
2. The reader copies the slot's data.
3. The reader reloads `seq`. If it changed, the copy may be torn; discard it and figure out what actually happened.
4. If `seq` still matches, the copy is valid.

This makes an undetectable dropped event impossible **by construction**, not by a best-effort counter that could itself race.

### Why the slot payload is atomic words, not a plain struct

The naive version of the protocol above stores the record as a plain `store.Record` field, guarded only by the `seq` check around it. That is not race-clean: a Drop subscriber's plain read of that field is concurrent with the writer's plain write, with no synchronizing operation between them, and `go test -race` is right to report it — a seqlock's whole premise is that the reader may observe torn data, so there is no happens-before edge for the race detector to find, real or false.

`internal/fanout.slot` instead holds the record as `[8]atomic.Uint64` (`store.RecordSize / 8`), reusing `store.EncodeRecord`/`store.DecodeRecordFields` — the project's one encoding of the wire layout — to stage a record into that form. Every byte either side touches is an atomic word. There is no plain concurrent access anywhere for the race detector to find, by construction rather than by argument.

`seq` itself carries more than a generation counter: it is `2*n` while emit index `n` is being written, and `2*n+1` once every word is published. A reader wanting index `n` that observes anything else knows immediately whether the writer is still publishing `n` (wait) or has already moved past it (catch up to whatever index the slot now reports). The writer stores the even value **before** touching any word, never after: if it wrote the words first, a reader could load an older, still-published odd `seq`, copy the new write's half-finished words, reload that same stale `seq`, and accept torn data as a complete read. Invalidating first closes that window.

**Cost, measured, not assumed** (`BenchmarkRingWrite`, `BENCHMARKS.md`): about 57 ns/op with no blob, still zero allocations. That is roughly ten sequentially-consistent stores per record — `store.EncodeRecord` plus eight word stores plus two `seq` publishes — against a plain struct assignment's one. Reads pay nothing extra on amd64, where `atomic.Uint64.Load` compiles to a plain `MOV`. Two optimisations are named but deliberately not built, per the project's profile-first rule: dropping to 7 slot words (the wire form's last 8 bytes are always reserved zeros), and skipping the atomic path entirely when the subscriber set holds no Drop subscriber (the Block barrier then already guarantees exclusivity). Revisit only with a profile showing this is the bottleneck.

### The blob arena

A snapshot pointer's blob payload is copied into a parallel arena, sized once at construction to `Config.MaxBlobBytes`, addressed by slot index and covered by the same `seq` check as the record itself. This is a deliberate departure from letting a subscriber read the payload out of `store.Reader` directly: a venue's data spans many day files, each with its own blob region starting at offset zero, so a bare `(VenueID, BlobOffset)` pair cannot identify which file's blob region it names. Copying also buys something the alternative could not: a subscriber can safely keep draining the ring after the merge that produced it has closed its underlying files, because the payload no longer aliases the mmap.

A blob larger than `MaxBlobBytes` is a construction-time-sized error (`ErrBlobTooLarge`) at `Write`, never a silent truncation.

## `Block` and `Drop`

Set explicitly at `Subscribe`; there is no meaningful zero value for `BackpressureMode`, so an unset mode is rejected (`ErrModeUnset`) rather than silently defaulting to one.

**Block**: before overwriting the slot for emit index `n`, the writer waits until every registered Block subscriber's cursor has moved past the index that slot currently holds — `min(cursor) > n - capacity`. An empty Block set never holds the writer back (`minBlockCursor` returns `MaxUint64` for one). The wait is a `runtime.Gosched` spin, never a clock read on the fast path and never a `select`: this package bans a multi-clause `select` on the same reasoning `internal/merge`'s single-goroutine merge already applies to its own cancellation path (see `internal/merge/merge.go`'s `refill`), and a plain spin that yields the processor is the simplest thing that stays correct without one.

**Drop**: the writer never waits for a Drop subscriber. If it laps one, the subscriber's next read detects this through the lapping protocol above and reports the exact gap. On overflow, the writer always drops the **oldest** unread data — the record whose emit index the slot is about to be reused for — which keeps a Drop subscriber as close to the live edge as possible rather than replaying an ever-growing backlog of stale data.

### The Block/Drop coupling is real, and it is observable

A slow Block subscriber throttles the entire ring: the writer will not advance past `min(cursor)` over Block subscribers, which means it also cannot lap any Drop subscriber. **One slow Block subscriber changes whether Drop subscribers ever see a gap at all.** This is correct, not a bug, but it must not be invisible — see `pacing_slip` below.

## Gap reporting

`Gap{FirstMissedIndex, LastMissedIndex, Count}` uses the **global emit index**, never a venue sequence number: a gap in a merged multi-venue stream spans several venues and has no single sequence range that describes it. `Count` is always `LastMissedIndex - FirstMissedIndex + 1`.

A gap is delivered attached to the record that follows it, in the same `Delivery{Record, Blob, Gap, HasGap}` — not as a separate frame a subscriber could miss. This costs nothing extra: the catch-up target the lapping protocol already computes (`internal/fanout.Ring.next`) is exactly the record the gap precedes, so the two are known together, not in two passes.

**The gap never reaches the canonical hash.** It lives in delivery framing only. A seek-replay that starts partway through the stream must produce the same content hash as the equivalent suffix of a full replay; if gap reporting were part of the hashed projection, a seek would change the hash for reasons unrelated to content, breaking the seek-suffix property `internal/merge` already establishes at the merge stage.

## `pacing_slip`

`Ring.PacingSlipNanos()` is the cumulative real time `Write` has spent parked at the Block barrier. It is read through `Config.Clock` (defaulting to `clock.RealClock{}` when unset) exactly like every other package that needs time — this is not a second exception to "`time.Now` appears exactly once," because `RealClock` stays the one call site regardless of who constructs a value of it. The clock is read only when the barrier actually blocks, never on the fast path, so it costs nothing in steady state.

This is the mechanism that keeps the Block/Drop coupling above from being invisible: without it, a single slow Block subscriber silently turning a fast replay into a much slower one has no operator-visible signal at all.

There is no pacing schedule to compare against yet — that is `08-pacing.md`'s job. This gauge measures Block-barrier wait time; M8 extends the definition once a schedule exists to compare it to.

## Subscriber lifecycle and the control queue

`Subscribe` and `Unsubscribe` resolve immediately, with no queue involved, until `Ring.StartEmitting` has been called — the common case of a subscriber set fixed before the first event (see `16-open-questions.md`'s Q4). Once emitting has started, a join or leave is **queued** and applied by the emit goroutine between records, via `applyControl`, rather than mutating the subscriber set while the emit goroutine might be iterating it. The queue is a mutex-guarded slice, not a channel: telling "queue this" apart from "the ring is closed" under one lock needs a condition a channel-based queue cannot express without a second `select` case on the receive, which is exactly the construct this package's `no-multi-select` rule keeps out.

`applyControl` is cheap whether or not anything is pending, so the Block barrier's spin loop calls it on every iteration. That is what lets a departing Block subscriber release a writer parked waiting for it — without it, the barrier could deadlock on the last Block subscriber's own departure.

`SetEnd` closes the control queue in the same call, failing any request still pending with `ErrClosed`, so "the stream has ended" is one call rather than two things a caller has to remember to do together.

### Leaving releases a parked reader

Removing a subscriber from the barrier is not enough on its own. `Subscriber.Next` parks until a record is ready, the ring ends, or the subscriber is stopped, so a subscriber that only left the barrier would keep spinning in `Next` until the whole ring ended — and a `Drop` subscriber, which was never on the barrier's list at all, would see no effect from leaving whatsoever. A client that disconnects mid-stream is exactly that case, and it must not need the run to finish before its reader goroutine can return.

`Subscriber.Cancel` is the out-of-band stop for one subscriber. It sets a second one-way flag beside the watchdog's eviction flag, and `Next` tests both before every read attempt, so `Cancel` returns `ErrCanceled` from the current parked call and from every call after it. `Ring.Unsubscribe` calls it on the subscriber it removes, **after** the removal has been applied on every return path: the reverse order would leave a canceled subscriber registered on the barrier with a cursor that has stopped advancing, which is precisely what the writer waits on forever.

Cancellation is out of band in the same sense eviction is, and for the same reason it is safe: the flag is read only by the canceled subscriber's own `Next`, it never changes any record's content, order, or emit index, and every other subscriber's stream is byte-identical to a run where nothing was ever canceled. The two flags are independent and neither is ever cleared, so no interleaving of `Cancel` and an eviction can produce a state `Next` reads inconsistently; a subscriber that is both reports `ErrEvicted`, fixed by the order of the two checks rather than by which store landed first.

`Cancel` alone does not unregister a `Block` subscriber. A caller that stops one for good calls `Unsubscribe`, which does both.

### `StartAt` and reproducibility

`Beginning()`, `ExchangeTs(t)`, and `EmitIndex(i)` are fully reproducible for a subscriber joining **before the first event is emitted** — the same starting content every run. `EmitIndex(i)` stays exactly reproducible even for a **mid-run** join: it pins content directly, and if `i` has already been overwritten the ordinary lapping protocol reports the precise gap on the first read, with no special-casing needed at Subscribe time. `Beginning()`/`ExchangeTs(t)` resolved mid-run instead depend on when the joining goroutine's request happens to be applied, which is a wall-clock artifact. `Live()` is never reproducible, by definition — it means "wherever the write index currently is."

A Block subscriber whose resolved start position has already been overwritten is rejected outright (`ErrStartLapped`): Block promises no loss, and the ring cannot reproduce data it no longer holds. A Drop subscriber in the same situation needs no special handling — its first `Next` call reports the exact gap through the ordinary path.

## The stalled-Block-subscriber watchdog

A Block subscriber that stops reading entirely — a crashed client, a network partition — would otherwise stall the writer forever: nothing else in this package forces a Block subscriber out *on its own*. `Unsubscribe` above needs a caller who has already noticed, and the case the watchdog exists for is precisely the one where nobody has. `Config.WatchdogTimeout` (nanoseconds; zero disables it) is how long the writer will wait at the barrier with no progress from the subscriber actually responsible before evicting it and moving on.

The check lives inside `waitForBlockBarrier`'s own spin loop: each iteration that finds the barrier still blocked reads `Config.Clock` (see `docs/clock.md` for why this is not a new exception to the `time.Now` invariant) and compares elapsed time against the timeout, restarting its own window after each eviction so a second stalled subscriber gets its own full timeout rather than being evicted immediately by a stale window. The victim is always the Block subscriber with the smallest cursor — the one actually responsible for the wait — never chosen by iteration position.

Eviction is **out of band and final**. It is out of band because it never changes any record's content, order, or emit index — every surviving subscriber's stream is identical to a run where the watchdog never fired. It is final because `Subscriber.Next` checks eviction **before** attempting a read, on every call, not only once: the ordinary lapping protocol could otherwise still serve an evicted subscriber's stale cursor through an ordinary catch-up (whatever now occupies the slot it wanted), turning eviction into a one-time skip instead of the severing it is meant to be. Once evicted, a subscriber's `Next` returns `ErrEvicted` forever.

This is also the one place in the project where a run's outcome is not fully determined by content and order alone: which subscriber, if any, gets evicted, and exactly when, depends on real scheduling. A `SimClock`-driven determinism run leaves `WatchdogTimeout` at its zero default, which disables eviction entirely — "no progress for a real duration" has no meaning against a clock that never advances on its own, and content determinism must never depend on it.
