# Orderbook reconstruction

Scope: `internal/book`'s sorted-slice level container, delta semantics, the snapshot-epoch contract, the epoch-based seek warm-up algorithm, the warm-up-delta emission policy, and why this package carries no allocation gate.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md).

## A library, off the hot path

`internal/book` reconstructs one instrument's L2 orderbook from the snapshot and delta records `internal/store` serves. It is used by `cmd/convert` (to emit snapshot pointers at epoch boundaries), by seek warm-up (rebuilding a valid book before a mid-stream replay starts), and optionally by an external client replaying its own received stream.

It is never invoked per-record on the shared fan-out path. If it were, its cost would multiply by `subscribers x instruments x levels`, which is exactly the throughput bottleneck the merge and fan-out stages are designed to avoid. Fan-out delivers raw deltas and snapshot-pointer records; reconstructing a book from them is the receiving side's job.

## Sorted slice, never a map

Each side of the book — bid, ask — is a slice of `(price, size)` levels, sorted ascending by price, with binary-search insert, in-place update, and remove. **Never `map[price]level`.** A map's iteration order is undefined, which is disqualifying on its own per the root `CLAUDE.md`, and a sorted slice also gives ordered traversal for free — a top-N-levels view or a spread calculation reads directly off one end of the slice, with no separate sort step.

One comparator, `priceLess`, orders both sides. The two sides differ only in which end of the slice their caller treats as "best": `Book.Bids()` reads highest-price-first, `Book.Asks()` reads lowest-price-first, and both are documented conventions on the accessor, not something the underlying `side` type knows about.

Insert-or-update-in-place is one search-then-splice operation, so it can never leave two levels at the same price on one side — that outcome is impossible by construction, not just checked for.

## Delta semantics

A Delta record's `Size` field is the level's **new size** at `Price` — a replace, never an increment. A `Size` of zero means "remove this price level."

Applying a delta that removes a price not currently in the book is `ErrLevelNotFound`, a typed error, not a silent no-op: it means the book's state has already diverged from the venue's, and that has to surface rather than compound silently.

## The epoch-run contract

A **snapshot epoch** is a contiguous run of consecutive `RecordTypeSnapshotPointer` records in the record stream, one per active instrument, all at the same point in the stream. This is the contract `cmd/convert` must produce when it exists (see `04-binary-format.md`'s snapshot-epoch resolution and `format.md`'s "Snapshot epochs" section): every instrument gets a snapshot together, so one epoch index answers the seek question for every instrument at once.

`store.Reader.SnapshotBefore` is instrument-agnostic: it returns the last snapshot-pointer record at or before a given index, regardless of which instrument it names. For a multi-instrument epoch this lands on the epoch run's *last* record, not its first. `internal/book` resolves this itself: it walks backward from that record over the contiguous run of snapshot-pointer records to find the run's start, then forward through the run to find the specific record for the instrument it wants.

A sparse cadence — an instrument missing from a given epoch's run — is possible even though the epoch design intends a snapshot for every instrument every time. `WarmUp` treats a missing instrument as "no usable snapshot at this epoch" and falls back to an earlier epoch, or ultimately to a full scan from record 0, rather than erroring out.

## The warm-up algorithm

`WarmUp(r, instrumentID, targetTs)` reconstructs `instrumentID`'s book at `targetTs` and returns the index canonical replay should continue from — the same index `r.SeekTime(targetTs)` would return:

1. `SeekTime(targetTs)` finds the first record at or after `targetTs`. Call its index `idx`.
2. Find the last snapshot-pointer record at or before `idx-1` via `SnapshotBefore`, walk backward to that epoch run's start, then forward through the run for `instrumentID`'s own snapshot. If the run does not carry one, retry against the epoch before it, and so on.
3. Decode that snapshot's blob and load it into a fresh `Book`.
4. Replay every Delta record for `instrumentID` from the end of the epoch run (not from the snapshot record itself — other instruments' records inside the same run are irrelevant to this one) up to (exclusive) `idx`.

This bounds ordinary seek cost to one epoch's worth of deltas, not an unbounded rewind to the start of the file. Because `exchange_ts` is non-decreasing per venue (`format.md`), ascending record index within one venue's file already **is** ascending venue-sequence order, so this algorithm walks a single `*store.Reader`'s indices directly — it does not need `internal/merge`'s cross-venue ordering at all.

When no snapshot precedes `targetTs` — including a target at or before the file's first record, or an instrument absent from every epoch that does precede it — `WarmUp` degrades to a full scan from record 0. This is still correct, only unbounded, and is the expected cost of seeking before the first usable epoch.

## Warm-up deltas are consumed internally, never re-emitted

This is the open question `09-book.md` leaves for this package to resolve: whether the deltas `WarmUp` replays between an epoch and the seek target are emitted to a subscriber (flagged as pre-`T` warm-up) or consumed internally and discarded once the resulting `Book` is built.

**Decision: consumed internally, never re-emitted.** `WarmUp` returns a `*Book` and a resume index; the deltas it applied along the way exist only as intermediate mutations to that `Book` value and are not surfaced anywhere else. A subscriber that seeks to `T` receives the merged stream starting at the resume index onward — nothing earlier, warm-up or otherwise.

This is what keeps `internal/merge`'s seek-suffix test exact: `replay(from=T)` must be a byte-for-byte suffix of `replay(from=0)`, with no extra records inserted at the front and no flag field the comparison would have to strip out. Emitting warm-up deltas, even flagged, would mean `replay(from=T)`'s first records are not literally present anywhere in `replay(from=0)`'s output at the matching position — they would carry a flag `replay(from=0)` never sets, breaking the exact-suffix property this repository treats as a hard invariant, never a fuzzy one.

## Off the hot path: no allocation gate

`internal/book`'s own allocation profile is **not** gated by the M7 zero-allocation benchmarks. Those benchmarks assert on the path up to the fan-out boundary; this package sits entirely outside it, by the scoping above. `ApplySnapshot`, `side.reset`, and `Book.Bids()`/`Asks()` all allocate freely — a fresh slice per snapshot load, a copy per read — because none of that cost is on any path the M7 benchmarks measure.

Do not add an allocation-gate benchmark to this package on the assumption that it needs one to match `internal/merge` and `internal/fanout`'s convention. It does not: `cmd/lint-determinism`'s `hotPath` check does not include `internal/book` for exactly this reason (see `scope.go`), even though `orderedPath` — the narrower, map-ordering-only rule — does.
