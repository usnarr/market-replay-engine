# The merge stage

Scope: `internal/merge`'s fixed-size loser tree, its sentinel padding and cached keys, the per-venue `Cursor`, the worker pool and batch decode, the single-case-receive rule, and the seek-suffix contract.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md).

## What the stage is for

One `Cursor` per venue goes in. One canonically ordered stream comes out, through `Merger.Next`. Downstream, `fanout.Ring.Run` drains that stream into the shared ring — see [`backpressure.md`](./backpressure.md).

`NewMerger` decodes in the calling goroutine and starts nothing. `NewConcurrentMerger` decodes through a pool of worker goroutines. **Their output is identical, byte for byte, at any worker count.** Everything below exists to make that a structural property rather than something that happens to hold.

## A loser tree, never `container/heap`

`container/heap` is banned outright, and the linter enforces it. `heap.Interface` dispatches through interface methods, and `heap.Push`/`heap.Pop` take `any`, which boxes every value pushed — both disqualifying on a path this project keeps free of allocation and indirection.

A loser tree also costs about half a binary min-heap's comparisons per pop: one replay of the path from a leaf to the root, `log2(k)` comparisons, against the heap's sift-down.

But the property the determinism guarantee actually rests on is a different one. **The tree's shape is settled by `k` at construction and never changes** — not as records are consumed, not as the data's distribution shifts, not as cursors run out. `LoserTree.fix` walks the same path for a given cursor every single time, because that path is a function of the cursor's leaf index and nothing else.

A heap re-shapes itself on every pop, and which way it re-shapes depends on the comparisons it happens to make. It would also need exhausted cursors removed from it, which would make its size depend on how much data each venue held. The merged order would then be correct because an unordered structure happened to produce ordered output, rather than because the structure cannot do anything else. That distinction is the whole argument, and it is why this package hand-rolls a tournament tree instead of calling the standard library.

## Sentinels keep the shape a function of `k` alone

`SentinelKey` is the maximum value of every field of the ordering key, so it sorts after every real key. An empty or exhausted cursor holds it, loses every match, and is never selected — so it **keeps its leaf** instead of being removed from the tree. An empty venue partition is therefore completely ordinary: it still knows its venue, still occupies a leaf, and still contributes nothing.

`NewLoserTree` rounds the leaf count up to a power of two. The leaves from `k` upward are padding that stays at `SentinelKey` forever. That rounds the tree to a perfect binary shape, so the node arithmetic in `fix` never has to branch on `k`.

Two consequences worth stating, because both look like edge cases and neither is:

- `Merger.Next` ends the stream when the winner's key is `SentinelKey`, which happens only once **every** venue is exhausted — never when the first one is. A venue that runs out early simply stops winning.
- `LoserTree.Advance` skips its ordering check when the new winner is a sentinel. Every exhausted cursor holds the same sentinel value, so comparing them would read the end of the stream as a duplicate key. Relatedly, `less` reports false for equal keys, so an incumbent keeps its node; only sentinels can ever be equal there, and which sentinel holds a node is never observable, because the merge stops as soon as one wins.

## Keys are cached in the tree

The tree stores one key per leaf, alongside the cursor index. A comparison reads two cached keys and never calls back into a `store.Reader` to decode a record a second time. `Merger` separately holds one `Event` per venue — the record that key belongs to — because the tree needs every venue's *next* key to pick a winner, which means the merge always runs one record ahead of what it emits.

## The ordering key, and why a cross-cursor duplicate is impossible

Total order is `(exchange_ts, venue_id, sequence_number, instrument_id)`; `compareKey` is the only thing that defines it. See [`format.md`](./format.md) for the key's own definition and for why `instrument_id` is part of it.

`newMerger` rejects two cursors holding the same venue with `ErrDuplicateVenue`. That single check is what makes the rest structural: **two live cursors differ in `venue_id` by construction, so they can never tie.** A duplicate key can therefore only come from inside one partition, and that narrows where the check has to happen rather than requiring a tie-break rule the project refuses to have.

One function, `checkOrder`, is called by every layer that could see the problem:

| Where | What it sees |
|---|---|
| `Cursor.Next` | two consecutive records in one file, and across a file boundary |
| `batch.fill` | two consecutive records inside one worker's batch |
| `venueFeed.checkSeam` | the seam between two consecutive batches of one venue |
| `LoserTree.Advance` | the retiring key against the key taking the root, across the whole merged stream |

A repeated key is `ErrDuplicateKey`, a key that goes backwards is `ErrOutOfOrder`. Both are hard stops. Two records sharing a key are a corrupt artifact, not a tie to break — the writer already rejects one at write time, so reaching this check means the file came from somewhere else.

## `Cursor` presents one venue as one sequence

A venue's data is usually many files. `Cursor` hides those boundaries from the merge and carries the last key seen **across** them. That matters for one specific failure: a capture window that overlaps its neighbour puts a repeated or backwards key exactly at a file boundary, which is the one place a per-file check cannot see it.

The cursor is also the only thing that walks a file in record order, so it is where each block's checksum is verified, once, on first touch — not per record.

A `Cursor` is not safe for concurrent use. One goroutine owns it, and that ownership is also what makes closing its files safe: a `store.Reader` must not be closed while anything is still reading from it.

## The worker pool decodes; it never orders

The concurrent path splits two jobs that are easy to conflate:

**Workers decode.** A fixed pool pulls `workItem`s — a reader, a start index, a count, and a batch to fill — from one shared queue. Workers are interchangeable: none carries an index, and nothing a worker produces depends on which one it is. A `batch` holds its venue, its events and its error, and deliberately **no worker id, no arena index, no goroutine-assigned counter.** Decode is a pure function of the file bytes and the record range, so nothing about which worker ran, or when, can reach the merged stream. That is what makes the worker count irrelevant to the output.

**One goroutine per venue orders.** `venueFeed.run` owns its venue's channel and is the only thing that sends on it, in file and record order — never in the order workers happen to finish. Letting a pool worker send directly would put an out-of-order stream in front of the loser tree, and nothing downstream re-sorts.

It also checks the seam between consecutive batches, because it is the only goroutine that ever sees two of them in sequence. A worker validates inside its own batch; a file boundary always falls on a seam.

Two constants shape this, and both are chosen rather than tuned:

- **1024 records per batch** matches the store's default block size, so a batch that starts at a multiple of it starts on a block boundary and each block is checksummed by exactly one worker. A file written with another block size still decodes correctly; some blocks are just verified more than once.
- **Three batches per venue**: one being decoded, one queued behind it, one in the merge's hand. They rotate through a free channel and are never reallocated, so steady-state decode allocates nothing. A venue issues work only while it holds fewer than two, leaving one for the merge — which keeps the owner's only wait the send, where backpressure from a slow merge belongs.

## A single-case blocking receive, never a `select`

When a venue's batch runs out, the merge blocks on **one** channel receive. Never a `select` across several cursors: the runtime picks pseudo-randomly among ready cases, and that pick would decide event ordering. The linter's `no-multi-select` rule enforces this for the package.

There is also no second `select` case for cancellation, and that has a consequence worth stating plainly: **closing a merger part way through discards the run.** The channel closing is the only signal, and the events emitted so far are not a valid prefix to keep — they are the start of a run that was abandoned. Making an abort graceful would need exactly the second case this rule forbids, and the abandoned run's length would then depend on scheduling.

`Close` sets an abort flag before draining, so reader goroutines stop decoding the rest of the dataset on the way out. Draining alone would finish the run correctly but would make `Close` cost the whole remaining replay.

## An error is final

`Merger.Next` keeps returning the first error it hit. A run that rejected its input has no valid remainder to hand back. `batch.fill` follows the same rule one level down: it records the first problem and stops, and a batch whose error is set is never emitted, **not even the records before the error**.

## The seek-suffix contract

`replay(from=T)` must be an exact, record-for-record suffix of `replay(from=0)`. This is the strongest single assertion in the suite, because it exercises the sparse time index, partial partitions and the whole merge at once.

`Cursor.SeekTime` advances to the first record at or after `T` using each file's own sparse time index rather than scanning. The property that makes the result a *suffix* is that `exchange_ts` is non-decreasing within one venue — enforced by the converter at write time, see [`convert.md`](./convert.md). Records a seek skips are therefore always a contiguous run at the front of the partition, never something spread through it.

Two rules follow, both load-bearing:

- Seek **before** the first `Next`. Calling it after panics, because `Next` may already have advanced past what a later seek target would land on.
- `newVenueFeed` starts from the cursor's current file and record index, not from the beginning. That is the only thing that makes a seek take effect on the concurrent path, which never calls `Cursor.Next` at all.

`TestDeterminismSeekSuffix`, `TestDeterminismSeekSuffixWithTheWorkerPool` and `TestDeterminismSeekSuffixAcrossVenueOrder` prove it. See [`determinism.md`](./determinism.md).

## Chaos perturbation

The chaos variant shifts how the merge, the venues and the worker pool interleave, without touching a clock. Each venue's perturbation draws from its own `math/rand/v2` source seeded from `(seed, venueID)` — never from a worker index or any start-order-derived value, which would reintroduce precisely the goroutine-identity dependency the test exists to rule out. A per-venue source also avoids sharing one `*rand.Rand`, which has no internal lock and would be a genuine data race.

It perturbs with `runtime.Gosched`, never `time.Sleep`: a real sleep reads the clock indirectly through the scheduler, which this repository allows only inside `internal/clock`, and it would slow the test for nothing `Gosched` does not already give.

In every production replay the perturbation hook is nil — one nil check, with no call behind it.
