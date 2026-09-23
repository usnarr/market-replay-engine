# Documentation index

Docs explain the **why**. The code shows the **what**. See the root [`CLAUDE.md`](../CLAUDE.md) for the project overview and invariants.

| File | Scope |
|---|---|
| [`format.md`](./format.md) | The hot-tier binary format: record layout, header, indexes, checksums, the canonical hash projection. |
| [`merge.md`](./merge.md) | The merge stage: the fixed-size loser tree and why it is not `container/heap`, sentinel padding, the per-venue cursor, the worker pool and batch decode, and the seek-suffix contract. |
| [`book.md`](./book.md) | Orderbook reconstruction: the sorted-slice level container, delta semantics, the snapshot-epoch contract, epoch-based seek warm-up, and the warm-up-delta emission policy. |
| [`determinism.md`](./determinism.md) | What "deterministic" means precisely, the determinism boundary, and the first-party scoping of the `time.Now` invariant. |
| [`backpressure.md`](./backpressure.md) | `Block` and `Drop` semantics, the shared-ring design, the lapping protocol, gap reporting, the drop-oldest overflow policy. |
| [`clock.md`](./clock.md) | The `Clock` interface, `RealClock`, `SimClock`, pacing, the rational speed representation, and the release-batching policy at high speed. |
| [`replayd.md`](./replayd.md) | The server and its gRPC API: the `Subscribe` contract, start positions and rational speed on the wire, the status-code mapping, the pinned flow-control windows, the metrics, and `cmd/replayd`'s flag surface. |
| [`convert.md`](./convert.md) | The archive-tier converter: the canonical source Parquet schema, the venue partitioning policy, snapshot epoch placement, byte-reproducible artifacts, the artifact content hash, and what `cmd/convert` rejects rather than repairs. |
| [`no-database.md`](./no-database.md) | Why the project never adds a database, and the run-manifest handoff between `replayd` and `cmd/catalogue` that keeps that true. |
| [`bench.md`](./bench.md) | How performance is measured and defended: the allocation gate's design, the profile-first rule, `BENCHMARKS.md`'s append-only protocol, and the end-to-end load harness. |

Each file's content is written once, by the milestone that first needs it, and updated in place afterward — never duplicated into a second file on the same topic.
