# Documentation index

Docs explain the **why**. The code shows the **what**. See the root [`CLAUDE.md`](../CLAUDE.md) for the project overview and invariants.

| File | Scope |
|---|---|
| [`format.md`](./format.md) | The hot-tier binary format: record layout, header, indexes, checksums, the canonical hash projection. |
| [`book.md`](./book.md) | Orderbook reconstruction: the sorted-slice level container, delta semantics, the snapshot-epoch contract, epoch-based seek warm-up, and the warm-up-delta emission policy. |
| [`determinism.md`](./determinism.md) | What "deterministic" means precisely, the determinism boundary, and the first-party scoping of the `time.Now` invariant. |
| [`backpressure.md`](./backpressure.md) | `Block` and `Drop` semantics, the shared-ring design, the lapping protocol, gap reporting, the drop-oldest overflow policy. |
| [`clock.md`](./clock.md) | The `Clock` interface, `RealClock`, `SimClock`, pacing, the rational speed representation, and the release-batching policy at high speed. |
| [`convert.md`](./convert.md) | The archive-tier converter: the canonical source Parquet schema, the venue partitioning policy, snapshot epoch placement, and what `cmd/convert` rejects rather than repairs. |
| [`no-database.md`](./no-database.md) | Why the project never adds a database, and the run-manifest handoff between `replayd` and `cmd/catalogue` that keeps that true. |

Each file's content is written once, by the milestone that first needs it, and updated in place afterward — never duplicated into a second file on the same topic.
