# Why no database

Scope: why this project never adds a database on the replay path, and the run-manifest file handoff between `replayd` and `cmd/catalogue` that makes "SQLite is not touched during a replay" true by construction, not just by convention.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md).

## The replay path has no room for one

The merge and fan-out stages work in tens of nanoseconds per record. A loopback query to a database process is measured in tens of microseconds — three orders of magnitude more, for a single call. A replay that touched a database once per record would be a thousand times slower than one that did not, and a replay that touched one once per second would still be a replay whose timing depends on another process's scheduler.

That second point is the one that actually settles it. Throughput is negotiable in this project; determinism is not. A network hop introduces a source of variation nothing in this repository can control: connection setup, query planning, lock contention, a background checkpoint. None of it is reproducible, and all of it would sit inside the window a paced replay is trying to hit. This is not a trade-off to weigh against a feature. It is out of scope.

The data itself has no need for one either. A replay reads a memory-mapped file of fixed-stride records with two sorted indexes in the same file (`format.md`). That *is* the storage engine, built for exactly one access pattern, and a general-purpose one would be slower at it while offering query shapes nothing here asks for.

## SQLite is the one exception, and it is offline

The run catalogue and the benchmark history are real needs: "which artifact did this run read, and what did it produce" and "did this commit change a measured number" are questions worth keeping answers to. They are also questions asked *after* a run, by a person, never during one, by the engine.

So `cmd/catalogue` owns them, in its own SQLite file, as a separate program that runs at whatever cadence an operator chooses.

## The handoff is a file

```
replayd  ──writes──▶  run manifest (JSON on disk)  ──reads──▶  cmd/catalogue  ──writes──▶  SQLite
```

`replayd` writes a plain JSON file when a run ends and stops there. It has no idea `cmd/catalogue` exists. `cmd/catalogue` scans a directory of those files later, offline, and is the only program in this repository that ever opens SQLite.

This is a mechanism, not an intention. Three things make it structural:

- **Separate modules.** `cmd/catalogue` has its own `go.mod` and does not require the `replay` module. The root module cannot import it, and it cannot import the root module's packages.
- **A duplicated struct, on purpose.** `cmd/catalogue`'s `Manifest` is a hand-written mirror of the shape `cmd/replayd/manifest.go` writes, with matching JSON tags, rather than a shared type. Sharing the type would mean sharing a module, which is precisely the edge this design refuses to draw. The duplication is what keeps the dependency graph honest, and the manifest round-trip test is what keeps the two in step.
- **`replayd` links no SQLite driver.** Not "does not call one" — does not contain one.

### Checking it

From the repository root:

```
GOWORK=off go list -m all | grep -E 'modernc|parquet'
```

No output. The `replay` module requires `go-cmp`, `grpc-go`, `client_golang` and the `protobuf` runtime `grpc-go`'s generator emits imports for, and the transitive closure this command prints holds no storage engine at all.

`GOWORK=off` is not decoration. The repository root carries a `go.work` listing all four modules, and in workspace mode `go list -m all` reports the union of every module in the workspace — so without the flag the command prints `github.com/parquet-go/parquet-go` and `modernc.org/sqlite`, which says only that the workspace contains the tools, not that the server depends on them. The flag asks the question that matters: what does the `replay` module itself require.

## What the catalogue stores

Two tables, in `cmd/catalogue/schema.sql`, kept small on purpose — a column is added when a query needs it, not in case one might.

`runs` holds a run ID, the dataset artifact hash, the run's configuration, the final canonical hash, and a timing summary. The run ID is the manifest file's own name: a manifest carries no ID, and the operator already chose that name. The dataset hash is one SHA-256 over every dataset file's path and content hash in manifest order, so a run names the dataset it read with one value; the manifest file remains the record of which files those were, and `convert.md` explains where a file's own hash comes from.

`benchmarks` holds one row per measurement: the benchmark's name with its `-N` parallelism suffix stripped, the commit, the iteration count, `ns/op`, `B/op`, `allocs/op`, and a sample ordinal so that a `make bench BENCHCOUNT=6` run stores six comparable samples rather than one. A result file measured without `-benchmem` is rejected rather than stored with zeroes, because `allocs/op` is the number the allocation gate exists for.

`cmd/catalogue` never reads the wall clock. A run's timestamp comes from its manifest; a benchmark's comes from the result file's modification time, which is when `make bench` wrote it. The root `CLAUDE.md`'s `time.Now` rule holds here too, even though nothing this tool does is on a replay path.
