# Determinism guarantees

Placeholder. Filled in during M9 (`cmd/replayd`).

Scope: precisely what "deterministic" means for this project, where the determinism boundary sits (the in-process fan-out interface, not the gRPC wire), and the first-party-only scoping of the `time.Now` invariant — `grpc-go`, `prometheus/client_golang`, and the Go runtime itself call `time.Now` internally and are outside this repository's control.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md).
