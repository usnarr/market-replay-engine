# The replayd server and its gRPC API

Scope: `api/replay.proto`'s `Subscribe` contract, how a start position and a speed are represented on the wire, the status-code mapping, the pinned gRPC flow-control windows, the metrics `replayd` exposes, and the command's own flag surface.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md).

## `replayd` is an assembly, not a layer of logic

`cmd/replayd` opens the hot-tier files, builds one `merge.Cursor` per venue, wires a `merge.Merger` into a `fanout.Ring`, and translates between those types and the wire schema. It holds no replay logic of its own: everything below the translation lives in `internal/*`, so the core packages stay testable with no gRPC server in the loop.

`clock.RealClock` is constructed here and nowhere else in the command. Everything downstream takes the `clock.Clock` interface. "`time.Now` appears exactly once" is a first-party rule; grpc-go and the Go runtime read real time internally and are outside this repository's control — see [`determinism.md`](./determinism.md) for the exact scope of that exception.

`NewServer` does **not** start emitting. The first accepted `Subscribe` does, so a subscriber asking for the beginning of the stream actually receives it.

## One RPC

```protobuf
rpc Subscribe(SubscribeRequest) returns (stream SubscribeResponse);
```

The request fixes three things before the first record is delivered, and none of them can change afterward: the backpressure mode, the start position, and the run's speed.

There is **no `map<>` field anywhere in the schema, and there must never be one.** Protobuf leaves map field marshaling order undefined, which would put nondeterminism back at the one boundary that looks like a message definition rather than like code. Where a map would be the obvious shape, the schema uses a repeated field in a stated order.

### Mode is explicit, because proto3 has no field presence here

`Mode` is `MODE_UNSPECIFIED = 0`, `MODE_BLOCK = 1`, `MODE_DROP = 2`. The server rejects `MODE_UNSPECIFIED` rather than defaulting it.

This is not defensive coding, it is forced by the combination of two facts: proto3 gives an unset scalar field the zero value, and this project's backpressure invariant says the mode is explicit at subscribe time. If `MODE_BLOCK` were zero, an unset field would be indistinguishable from a deliberate Block request, and "never a default that silently picks one" would be false for every client that forgot the field. Reserving zero for "unspecified" is what keeps the invariant true on the wire as well as in `internal/fanout`. See [`backpressure.md`](./backpressure.md).

### `StartAt` is a oneof, so the kind is on the wire

The four cases mirror `internal/fanout`'s four constructors exactly:

| Wire field | Type | `internal/fanout` | Meaning |
|---|---|---|---|
| `beginning` | `StartAtBeginning` | `Beginning()` | the first record the ring still holds |
| `exchange_ts` | `int64` | `ExchangeTs(t)` | the first record at or after `t` |
| `emit_index` | `uint64` | `EmitIndex(i)` | that global emit index; a future one is legal, the subscriber waits |
| `live` | `StartAtLive` | `Live()` | whatever the write index is at registration |

`StartAtBeginning` and `StartAtLive` are empty messages rather than an enum value or a bare bool. A start position that takes no parameter is still its own oneof case, which keeps "which start kind" explicit on the wire instead of inferred from a zero value — the same reasoning as `MODE_UNSPECIFIED`. An unset oneof is rejected.

`live` is the one start position this project does not claim is reproducible: its exact starting point depends on the wall-clock timing of the subscribe call.

### Speed is two integers, never a double

`speed_num` and `speed_den` are `int64`. They go straight to `fanout.NewSpeed`, which reduces them to lowest terms.

A `double` on the wire would reintroduce, one layer out, exactly the rounding hazard `fanout.Speed`'s exact-rational design exists to remove. [`clock.md`](./clock.md) has that argument in full; the short form is that Go fuses floating-point multiply-add on some target architectures and not others, so the same expression can round differently per build — and pacing timing decides when a Drop subscriber gets lapped, which makes it observable in gap output. There is no float anywhere in this schema, including `Level`, whose price and size are scaled integers because a double can be NaN.

One speed per run, not per subscriber. The first accepted request sets it; the server runs one paced emit loop. A later request naming a different speed is rejected rather than silently ignored.

## `Record` is a projection, never the raw blob

`Record` carries the format's own values, unchanged, field by field. The proto numeric widths are wider than the format's — `venue_id`, `record_type`, `side_flags` and `level_count` are a `uint16`, two bytes and a `uint16` on disk — so a client can rebuild the canonical projection from this message alone. See [`format.md`](./format.md).

For a snapshot pointer, `levels` holds every level **already decoded**, bids first and then asks, with `bid_count` saying where the split is. Never the raw blob bytes: shipping those would nest this project's private binary format inside its public schema and leave every client to parse it.

`Delivery.Blob` aliases the subscriber's own buffer and is invalid after that subscriber's next `Next`, so the server copies every byte out before its send loop goes round again.

## A gap travels with the record that follows it

`SubscribeResponse` is one delivery: a record, plus the gap that immediately preceded it when the writer had already lapped this subscriber. `has_gap` says whether the three gap fields mean anything, and `missed_count` is always `last_missed_index - first_missed_index + 1`.

The gap unit is the run's **global emit index**, never a venue sequence number: a gap in a merged multi-venue stream spans several venues and has no single sequence range that describes it. A gap is delivery framing and is never part of the canonical hash. See [`backpressure.md`](./backpressure.md).

## Status codes

Each code says something different about what went wrong, so none of them collapses into `Internal`.

Registration failures, from `subscribeStatus`:

| Condition | Code |
|---|---|
| `fanout.ErrModeUnset`, `fanout.ErrStartUnset` | `InvalidArgument` |
| `fanout.ErrStartLapped` | `OutOfRange` |
| `fanout.ErrClosed`, a speed that disagrees with the run's | `FailedPrecondition` |
| anything else | `Internal` |

`OutOfRange` for `ErrStartLapped` is the precise answer: the client asked for a position the ring no longer holds, which is a statement about where the data is, not about the request being malformed. A Block subscriber gets this because Block promises no loss and the ring cannot reproduce what it has overwritten; a Drop subscriber never needs it, because its first read runs the ordinary lapping protocol and reports the exact gap.

Request translation failures — an unspecified mode, an unset start oneof, a speed `NewSpeed` rejects — are `InvalidArgument` before registration is even attempted.

Delivery failures, from `deliveryStatus`:

| Condition | Code |
|---|---|
| `fanout.ErrCanceled` **and** the stream context is done | the context's own status |
| `fanout.ErrCanceled` with a live context | `Aborted` |
| `fanout.ErrEvicted` | `Aborted` |
| anything else | `Internal` |

The first row is what keeps a client's own disconnect from being reported as a server fault. grpc-go's stream context is the one place this server observes "the client went away", and all it can do is stop that one subscriber — which `internal/fanout` defines as out of band, since cancelling one subscriber cannot change what any other receives. The watcher goroutine that does this uses a single-clause receive, never a `select`; the handler's own return cancels the context, so the goroutine always ends.

## Flow-control windows are pinned, and that is a determinism fix

```go
initialWindowSize     = 1 << 20  // 1 MiB
initialConnWindowSize = 1 << 20
readBufferSize        = 1 << 19  // 512 KiB
writeBufferSize       = 1 << 19
```

Left unset, grpc-go sizes the HTTP/2 flow-control window with a **BDP estimator that times its own ping round trips** — a real-time read inside the transport, sitting on the path that decides when a Block subscriber's backpressure actually engages. Passing an explicit initial window sets grpc-go's `StaticWindowSize`, which turns that estimator off for the connection's whole life.

Two details that make this work rather than merely look right:

- grpc-go **ignores an initial window below 64 KiB**, so the value has to clear that threshold to mean anything at all. 1 MiB does.
- The client half matters equally. A client that dials without the matching options leaves its own receive window estimated, which puts the real-time read back on the other end of the same stream. `grpcDialOptions` exists for that, and the integration test dials with it.

Nothing depends on the exact numbers, only on their being pinned instead of estimated. See [`determinism.md`](./determinism.md).

## Metrics

Served in Prometheus text format from `MetricsHandler`, on `-metrics-listen`. Each server keeps its own registry rather than the package default, so two servers in one process never collide.

| Metric | Kind | Meaning |
|---|---|---|
| `replay_records_emitted_total` | counter | records delivered to subscribers |
| `replay_bytes_emitted_total` | counter | record and snapshot payload bytes, before gRPC's own framing |
| `replay_gaps_detected_total` | counter | gaps reported to Drop subscribers |
| `replay_records_missed_total` | counter | records inside reported gaps, summed |
| `replay_subscribers` | gauge | subscribers currently attached |
| `replay_emit_index` | gauge func | the next emit index the writer will assign |
| `replay_pacing_slip_nanoseconds` | gauge func | cumulative nanoseconds parked at the Block barrier |

Three deliberate shapes here:

- **Counters and gauges only, never a histogram.** A histogram observation needs a clock read to bucket by, and a clock read on the replay path is banned.
- **Concrete instruments, never a `*CounterVec` looked up by label.** `WithLabelValues` does a map lookup and builds a label slice on every call — an allocation and a data-dependent cost on a path that permits neither. Every instrument is resolved once at construction.
- **The last two are `GaugeFunc`s over the ring's own atomics**, read when Prometheus scrapes rather than written per record, so they cost the send loop nothing at all.

The byte counter measures the record and its payload, not the framed wire size, which depends on gRPC's codec and would cost a second pass over every message to measure.

## The command's flag surface

| Flag | Default | Meaning |
|---|---|---|
| `-file` | — | a hot-tier file to replay; repeat once per file |
| `-listen` | `127.0.0.1:0` | address to serve gRPC on |
| `-metrics-listen` | empty | address to serve Prometheus metrics on; empty disables them |
| `-workers` | 4 | merge decode worker count; changes concurrency only, never the stream |
| `-capacity` | 1024 | fan-out ring slot count; a power of two |
| `-max-blob-bytes` | 4096 | longest snapshot payload the ring will carry |
| `-from-ts` | unset | replay only records at or after this exchange timestamp |
| `-watchdog-ns` | 0 | evict a Block subscriber that holds the writer this long with no progress; 0 disables |
| `-manifest` | empty | write the run manifest here when the run ends |

`-file` is repeatable rather than one comma-separated string, because a path may legally contain a comma — and the server sorts the files itself anyway.

`-from-ts` is read through `flag.Visit`, not from its value: a seek to timestamp zero is a real request, so "was it set" cannot be recovered from the value alone. The seek is applied to each cursor **before its first `Next`**, which is what `Cursor.SeekTime` requires and what makes the merge's seek-suffix property hold for the whole run. It is a property of the run, and a different thing from a subscriber's own start position, which is resolved against the ring.

## File order comes from content, not from the command line

Files are grouped into cursors by the venue each file's own header declares, then ordered by venue id, then by first exchange timestamp, with the path as the final tie-break. The same set of files therefore produces the same cursor list however the command line happened to order them. A sorted slice, never a map.

## The run manifest

On a clean end, `replayd` writes a plain JSON manifest to `-manifest`: the dataset's files with the content hash `cmd/convert` stamped beside each one, the run's config (speed as `num`/`den`, never a float), the subscriber mix as **counts** rather than a list or a map, and the result — emit index at end, pacing slip.

`replayd` writes a file and stops there. It never imports `cmd/catalogue` and never links a SQLite driver, which is what keeps "SQLite is not touched during a replay" true by construction rather than by intent. See [`no-database.md`](./no-database.md).

## What covers the transport

`TestIntegrationGRPCHashMatchesInProcess` starts a real server, connects a real client, and asserts the canonical hash the client computes from received messages equals the in-process merged hash for the same dataset. Its name deliberately does not begin with `TestDeterminism`, so `make determinism` never sweeps it into the core suite — the determinism boundary is the in-process fan-out interface, and the wire format sits outside it. See [`determinism.md`](./determinism.md).
