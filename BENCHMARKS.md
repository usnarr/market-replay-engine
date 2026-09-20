# Benchmarks

This file is a deliverable, not documentation. It is updated in the same commit as any change that moves a measured number.

The sequence for any performance change is: profile → hypothesise → change → measure → write it up here. A performance change with no committed before/after numbers gets reverted, however obviously correct it looks.

Each table below is append-only history, one row per committed change, not a single current-state snapshot — a regression stays visible in context instead of being silently overwritten.

<!--
## <benchmark name>

| Date | Commit | Change | Before | After | Profile |
|---|---|---|---|---|---|
| YYYY-MM-DD | abc1234 | one-line description | ... | ... | profiles/<name>.svg |
-->

## Merge throughput

_No measurements yet. First entry lands with the M4 checkpoint in `internal/merge`._

## Fan-out throughput

Environment for every row below: `go1.23.4 windows/amd64`, `GOMAXPROCS=8`, 11th Gen Intel Core i7-11370H @ 3.30GHz.

| Date | Commit | Change | Before | After | Profile |
|---|---|---|---|---|---|
| 2026-09-20 | (this commit) | M6: shared ring, atomic-word slot | — | see below | — |

```
BenchmarkRingWrite/no_blob-8               56.87 ns/op   0 B/op   0 allocs/op
BenchmarkRingWrite/with_a_snapshot_blob-8 100.20 ns/op   0 B/op   0 allocs/op
BenchmarkRingRead-8                        15.39 ns/op   0 B/op   0 allocs/op
```

The ring's slot payload is atomic words, not a plain struct: a Drop subscriber may
copy a slot the writer is concurrently overwriting, and a plain field there is a
genuine data race, not a false positive, under `go test -race`. The measured cost of
that choice is the ten sequentially-consistent stores `BenchmarkRingWrite/no_blob`
pays per record (`store.EncodeRecord` plus eight word stores plus the two `seq`
publishes) against a plain-struct assignment's one. Reads pay nothing extra on amd64:
`atomic.Uint64.Load` compiles to a plain `MOV`. See `docs/backpressure.md`.

Two profile-gated optimisations are deliberately not built yet, per the project's
profile-first rule: dropping to 7 slot words (wire bytes 56-63 of a `store.Record`
are always reserved zeros), and skipping the atomic path entirely when the
subscriber set holds no Drop subscriber (the Block barrier then already guarantees
exclusivity). Revisit only if a profile shows ring writes are the pipeline's
bottleneck.

## Encode/decode throughput

Environment for every row below: `go1.23.4 windows/amd64`, `GOMAXPROCS=8`, 11th Gen Intel Core i7-11370H @ 3.30GHz. Numbers from a different machine are not comparable to these.

| Date | Commit | Change | Before | After | Profile |
|---|---|---|---|---|---|
| 2026-09-19 | (this commit) | M2 baseline: record codec, reader access, blob decode, canonical hash | — | see below | — |

```
BenchmarkEncodeRecord-8          4.157 ns/op   15395 MB/s   0 B/op   0 allocs/op
BenchmarkDecodeRecordFields-8    6.298 ns/op   10162 MB/s   0 B/op   0 allocs/op
BenchmarkDecodeRecord-8         12.600 ns/op    5081 MB/s   0 B/op   0 allocs/op
BenchmarkRecordAt/sequential-8   9.169 ns/op    6980 MB/s   0 B/op   0 allocs/op
BenchmarkRecordAt/random-8      17.820 ns/op    3591 MB/s   0 B/op   0 allocs/op
BenchmarkBlob/payload-8         45.120 ns/op   17908 MB/s   0 B/op   0 allocs/op
BenchmarkBlob/append_levels-8  149.600 ns/op                0 B/op   0 allocs/op
BenchmarkCanonicalV1/delta-8    17.130 ns/op    2335 MB/s   0 B/op   0 allocs/op
BenchmarkCanonicalV1/snapshot-8 58.760 ns/op   14398 MB/s   0 B/op   0 allocs/op
BenchmarkCanonicalV1/delta_sha256-8  44.590 ns/op  897 MB/s  0 B/op  0 allocs/op
```

`decodeRecordFields` is the reader's hot path and skips validation; `decodeRecord` pays for the reserved-byte and structural checks and is used only on input that has not been through a block checksum.

SHA-256 runs at 897 MB/s against CRC-32C's 2335 MB/s on the same 40-byte projection — a 2.6× cost per record. That is why CRC-32C is the default canonical hash and SHA-256 is opt-in, for attestation only.

The M7 allocation gate is not wired in yet, but every hot-path benchmark already reports zero allocations.

### Block size

`block_size_records` was 4096 in the plan, as a guess. This measurement set it to **1024**.

| Block size | VerifyBlock | Throughput | Metadata | SeekTime |
|---|---|---|---|---|
| 64 | 146.1 ns | 28029 MB/s | 0.488% | 376.6 ns |
| 256 | 595.7 ns | 27503 MB/s | 0.122% | 1297 ns |
| **1024** | **2525 ns** | **25958 MB/s** | **0.031%** | **4944 ns** |
| 4096 | 9454 ns | 27730 MB/s | 0.008% | 19270 ns |
| 16384 | 40686 ns | 25772 MB/s | 0.002% | — |

Verification throughput is flat at 26-28 GB/s across a 256× range of block sizes, so it does not constrain the choice at all — CRC-32C is hardware accelerated and there is no per-call overhead worth amortising past 64 records. The real trade-off is the other two columns, which move in opposite directions: metadata is 20 bytes per block (one footer entry plus one time index entry), while seek cost is linear in the block size, because the index narrows the answer to one block and the rest is a scan.

1024 records is 64 KiB. Metadata is 0.03% of the file, seek is under 5 microseconds, and the block matches Windows' 64 KiB mapping allocation granularity.

## Pacing accuracy

_No measurements yet. First entry lands with M8 pacing._

## End-to-end replay rate

_No measurements yet. First entry lands once `cmd/replayd` (M9) can run a full synthetic dataset end to end._
