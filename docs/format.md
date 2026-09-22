# Binary format

Scope: the hot-tier binary format's record layout, header, sparse time index, snapshot position index, per-block checksums, the sidecar blob region for variable-length snapshot data, and the versioned canonical hash projection used by the determinism test.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md).

## One file, one venue

A file holds the records of one venue, in total order. A venue's data usually spans many files, one per day. Grouping them is `cmd/convert`'s job, not this format's.

Every file is written once and never changed. `internal/store.Writer` creates the file, appends records in order, and finalizes it. Nothing reopens a finalized file for writing.

## A fixed stride, and the contradiction it creates

Record `i` starts at `header_size + 64*i`. A record index converts to a file offset by arithmetic alone. Every index in this format depends on that.

A 50-level orderbook snapshot needs at least 800 bytes of level data. It cannot fit a 64-byte record. A fixed stride and inline variable-length snapshots cannot both be true.

The format resolves this with a **sidecar blob region**. A snapshot appears in the record array as a fixed-width pointer record. Its level data lives in a separate contiguous region of the same file. Two alternatives were rejected:

- Continuation records spanning several slots. Every index probe would have to walk backwards to find where an object starts.
- Variable-width records. This loses O(1) record indexing, which the whole design rests on.

## The record

64 bytes, little-endian. The four ordering-key fields come first, in key order, so a key comparison reads only the leading 22 bytes.

| Offset | Size | Field | Type | Notes |
|---|---|---|---|---|
| 0 | 8 | `exchange_ts` | int64 | nanoseconds, the same representation `Clock.Now` uses |
| 8 | 8 | `sequence_number` | uint64 | per-venue sequence space; gaps are normal |
| 16 | 4 | `instrument_id` | uint32 | |
| 20 | 2 | `venue_id` | uint16 | |
| 22 | 1 | `record_type` | uint8 | 0 delta, 1 trade, 2 snapshot pointer |
| 23 | 1 | `side_flags` | uint8 | bit 0 is the side; bits 1-7 are reserved and must be zero |
| 24 | 8 | `price` | int64 | scaled integer |
| 32 | 8 | `size` | int64 | scaled integer |
| 40 | 8 | `blob_offset` | uint64 | snapshot pointer only; relative to `blob_region_offset` |
| 48 | 4 | `blob_len` | uint32 | snapshot pointer only |
| 52 | 2 | `level_count` | uint16 | snapshot pointer only |
| 54 | 10 | reserved | — | must be zero |

There is one layout for every record type, not a union with type-dependent offsets. This costs a little density. It keeps decode a single straight-line function that never branches on layout.

**Every byte is defined.** A field a record type does not use must be zero. A reader rejects a record that sets one. Without that rule two files that mean the same thing could differ in bytes nothing reads, and `canonical_v1` would hash them differently.

### Encode and decode one field at a time

Use `encoding/binary`'s `LittleEndian` functions. Never cast a Go struct onto the file bytes, and never use `binary.Read` or `binary.Write`.

A struct cast lets compiler-inserted padding into the file, and that padding is whatever the allocator last left there. `binary.Read` and `binary.Write` use reflection and allocate. Either one would put bytes into a file that nothing defines, and the hash over that file is the only claim this project makes.

### Scaled integers, never floats

`price` and `size` are `int64`, divided by the `price_scale` stored in the header. A `float64` can be NaN, which is not equal to itself. That breaks the comparator, and it breaks the hash. An `int64` is exact, orderable and hashable.

## The ordering key

Total order is `(exchange_ts, venue_id, sequence_number, instrument_id)`.

`instrument_id` is the final tie-break. One venue can emit the same sequence number for two instruments at the same timestamp, so the first three fields alone are not unique.

The writer rejects a record whose full key does not strictly increase. It does not check `sequence_number` on its own: gaps and per-gateway resets are normal in real venue data, and the key as a whole is what has to increase. An equal key is a data problem to fix upstream, and it is far cheaper to find at write time than during a six-hour replay.

`exchange_ts` is non-decreasing within a venue. The converter enforces this, and rejects source data that violates it rather than repairing it. Canonical replay order is `exchange_ts`-first, but orderbook deltas must apply in venue-sequence order. If `exchange_ts` could go backwards while `sequence_number` still increased, canonical order would apply deltas out of sequence and silently corrupt the reconstructed book.

## The header

The header's defined fields occupy 128 bytes. Zero padding then runs out to `header_size`, which is where record 0 starts.

`header_size` is `os.Getpagesize()` at write time, rounded up to cover the fields. The writer stores the value it used, so a reader on a host with a different page size still finds record 0. Nothing assumes 4096: macOS on Apple silicon uses 16384-byte pages.

A writer may also pin `header_size` instead of reading it from the host, through `store.NewWriterWithOptions`. `cmd/convert` does, for both `header_size` and `block_size_records`, because "the same input converts to the same bytes" cannot hold otherwise: a conversion on a 16384-byte-page host would put record 0 somewhere a 4096-byte-page host never would, and every offset in the file after it would shift. Pinning a page size costs a reader nothing — it reads `header_size` from the header either way.

| Field | Type | Notes |
|---|---|---|
| `magic` | [8]byte | `\x89RPL\r\n\x1a\n` |
| `format_version` | uint32 | exact match; a reader never guesses at compatibility |
| `venue_id` | uint16 | every record in the file carries this venue |
| reserved | — | bytes [14, 16); must be zero |
| `price_scale` | int64 | positive power-of-ten divisor for `price` and `size` |
| `record_count` | uint64 | |
| `min_exchange_ts` | int64 | zero when the file holds no records |
| `max_exchange_ts` | int64 | zero when the file holds no records |
| `header_size` | uint32 | the offset of record 0 |
| `block_size_records` | uint32 | records per checksummed block |
| `blob_region_offset` | uint64 | |
| `blob_region_len` | uint64 | |
| `time_index_offset` | uint64 | |
| `time_index_count` | uint64 | one entry per block |
| `snapshot_index_offset` | uint64 | |
| `snapshot_index_count` | uint64 | |
| `footer_offset` | uint64 | |
| `trailer_crc32c` | uint32 | covers both indexes and the footer |
| reserved | — | bytes [116, 120); must be zero |
| `finalized` | uint8 | 0 or 1 |
| reserved | — | bytes [121, 124); must be zero |
| `header_crc32c` | uint32 | covers bytes 0 to 120 |

Nine reserved bytes, across those three ranges. `internal/store` validates every one of them as zero and returns `ErrReserved` otherwise, the same rule the record layout follows: every byte is defined, so two headers that mean the same thing cannot differ in bytes nothing reads. The ranges exist because each following field is aligned to its own width — `price_scale` to 8, `finalized` after the checksummed prefix, `header_crc32c` to 4.

The magic ends with the PNG `\r\n\x1a\n` run. Git on Windows converts the line endings of a file it mistakes for text. That turns into a magic mismatch here, not a silently corrupt record array.

`finalized` and `header_crc32c` are the last two fields, in that order, so the header checksum covers one contiguous range that excludes both by construction. A checksum cannot cover itself, and the writer sets the finalized byte after the checksum is already on disk.

### The regions are exact

A file is exactly its regions, in this order:

```
header | records | blobs | time index | snapshot index | footer
```

Each region starts exactly where the previous one ended, and the footer ends exactly at the end of the file. A reader that merely required the offsets to increase would accept gaps and trailing bytes, which would let two different byte strings describe the same file. `cmd/convert` must produce byte-identical artifacts, so that cannot be allowed.

A reader checks every bound before it allocates anything sized from a header field. Each extent check is written as a subtraction against the remaining headroom, never as `a + b`, so a hostile header cannot wrap uint64 into a region that looks valid.

## Finalization

A writer finishes a file in exactly this order:

1. the record array
2. the blob region
3. the time index, the snapshot index, the footer
4. the header, with the finalized byte clear
5. fsync
6. the finalized byte
7. fsync

A reader rejects a file whose finalized byte is clear. A writer that dies at any earlier point therefore leaves a file nothing will read, rather than one whose header advertises an index that was never written. The cost is one byte and two fsyncs.

There is no directory fsync. Windows has no equivalent. The durability this gives is "the file's own bytes reached the disk", not "the directory entry did".

## The two indexes

The **sparse time index** is a sorted `[]int64` holding one timestamp per block: the `exchange_ts` of that block's first record. It stores timestamps only, never `(timestamp, offset)` pairs. With a fixed stride the offset is arithmetic once the record index is known, so storing it again would be redundant.

Its entries are **non-decreasing, not strictly increasing**. One venue repeats a timestamp, and a run of equal timestamps can straddle a block boundary or be longer than a whole block.

The **snapshot position index** is a sorted `[]uint64` of the record indexes that are snapshot pointers. Its entries strictly increase, and each must name a record the file holds. A time seek needs it: seeking to `T` finds the record at or after `T`, rewinds to the most recent snapshot at or before that point, and replays deltas forward to reconstruct a valid book. Without this index, "the last snapshot before `T`" has no efficient answer.

A reader checks both orderings once, at open. A binary search over an unsorted index returns an arbitrary answer instead of failing.

## Seeking

`SeekTime(t)` returns the index of the first record whose `exchange_ts` is at or after `t`. It returns the record count when every record is earlier. That is a valid end cursor, not an error: the common caller is "replay from `t` to the end", and an error there would force every call site to special-case the last record.

Because the index is non-decreasing, the obvious algorithm is wrong. Finding the last block that starts at or before `t` and scanning inside it skips matches: the first record holding `t` can sit in the block *before* the one the search lands on. The correct search finds the first index entry **at or after** `t`. The entry before that one is then strictly earlier than `t`, which bounds the scan to that block plus one record.

Seek cost is linear in `block_size_records`, because the index narrows the answer to one block and the rest is a scan. See `BENCHMARKS.md` for the measurement that sets the default block size.

## Snapshot epochs

At each epoch the converter emits a snapshot pointer for every instrument active in that venue, at the same point in the record stream.

This bounds seek cost to "rewind to one epoch, then replay forward". The alternative is per-instrument warm-up, where each instrument becomes book-valid independently and the read API has to expose a "book not yet valid for instrument X" state. Epochs cost more storage, including a full snapshot for quiet instruments. They keep one epoch index able to answer the seek question for every instrument at once.

## The blob region

A snapshot's levels live in the blob region. `blob_offset` is relative to `blob_region_offset`, so a record can be encoded before the region's own position in the file is known.

```
offset 0: crc32c     uint32   over bytes [4, blob_len)
offset 4: bid_count  uint16
offset 6: ask_count  uint16
offset 8: levels     (bid_count + ask_count) x 16 bytes: price int64, size int64
```

Bids come first, then asks. A level carries no side of its own: two counted runs are denser than a side byte per level, and leave no undefined padding.

`blob_len` is `8 + 16*(bid_count + ask_count)`, and `level_count` is `bid_count + ask_count`. Both relationships are checked on read, so a reader can reject an inconsistent snapshot pointer without touching the blob at all.

## The integrity chain

Nothing in this format is unprotected, and nothing is checked before it is needed:

| Region | Covered by | Checked |
|---|---|---|
| header | `header_crc32c` | at open |
| time index, snapshot index, footer | `trailer_crc32c` in the header | at open |
| record array | one CRC-32C per block, in the footer | per block, on first touch |
| a snapshot's levels | the blob's own CRC-32C | when that blob is read |

The record array is checksummed **per block, not per file**. Reading record 0 must not require touching the last byte of a multi-gigabyte file. The final block is short whenever `record_count` is not a multiple of the block size, and is never padded out: its checksum covers exactly the records it holds. Each footer entry records where its block starts, and a reader rejects a footer whose entries disagree with the block size, so a hostile file cannot describe overlapping blocks.

The trailer checksum exists because the block checksums cover the record array only. Without it a single flipped byte in the snapshot index would silently point a seek at the wrong epoch. The trailer is small and read in full at open, so checking it costs nothing extra.

A blob is not covered by any block checksum, because a seek reads a blob without touching the block its pointer record lives in.

**Verification is the caller's to schedule.** `Reader.RecordAt` does not verify: a branch on every record access would cost more than it buys, and it would make an allocation-free return impossible. Callers call `Reader.VerifyBlock` once per block instead. The consequence is that a seek's answer means something only for a file whose blocks verify, because the index is trusted and a record array that contradicts it is corruption.

## The canonical projection

The determinism test hashes an explicit, versioned field projection. It does not hash raw file bytes or raw mmap bytes.

```
canonical_v1(record) = LE(exchange_ts) || LE(sequence_number) || LE(instrument_id)
                     || LE(venue_id) || record_type || side_flags
                     || LE(price) || LE(size)
                     || [if snapshot pointer: LE(level_count) || blob payload]
```

The blob payload is the blob without its four-byte checksum prefix: the counts and the levels.

`blob_offset` and `blob_len` are deliberately absent. They say where a snapshot sits in one particular file, not what it holds. Two conversions of the same session that pack blobs differently must still match.

Any capture-time field, such as a `recv_ts` added at the source-capture stage, is excluded for the same reason. Two independent captures of the same market session would otherwise never match, which would make "deterministic" mean nothing to a user comparing two datasets.

A projection is also future-proof in a way raw bytes are not. A format version that repurposes the reserved bytes would otherwise break every historical baseline for a field that was never semantically meaningful.

### Framing

Each record's canonical bytes are length-prefixed before they reach the hash. The run ends with the record count and a fixed terminator:

```
per record:  uint32 LE length || canonical bytes
at the end:  uint64 LE record_count || "CANONv1\x00"
```

Without this framing two different record sequences can concatenate to the same byte stream — a snapshot with a long payload against a snapshot with a short one followed by another record. Their digests would then collide, and that collision would be a bug in the test rather than in the data.

### The hash

**CRC-32C**, via `hash/crc32.MakeTable(crc32.Castagnoli)`, always on. It is in the standard library and has a hardware instruction on amd64 and arm64, so it does not become the throughput bottleneck.

**SHA-256** is available behind an explicit flag, for artifact attestation. It is never the default. It runs at roughly a third of CRC-32C's rate on this projection, which would make the always-on hash the bottleneck instead of the merge — the same failure this project warns about for the merge itself.

**Never use `hash/maphash`.** Its seed is randomized per process by design, which would make the digest worthless for comparing two runs. `cmd/lint-determinism` rejects the import.

## Operational constraints

**64-bit only.** A 32-bit address space cannot map files in the size range this engine targets.

**A finalized file is immutable.** Never write to a file that is mapped for reading anywhere. If a mapped file is truncated or modified, unix delivers `SIGBUS` and Windows delivers `EXCEPTION_IN_PAGE_ERROR`. Neither is a panic a program can recover from. The mitigation is procedural, not code: treat artifacts as immutable once finalized, and verify the size and header at open.

**A `Reader` is immutable once `Open` returns**, so any number of goroutines may read from it at once. `Close` is the exception, and ordering it is the caller's responsibility: it must not run while another goroutine is still reading. Synchronising that inside the reader would cost the hot path something the merge stage's own lifecycle already prevents, since it closes a cursor's reader only after draining it.

**One `unsafe` conversion exists, on Windows only.** `MapViewOfFile` returns a bare address, so `mmap_windows.go` turns it into a byte slice. This is not the struct-over-file-bytes cast the format rules forbid: no Go type is laid over file data, and every field is still decoded one at a time. `go vet`'s `unsafeptr` check is disabled for `internal/store` alone because of it, and `make lint` says so.

**A blob returned by `Reader.Blob` aliases the file image.** It stays valid until `Close`. Once the file is memory-mapped, touching it afterwards faults rather than panics, so a caller that needs it for longer must copy it.
