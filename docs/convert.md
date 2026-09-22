# The archive-tier converter

Scope: `cmd/convert`'s canonical source Parquet schema, its venue partitioning policy, where it places snapshot epochs, what makes its artifacts byte-reproducible, the content hash that identifies one, and what it rejects rather than repairs.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md). The output format is [`format.md`](./format.md).

## Its own module, its own dependency

`cmd/convert` is a separate Go module. A Parquet reader is not on the server module's approved dependency list, and the server has no reason to link one: conversion is offline work that runs once per artifact, not per replay. Keeping it in its own module makes the claim checkable rather than aspirational — `GOWORK=off go list -m all` from the repository root never names a Parquet library.

The dependency is `github.com/parquet-go/parquet-go`, pinned at v0.25.1. Pure Go, so conversion cross-compiles and needs no C toolchain. v0.25.1 is the last release that builds under this repository's `go1.23.4` toolchain pin; v0.26.0 and later require go1.24.9. The pin moves when the toolchain pin moves, not before.

## The canonical source schema

**Provisional.** This schema is a starting point, not a commitment. It was designed against `store.Record`, not against real archive data, because no sample of the latter exists yet. Revisit it once real source data does: a schema that no real producer writes is worth changing.

One schema, never a configurable mapping. A mapping layer would put the question "which column was `price`?" between an artifact and its inputs, and that question has to have one answer for "the same input converts to the same bytes" to mean anything.

```
message SourceRow {
  required int64 exchange_ts     (INT(64,true));
  required int64 sequence_number (INT(64,false));
  required int32 instrument_id   (INT(32,false));
  required int32 venue_id        (INT(32,false));
  required int32 record_type     (INT(32,false));
  required int32 side_flags      (INT(32,false));
  required int64 price           (INT(64,true));
  required int64 size            (INT(64,true));
  repeated group levels {
    required int32 side  (INT(32,false));
    required int64 price (INT(64,true));
    required int64 size  (INT(64,true));
  }
}
```

The eight scalar columns mirror `store.Record`'s public fields one for one, by name. `record_type` and `side_flags` carry the same values `format.md` defines. `price` and `size` are scaled integers, divided by a `price_scale` the converter is told and the source does not carry — never floats, for the reason `format.md` gives.

`blob_offset`, `blob_len` and `level_count` have no source column. They say where a snapshot's levels sit in one particular output file, which is the writer's business. The source carries the levels themselves.

### Why three columns are wider here than in `store.Record`

`venue_id` is `uint16` in `store.Record` and `uint32` here; `record_type`, `side_flags` and `levels.side` are `uint8` there and `uint32` here. Parquet has no physical type narrower than 32 bits, and `parquet-go` writes no Go type narrower than 32 bits either, so an 8- or 16-bit column would be a schema no producer could actually write. The converter narrows each value itself and rejects one that does not fit, rather than truncating it.

### Levels, and why they are not a second file

A snapshot row carries its book in the repeated `levels` group. Every other row's group is empty. The alternative — a second Parquet file of levels, joined on the ordering key — would make a snapshot's levels reachable only through a join, and a join is one more place where "the same input" could produce two different orderings.

## One file per venue, per UTC day

**Provisional**, like the schema. `format.md` already requires one venue per file, so only the day half is a choice, and it is the one most market data archives are organized by. It keeps a single file at a size a reader can map without thinking about it. Revisit it once real source data says how the archive is actually laid out.

A file is named `venue-<venue_id>-<YYYY-MM-DD>.bin`. The date, not a day number, because the operator reading a directory listing is looking for a session.

A day boundary is UTC midnight, and the day count floors towards negative infinity rather than towards zero, so the hours before the Unix epoch are the day before it rather than sharing day 0 with the hours after.

The converter holds every partition it has opened in a slice sorted by (venue, day), searched by binary search, never in a map. `cmd/convert` is held to the root `CLAUDE.md`'s ordered-path rule exactly as the replay path is, even though it runs offline: the slice's order is the order files are finalized in and the order their paths are reported in, and a map would make both depend on Go's iteration order.

That is enforced, not left to discipline. `cmd/lint-determinism`'s `orderedPath` includes `cmd/convert`, alongside `internal/merge`, `internal/fanout`, `internal/store` and `internal/book`. `make lint` names this package explicitly, because `./...` from the repository root does not cross into a separate module. It does not get `hotPath`'s `any` and `fmt.Sprint` rules: nothing here is on a per-record hot path, and a conversion that allocates is only slower.

## Rejection is by column, never by guess

A source file whose schema is not exactly this one is rejected whole, with a `*SchemaError` naming the first offending column in the canonical schema's own declared order: a missing column, an added column, a column of the wrong type, and a column whose repetition differs are all the same class of fault. Matching in canonical order, rather than over the file's own field map, is what makes two files with the same fault produce the same message.

Nothing is inferred from a near miss. A `float64` `price` column is not "close enough" to an `int64` one, and a `recv_ts` column the canonical schema does not define is not silently dropped — `format.md` excludes capture-time fields from the canonical hash for a reason, and a converter that quietly accepted one would be deciding that question on its own.

A row whose values cannot become a `store.Record` is rejected the same way, with a `*RowError` carrying the row's ordinal in the source file and its ordering key: a `venue_id` that does not fit `uint16`, an undefined `record_type`, a reserved `side_flags` bit, levels on a row that is not a snapshot, a price or size on a row that is, and a level whose side is neither bid nor ask.

A rejected conversion leaves nothing behind. Every file it had opened is aborted and removed, because a half-converted directory is worse than an empty one: nothing downstream can tell it from a complete conversion.

## Snapshot epochs

`format.md` requires the converter to emit, at each epoch, one snapshot pointer for every instrument active in the venue, at the same point in the record stream. `book.md` calls that contiguous run an epoch run, and `book.WarmUp` reads it. This is where those records come from.

The converter keeps one `book.Book` per instrument per venue, folds every Delta record into it as it goes, and reloads it from any snapshot the source itself carries. A Trade changes no level, so it is not applied. The books live in a slice sorted by instrument ID, never a map: that order decides which sequence number each epoch record gets, and a map would make two conversions of the same input disagree.

### Where an epoch lands, exactly

The cadence is every `EpochEvery` records of one venue, 1000 by default. The cadence point is not where the epoch goes.

When the cadence comes due, the converter waits for the first record whose `exchange_ts` is strictly greater than the record before it. Call the preceding record's timestamp `T` and its sequence number `S`. The epoch's records are written at that boundary — after the record holding `T`, before the record that moved past it — each with `exchange_ts = T` and sequence numbers `S+1, S+2, …, S+N`, one per instrument in ascending instrument ID.

This is deliberately a nearest-boundary approximation rather than the cadence point itself. It is what makes the epoch's keys strictly increasing **by construction**, which is what `store.Writer` demands:

- Against the record before it: same `exchange_ts`, and `S+1 > S`. Every earlier record at `T` has a sequence number at or below `S`, because the file's own keys already increase.
- Against every record after it: their `exchange_ts` is greater than `T`, by the definition of the boundary the epoch was placed at. Nothing about their sequence numbers matters, because `exchange_ts` is compared first.
- Within the run: `S+1 < S+2 < … < S+N`.

There is no case left where an epoch record can collide with a real one, or sort before it. Splitting a run of equal timestamps would give up all three properties at once.

An epoch is written into the file holding `T`, which is not always the file the record that triggered it belongs to: when the boundary is also a day boundary, the epoch closes the previous day's file.

A snapshot pointer's sequence number is therefore converter-assigned, not source-derived. Nothing downstream reads that value for anything but ordering: `format.md`'s canonical projection hashes it, so two conversions of the same input must assign the same numbers — which this rule does — but no consumer attaches meaning to the number itself.

### A source whose book cannot be rebuilt is rejected

A Delta that removes a price level the converter has not seen returns `book.ErrLevelNotFound`, and the conversion stops with a `*RowError` naming that row. This is the reject-not-repair rule again: an epoch computed from a book that has already diverged from the venue's is worse than no artifact, because nothing downstream can tell the two apart. A source that starts mid-session therefore has to start from a snapshot per instrument.

## Byte-reproducible artifacts

The same source file, converted twice, produces byte-identical output. This is a requirement, not an aspiration: the whole-file hash in a run manifest identifies an artifact, and an identity that changes when nothing about the data did identifies nothing.

Three things make it true.

**The file geometry is pinned, not taken from the host.** `store.NewWriter` sets `header_size` from `os.Getpagesize()`, which is 4096 on most hosts and 16384 on Apple silicon. Record 0's offset then decides where every later byte sits, so the same input would convert to different bytes on different machines. `cmd/convert` uses `store.NewWriterWithOptions` and pins `header_size` to 4096 and `block_size_records` to 1024 instead. A reader is unaffected: it reads both values out of the header either way.

**Nothing about the output order is timing-dependent.** The converter reads row groups in file order on one goroutine; there is no decode fan-out whose completion order could reach the output. Partitions, books and venues are sorted slices, so the order files are created, epochs assign sequence numbers, and paths are reported in is a function of the data alone.

**Every undefined byte is explicitly zeroed.** That is `internal/store`'s discipline, not this converter's, and `format.md`'s "every byte is defined" rule is what makes it checkable.

`TestConvertIsByteReproducible` converts one source twice into two directories and compares the SHA-256 of every file. `TestConvertPinsTheArtifactLayout` reads `header_size` out of the header directly, because on a host whose page size is already 4096 the first test alone would pass even if the pin were removed.

## The artifact content hash, and why it is a sidecar

Each finished artifact gets a `<artifact>.hash` file holding its whole-file SHA-256 as lowercase hex and a newline — the digest alone, no file name, so nothing has to parse around a path that may since have moved. A run manifest carries this value, which is what scopes the project's determinism claim honestly: deterministic *given a fixed, identified artifact*, with that identity independently checkable by anyone who has the file.

It is not the per-record `canonical_v1` projection a `store.CanonicalHasher` computes under the `store.CanonicalSHA256` algorithm. That hash deliberately ignores where bytes sit in a file, so two conversions that pack blobs differently still match. This one is the opposite: it is about one file's exact bytes.

**Not in the header.** The header has nine reserved bytes across three ranges, and `internal/store` validates every one of them as zero; 32 do not fit. Writing the digest into the zero padding past the 128 defined bytes would be worse: the hash would then cover itself, and "the hash of everything except this hash" is a different, weaker thing than "the hash of this file".

`replayd` reads the sidecar if it is there and records an empty string if it is not. It never computes the digest itself — that would mean reading every dataset file in full at startup purely to write a manifest. An empty hash means "no converter stamped this file", which is the honest answer for a hand-written fixture or an `internal/synth` build, and the field is always serialized so that "none" reads differently from "not recorded".

## `exchange_ts` is checked, never repaired

`format.md` states that `exchange_ts` is non-decreasing within a venue, and names the converter as what enforces it. The converter tracks each venue's latest timestamp as it walks the source, across that venue's day files and not just within one of them, and rejects the whole conversion the moment one decreases. Equal timestamps are fine: a venue repeats one, and the ordering key's later fields separate two records that share it.

The rejection is a `*RowError` wrapping `ErrExchangeTsDecreased`, naming the row's ordinal in the source file and its ordering key.

**Reject, never re-sort.** Re-sorting on the full ordering key would produce a valid artifact, and the ordering key's values would even be unchanged — but it would also hide an upstream data-quality problem behind an artifact that looks fine. The specification takes the same line for the closely related case of a genuine key collision: a data problem to fix upstream. A repair mode would have to be an explicit opt-in, added only when real data shows one is needed, never the default.

This check is not the writer's duplicate. `store.Writer` rejects a full ordering key that does not strictly increase, which is a different and weaker condition: a record whose `exchange_ts` went backwards while its `sequence_number` kept rising passes the writer and fails here. That is exactly the case `format.md` warns about, where canonical replay order would apply orderbook deltas out of venue-sequence order and silently corrupt the reconstructed book.

### Level order is normalized, not preserved

A snapshot row's levels are sorted into a bid run and an ask run, bids highest-price first and asks lowest-price first — the order `internal/book` reads them in. Normalizing here makes the blob's bytes a function of the level set alone, and not of the order the source happened to list them in.
