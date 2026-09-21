# The archive-tier converter

Scope: `cmd/convert`'s canonical source Parquet schema, its venue partitioning policy, and what it rejects rather than repairs.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md). The output format is [`format.md`](./format.md).

## Its own module, its own dependency

`cmd/convert` is a separate Go module. A Parquet reader is not on the server module's approved dependency list, and the server has no reason to link one: conversion is offline work that runs once per artifact, not per replay. Keeping it in its own module makes the claim checkable rather than aspirational — `GOWORK=off go list -m all` from the repository root never names a Parquet library.

The dependency is `github.com/parquet-go/parquet-go`, pinned at v0.25.1. Pure Go, so conversion cross-compiles and needs no C toolchain. v0.25.1 is the last release that builds under this repository's `go1.23.4` toolchain pin; v0.26.0 and later require go1.24.9. The pin moves when the toolchain pin moves, not before.

## The canonical source schema

**Provisional.** This schema is a starting point, not a commitment. It was designed against `store.Record`, not against real archive data, because no sample of the latter exists yet. Revisit it once real source data does — the plan file that specifies this milestone flags exactly this, and a schema that no real producer writes is worth changing.

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

## Rejection is by column, never by guess

A source file whose schema is not exactly this one is rejected whole, with a `*SchemaError` naming the first offending column in the canonical schema's own declared order: a missing column, an added column, a column of the wrong type, and a column whose repetition differs are all the same class of fault. Matching in canonical order, rather than over the file's own field map, is what makes two files with the same fault produce the same message.

Nothing is inferred from a near miss. A `float64` `price` column is not "close enough" to an `int64` one, and a `recv_ts` column the canonical schema does not define is not silently dropped — `format.md` excludes capture-time fields from the canonical hash for a reason, and a converter that quietly accepted one would be deciding that question on its own.

A row whose values cannot become a `store.Record` is rejected the same way, with a `*RowError` carrying the row's ordinal in the source file and its ordering key: a `venue_id` that does not fit `uint16`, an undefined `record_type`, a reserved `side_flags` bit, levels on a row that is not a snapshot, a price or size on a row that is, and a level whose side is neither bid nor ask.

A rejected conversion leaves nothing behind. Every file it had opened is aborted and removed, because a half-converted directory is worse than an empty one: nothing downstream can tell it from a complete conversion.

## `exchange_ts` is checked, never repaired

`format.md` states that `exchange_ts` is non-decreasing within a venue, and names the converter as what enforces it. The converter tracks each venue's latest timestamp as it walks the source, across that venue's day files and not just within one of them, and rejects the whole conversion the moment one decreases. Equal timestamps are fine: a venue repeats one, and the ordering key's later fields separate two records that share it.

The rejection is a `*RowError` wrapping `ErrExchangeTsDecreased`, naming the row's ordinal in the source file and its ordering key.

**Reject, never re-sort.** Re-sorting on the full ordering key would produce a valid artifact, and the ordering key's values would even be unchanged — but it would also hide an upstream data-quality problem behind an artifact that looks fine. The specification takes the same line for the closely related case of a genuine key collision: a data problem to fix upstream. A repair mode would have to be an explicit opt-in, added only when real data shows one is needed, never the default.

This check is not the writer's duplicate. `store.Writer` rejects a full ordering key that does not strictly increase, which is a different and weaker condition: a record whose `exchange_ts` went backwards while its `sequence_number` kept rising passes the writer and fails here. That is exactly the case `format.md` warns about, where canonical replay order would apply orderbook deltas out of venue-sequence order and silently corrupt the reconstructed book.

### Level order is normalized, not preserved

A snapshot row's levels are sorted into a bid run and an ask run, bids highest-price first and asks lowest-price first — the order `internal/book` reads them in. Normalizing here makes the blob's bytes a function of the level set alone, and not of the order the source happened to list them in.
