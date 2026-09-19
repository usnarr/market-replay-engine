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

_No measurements yet. First entry lands with M6 in `internal/fanout`._

## Encode/decode throughput

_No measurements yet. First entry lands with M2 in `internal/store`._

## Pacing accuracy

_No measurements yet. First entry lands with M8 pacing._

## End-to-end replay rate

_No measurements yet. First entry lands once `cmd/replayd` (M9) can run a full synthetic dataset end to end._
