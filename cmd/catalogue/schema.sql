-- The run catalogue and the benchmark history. Two tables, no more:
-- every column here answers a question this project already asks, and a
-- schema grows only when a concrete query needs it.
--
-- Nothing in a replay opens this file. cmd/replayd writes a plain JSON
-- manifest and stops there; cmd/catalogue reads those manifests later,
-- offline, and is the only thing that ever links a SQLite driver. See
-- docs/no-database.md.

-- One finished replay run, as its manifest describes it.
CREATE TABLE IF NOT EXISTS runs (
    -- The manifest file's name without its extension. A manifest carries
    -- no ID of its own, and the operator already chose this name.
    run_id            TEXT    PRIMARY KEY,

    -- SHA-256 over every dataset file's path and content hash, in
    -- manifest order: one value for the dataset as a whole. The manifest
    -- file remains the record of which files those were.
    dataset_hash      TEXT    NOT NULL,

    -- The manifest's config object, verbatim. Stored as its own JSON
    -- rather than spread over columns: a column per setting would have
    -- to change every time a run option does.
    config            TEXT    NOT NULL,

    -- The run's canonical stream digest. Empty until replayd can produce
    -- one for free; see cmd/replayd/manifest.go.
    canonical_hash    TEXT    NOT NULL,

    started_unix_nano INTEGER NOT NULL,
    elapsed_nanos     INTEGER NOT NULL
) STRICT;

-- One benchmark measurement, from one `make bench` run.
CREATE TABLE IF NOT EXISTS benchmarks (
    name               TEXT    NOT NULL,
    git_commit         TEXT    NOT NULL,
    iterations         INTEGER NOT NULL,
    ns_per_op          REAL    NOT NULL,
    bytes_per_op       INTEGER NOT NULL,

    -- The zero-allocation gate's number. Kept as its own column, not
    -- inside a blob of text, because "did this commit allocate on the
    -- hot path" is the one benchmark question this project asks most.
    allocs_per_op      INTEGER NOT NULL,

    recorded_unix_nano INTEGER NOT NULL,

    -- One measurement per benchmark per commit per ingestion. Two
    -- `make bench` runs at the same commit are two rows, because
    -- comparing them is the point.
    PRIMARY KEY (name, git_commit, recorded_unix_nano)
) STRICT;
