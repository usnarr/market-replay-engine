package main

import (
	"database/sql"
	"path/filepath"
	"slices"
	"testing"
)

// openTestDB opens a catalogue in its own temp directory and closes it
// when the test ends.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "catalogue.db"))
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// tableNames returns the catalogue's tables, sorted.
func tableNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan() error = %v, want nil", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v, want nil", err)
	}
	return out
}

func TestOpen(t *testing.T) {
	t.Run("a_new_catalogue_holds_exactly_the_two_tables", func(t *testing.T) {
		db := openTestDB(t)

		got := tableNames(t, db)

		want := []string{"benchmarks", "runs"}
		if !slices.Equal(got, want) {
			t.Errorf("tables = %v, want %v", got, want)
		}
	})

	t.Run("opening_an_existing_catalogue_keeps_its_rows", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "catalogue.db")
		first, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v, want nil", err)
		}
		_, err = first.Exec(`INSERT INTO runs (run_id, dataset_hash, config, canonical_hash, started_unix_nano, elapsed_nanos)
			VALUES ('run-1', 'abc', '{}', 'def', 1, 2)`)
		if err != nil {
			t.Fatalf("Exec() error = %v, want nil", err)
		}
		if err := first.Close(); err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}

		second, err := Open(path)

		if err != nil {
			t.Fatalf("reopening: Open() error = %v, want nil", err)
		}
		defer second.Close()
		var n int
		if err := second.QueryRow(`SELECT count(*) FROM runs`).Scan(&n); err != nil {
			t.Fatalf("QueryRow() error = %v, want nil", err)
		}
		if n != 1 {
			t.Errorf("runs holds %d rows after reopening, want 1", n)
		}
	})

	t.Run("a_run_id_is_unique", func(t *testing.T) {
		db := openTestDB(t)
		const insert = `INSERT INTO runs (run_id, dataset_hash, config, canonical_hash, started_unix_nano, elapsed_nanos)
			VALUES ('run-1', 'abc', '{}', 'def', 1, 2)`
		if _, err := db.Exec(insert); err != nil {
			t.Fatalf("Exec() error = %v, want nil", err)
		}

		_, err := db.Exec(insert)

		if err == nil {
			t.Error("inserting the same run_id twice succeeded, want a constraint failure")
		}
	})
}
