package main

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"

	"replay/internal/store"
)

// sourceGroup is the canonical source schema written out by hand, so a
// test can change exactly one column and see what CheckSourceSchema
// makes of it. The first subtest below asserts it still matches the
// schema SourceRow itself produces, which is what stops the two drifting
// apart.
func sourceGroup() parquet.Group {
	return parquet.Group{
		"exchange_ts":     parquet.Int(64),
		"sequence_number": parquet.Uint(64),
		"instrument_id":   parquet.Uint(32),
		"venue_id":        parquet.Uint(32),
		"record_type":     parquet.Uint(32),
		"side_flags":      parquet.Uint(32),
		"price":           parquet.Int(64),
		"size":            parquet.Int(64),
		"levels":          parquet.Repeated(levelsGroup()),
	}
}

// levelsGroup is the canonical schema's repeated levels group, without
// the repetition, so a test can change one of its columns.
func levelsGroup() parquet.Group {
	return parquet.Group{
		"side":  parquet.Uint(32),
		"price": parquet.Int(64),
		"size":  parquet.Int(64),
	}
}

func TestCheckSourceSchema(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(parquet.Group)
		wantColumn string
		wantReject bool
	}{
		{
			name: "an_exact_match_of_the_canonical_schema",
		},
		{
			name: "a_renamed_top_level_column",
			mutate: func(g parquet.Group) {
				delete(g, "exchange_ts")
				g["exchange_timestamp"] = parquet.Int(64)
			},
			wantColumn: "exchange_ts",
			wantReject: true,
		},
		{
			name:       "a_missing_top_level_column",
			mutate:     func(g parquet.Group) { delete(g, "price") },
			wantColumn: "price",
			wantReject: true,
		},
		{
			name:       "an_extra_capture_time_column",
			mutate:     func(g parquet.Group) { g["recv_ts"] = parquet.Int(64) },
			wantColumn: "recv_ts",
			wantReject: true,
		},
		{
			name:       "a_signed_column_where_the_canonical_schema_is_unsigned",
			mutate:     func(g parquet.Group) { g["sequence_number"] = parquet.Int(64) },
			wantColumn: "sequence_number",
			wantReject: true,
		},
		{
			name:       "a_floating_point_price",
			mutate:     func(g parquet.Group) { g["price"] = parquet.Leaf(parquet.DoubleType) },
			wantColumn: "price",
			wantReject: true,
		},
		{
			name:       "an_optional_column_where_the_canonical_schema_requires_one",
			mutate:     func(g parquet.Group) { g["size"] = parquet.Optional(parquet.Int(64)) },
			wantColumn: "size",
			wantReject: true,
		},
		{
			name: "a_narrowed_column_inside_the_levels_group",
			mutate: func(g parquet.Group) {
				levels := levelsGroup()
				levels["price"] = parquet.Int(32)
				g["levels"] = parquet.Repeated(levels)
			},
			wantColumn: "levels.price",
			wantReject: true,
		},
		{
			name: "a_missing_column_inside_the_levels_group",
			mutate: func(g parquet.Group) {
				levels := levelsGroup()
				delete(levels, "side")
				g["levels"] = parquet.Repeated(levels)
			},
			wantColumn: "levels.side",
			wantReject: true,
		},
		{
			name: "a_levels_group_that_is_not_repeated",
			mutate: func(g parquet.Group) {
				g["levels"] = levelsGroup()
			},
			wantColumn: "levels",
			wantReject: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := sourceGroup()
			if tt.mutate != nil {
				tt.mutate(g)
			}

			err := CheckSourceSchema(parquet.NewSchema("SourceRow", g))

			if !tt.wantReject {
				if err != nil {
					t.Fatalf("CheckSourceSchema(canonical) = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, ErrSourceSchema) {
				t.Fatalf("CheckSourceSchema(%s) = %v, want an error wrapping ErrSourceSchema", tt.name, err)
			}
			var se *SchemaError
			if !errors.As(err, &se) {
				t.Fatalf("CheckSourceSchema(%s) = %v, want a *SchemaError", tt.name, err)
			}
			if se.Column != tt.wantColumn {
				t.Errorf("CheckSourceSchema(%s) column = %q, want %q", tt.name, se.Column, tt.wantColumn)
			}
		})
	}
}

// writeSourceParquet writes rows to a new Parquet file in dir and
// returns its path.
func writeSourceParquet[T any](t *testing.T, dir, name string, rows []T) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create(%s) error = %v, want nil", path, err)
	}
	w := parquet.NewGenericWriter[T](f)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("Write() error = %v, want nil", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("parquet Close() error = %v, want nil", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("file Close() error = %v, want nil", err)
	}
	return path
}

func TestCheckSourceFile(t *testing.T) {
	t.Run("a_file_in_the_canonical_schema_reports_its_row_count", func(t *testing.T) {
		dir := t.TempDir()
		rows := []SourceRow{
			{ExchangeTs: 1000, SequenceNumber: 1, InstrumentID: 10, VenueID: 7, RecordType: 0, Price: 100, Size: 3},
			{ExchangeTs: 1001, SequenceNumber: 2, InstrumentID: 10, VenueID: 7, RecordType: 2, Levels: []SourceLevel{
				{Side: 0, Price: 99, Size: 5},
				{Side: 1, Price: 101, Size: 4},
			}},
		}
		path := writeSourceParquet(t, dir, "source.parquet", rows)

		got, err := CheckSourceFile(path)

		if err != nil {
			t.Fatalf("CheckSourceFile(%s) error = %v, want nil", path, err)
		}
		if got != int64(len(rows)) {
			t.Errorf("CheckSourceFile(%s) = %d, want %d", path, got, len(rows))
		}
	})

	t.Run("a_file_in_another_schema_is_rejected_by_column", func(t *testing.T) {
		type otherRow struct {
			ExchangeTs int64 `parquet:"exchange_ts"`
			RecvTs     int64 `parquet:"recv_ts"`
		}
		dir := t.TempDir()
		path := writeSourceParquet(t, dir, "other.parquet", []otherRow{{ExchangeTs: 1000, RecvTs: 1001}})

		_, err := CheckSourceFile(path)

		if !errors.Is(err, ErrSourceSchema) {
			t.Fatalf("CheckSourceFile(%s) = %v, want an error wrapping ErrSourceSchema", path, err)
		}
		var se *SchemaError
		if !errors.As(err, &se) || se.Column == "" {
			t.Errorf("CheckSourceFile(%s) = %v, want a *SchemaError naming a column", path, err)
		}
	})

	t.Run("a_file_that_does_not_exist", func(t *testing.T) {
		_, err := CheckSourceFile(filepath.Join(t.TempDir(), "absent.parquet"))

		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("CheckSourceFile(absent) = %v, want an error wrapping os.ErrNotExist", err)
		}
	})
}

// TestSourceRowMirrorsStoreRecord pins the claim docs/convert.md makes:
// every store.Record field that describes an event has a column of the
// same name in the canonical source schema, and every column maps back.
// A field added to store.Record without a decision about the source
// schema fails here rather than silently converting to zero.
func TestSourceRowMirrorsStoreRecord(t *testing.T) {
	columnOf := []struct {
		field  string
		column string
	}{
		{"ExchangeTs", "exchange_ts"},
		{"SequenceNumber", "sequence_number"},
		{"InstrumentID", "instrument_id"},
		{"VenueID", "venue_id"},
		{"RecordType", "record_type"},
		{"SideFlags", "side_flags"},
		{"Price", "price"},
		{"Size", "size"},
	}

	// The blob fields say where a snapshot's levels sit in one particular
	// file. That is the writer's business: the source carries the levels
	// themselves, in the repeated levels group, and cmd/convert assigns
	// the offsets. See docs/convert.md.
	fileLayoutOnly := []string{"BlobOffset", "BlobLen", "LevelCount"}

	var fields []string
	rt := reflect.TypeOf(store.Record{})
	for i := 0; i < rt.NumField(); i++ {
		fields = append(fields, rt.Field(i).Name)
	}
	var columns []string
	for _, f := range canonicalSchema.Fields() {
		columns = append(columns, f.Name())
	}

	for _, m := range columnOf {
		if !slices.Contains(fields, m.field) {
			t.Errorf("store.Record has no field %q, but the canonical schema maps column %q to it", m.field, m.column)
		}
		if !slices.Contains(columns, m.column) {
			t.Errorf("the canonical schema has no column %q for store.Record field %q", m.column, m.field)
		}
	}
	for _, field := range fields {
		mapped := slices.ContainsFunc(columnOf, func(m struct{ field, column string }) bool { return m.field == field })
		if !mapped && !slices.Contains(fileLayoutOnly, field) {
			t.Errorf("store.Record field %q is neither a source column nor file-layout-only", field)
		}
	}
	for _, column := range columns {
		mapped := slices.ContainsFunc(columnOf, func(m struct{ field, column string }) bool { return m.column == column })
		if !mapped && column != "levels" {
			t.Errorf("canonical schema column %q maps to no store.Record field", column)
		}
	}
}
