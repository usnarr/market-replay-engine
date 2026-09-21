package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// SourceLevel is one price level of a snapshot row. Side is
// store.SideBid or store.SideAsk, widened to uint32 for the reason
// SourceRow gives. See docs/convert.md.
type SourceLevel struct {
	Side  uint32 `parquet:"side"`
	Price int64  `parquet:"price"`
	Size  int64  `parquet:"size"`
}

// SourceRow is one row of the canonical source schema: the archive-tier
// shape cmd/convert reads. Its eight scalar columns mirror
// store.Record's public fields one for one, and levels carries a
// snapshot row's book. See docs/convert.md.
//
// venue_id, record_type and side_flags are uint32 here and narrower in
// store.Record. parquet-go writes no Go type narrower than 32 bits, and
// Parquet has no physical type narrower either, so a 16- or 8-bit column
// would be a schema no producer could actually write. The converter
// narrows them itself and rejects a value that does not fit.
type SourceRow struct {
	ExchangeTs     int64  `parquet:"exchange_ts"`
	SequenceNumber uint64 `parquet:"sequence_number"`
	InstrumentID   uint32 `parquet:"instrument_id"`
	VenueID        uint32 `parquet:"venue_id"`
	RecordType     uint32 `parquet:"record_type"`
	SideFlags      uint32 `parquet:"side_flags"`
	Price          int64  `parquet:"price"`
	Size           int64  `parquet:"size"`

	// Levels is empty for every row but a snapshot. A repeated group
	// rather than a second file: a snapshot's levels and the record they
	// belong to then arrive together, so no join can put them out of
	// order.
	Levels []SourceLevel `parquet:"levels"`
}

// ErrSourceSchema is the base of every canonical-source-schema
// rejection. A source file that does not match is rejected whole:
// guessing at a near-miss column is how a silently wrong artifact gets
// written.
var ErrSourceSchema = errors.New("convert: source file does not match the canonical source schema")

// SchemaError names the one column a source file got wrong. It wraps
// ErrSourceSchema, so a caller can match the class with errors.Is and
// still print which column failed.
type SchemaError struct {
	// Column is the dotted path of the offending column, for example
	// "levels.price". It is empty only when the schema has no columns at
	// all.
	Column string

	// Want and Got describe the column as the canonical schema declares
	// it and as the file declares it. Got is "missing" for a column the
	// file omits, and Want is "no such column" for one it adds.
	Want string
	Got  string
}

func (e *SchemaError) Error() string {
	return fmt.Sprintf("%s: column %q is %s, want %s", ErrSourceSchema.Error(), e.Column, e.Got, e.Want)
}

func (e *SchemaError) Unwrap() error { return ErrSourceSchema }

// canonicalSchema is the one schema cmd/convert reads. It is built from
// SourceRow, not written out by hand, so the struct the converter
// decodes into and the schema it validates against cannot disagree.
var canonicalSchema = parquet.SchemaOf(SourceRow{})

// CheckSourceSchema reports whether got matches the canonical source
// schema, field for field and type for type. It returns a *SchemaError
// naming the first column that differs, taking the columns in the
// canonical schema's own declared order so that two files with the same
// fault are rejected the same way.
func CheckSourceSchema(got parquet.Node) error {
	return checkFields(nil, canonicalSchema.Fields(), got.Fields())
}

// checkFields compares one level of the two schemas. Fields are matched
// by name, and the canonical order drives the walk: a map of the file's
// fields would make which column a mismatched file is blamed for depend
// on Go's map iteration order.
func checkFields(path []string, want, got []parquet.Field) error {
	for _, w := range want {
		g, ok := findField(got, w.Name())
		if !ok {
			return &SchemaError{Column: columnPath(path, w.Name()), Want: describeNode(w), Got: "missing"}
		}
		if err := checkField(append(path, w.Name()), w, g); err != nil {
			return err
		}
	}
	for _, g := range got {
		if _, ok := findField(want, g.Name()); !ok {
			return &SchemaError{Column: columnPath(path, g.Name()), Want: "no such column", Got: describeNode(g)}
		}
	}
	return nil
}

// checkField compares one node against its canonical counterpart, then
// recurses into a group's own fields.
func checkField(path []string, want, got parquet.Node) error {
	if want.Repeated() != got.Repeated() || want.Optional() != got.Optional() || want.Leaf() != got.Leaf() {
		return &SchemaError{Column: columnPath(path, ""), Want: describeNode(want), Got: describeNode(got)}
	}
	if want.Leaf() {
		if describeNode(want) != describeNode(got) {
			return &SchemaError{Column: columnPath(path, ""), Want: describeNode(want), Got: describeNode(got)}
		}
		return nil
	}
	return checkFields(path, want.Fields(), got.Fields())
}

func findField(fields []parquet.Field, name string) (parquet.Field, bool) {
	for _, f := range fields {
		if f.Name() == name {
			return f, true
		}
	}
	return nil, false
}

// columnPath joins a field path into the dotted form a SchemaError
// reports. A leaf name of "" means path already ends at the column.
func columnPath(path []string, name string) string {
	if name != "" {
		path = append(path, name)
	}
	return strings.Join(path, ".")
}

// describeNode renders a node's repetition and type as one comparable
// string. A leaf's Type().String() carries both the physical and the
// logical type, which is what makes an int64 column standing in for a
// uint64 one a mismatch rather than a silent reinterpretation.
func describeNode(n parquet.Node) string {
	var b strings.Builder
	switch {
	case n.Repeated():
		b.WriteString("repeated ")
	case n.Optional():
		b.WriteString("optional ")
	default:
		b.WriteString("required ")
	}
	if n.Leaf() {
		b.WriteString(n.Type().String())
	} else {
		b.WriteString("group")
	}
	return b.String()
}
