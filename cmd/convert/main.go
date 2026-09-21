// Command convert turns an archive-tier Parquet file into the hot-tier
// binary format internal/store reads. It is an offline tool in its own
// Go module, so the server module never carries a Parquet reader. See
// docs/convert.md.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	in := flag.String("in", "", "source Parquet file, in the canonical source schema")
	flag.Parse()

	if err := run(*in); err != nil {
		fmt.Fprintln(os.Stderr, "convert:", err)
		os.Exit(1)
	}
}

// run checks one source file against the canonical source schema and
// reports how many rows it holds.
func run(in string) error {
	if in == "" {
		return fmt.Errorf("-in is required")
	}
	rows, err := CheckSourceFile(in)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %d rows match the canonical source schema\n", in, rows)
	return nil
}
