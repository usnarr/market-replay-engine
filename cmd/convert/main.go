// Command convert turns an archive-tier Parquet file into the hot-tier
// binary format internal/store reads, one file per venue and UTC day. It
// is an offline tool in its own Go module, so the server module never
// carries a Parquet reader. See docs/convert.md.
package main

import (
	"flag"
	"fmt"
	"os"
)

// defaultPriceScale is the divisor for price and size. The source
// carries scaled integers and no scale of its own, so the operator
// states it. Eight decimal places covers every venue this project has
// been pointed at.
const defaultPriceScale = 100_000_000

func main() {
	in := flag.String("in", "", "source Parquet file, in the canonical source schema")
	out := flag.String("out", "", "directory to write the hot-tier files into")
	priceScale := flag.Int64("price-scale", defaultPriceScale, "power-of-ten divisor for every price and size")
	epochEvery := flag.Int("epoch-every", DefaultEpochEvery, "snapshot epoch cadence, in records per venue")
	flag.Parse()

	opts := Options{PriceScale: *priceScale, EpochEvery: *epochEvery}
	if err := run(*in, *out, opts); err != nil {
		fmt.Fprintln(os.Stderr, "convert:", err)
		os.Exit(1)
	}
}

func run(in, out string, opts Options) error {
	if in == "" || out == "" {
		return fmt.Errorf("-in and -out are both required")
	}
	paths, err := Convert(in, out, opts)
	if err != nil {
		return err
	}
	for _, path := range paths {
		fmt.Println(path)
	}
	return nil
}
