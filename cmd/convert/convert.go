package main

import (
	"os"

	"github.com/parquet-go/parquet-go"
)

// CheckSourceFile validates path against the canonical source schema and
// returns how many rows it holds. It reads the file's footer only, so a
// schema fault costs nothing close to a whole conversion.
func CheckSourceFile(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return 0, err
	}
	if err := CheckSourceSchema(pf.Schema()); err != nil {
		return 0, err
	}
	return pf.NumRows(), nil
}
