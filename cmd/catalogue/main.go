// Command catalogue maintains the run catalogue and benchmark history
// in SQLite. It runs offline, after a replay has already finished, and
// is the only program in this repository that links a SQLite driver. See
// docs/no-database.md.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	db := flag.String("db", "", "SQLite catalogue file, created if it does not exist")
	flag.Parse()

	if err := run(*db); err != nil {
		fmt.Fprintln(os.Stderr, "catalogue:", err)
		os.Exit(1)
	}
}

func run(dbPath string) error {
	if dbPath == "" {
		return fmt.Errorf("-db is required")
	}
	db, err := Open(dbPath)
	if err != nil {
		return err
	}
	return db.Close()
}
