// Package cmd is outside the rule's scope (internal/merge, internal/fanout
// only). fmt.Errorf here is not flagged -- cmd/ is where errors are
// decided what to do with, per CLAUDE.md.
package cmd

import "fmt"

func wrap(err error) error {
	return fmt.Errorf("cmd: %w", err)
}
