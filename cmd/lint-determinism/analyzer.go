// Package main implements the determinism rules. Each rule is a separate
// *analysis.Analyzer, registered in allAnalyzers below and run together by
// the multichecker in main.go.
//
// # Suppressing a rule
//
// A rule fires because a construct is banned, not because it is wrong in
// every possible case. When a violation is a deliberate, reviewed
// exception, suppress it with a two-part gate rather than a bare nolint
// comment:
//
//  1. Add a comment on the offending line, or the line directly above it:
//
//     //lint-determinism:ignore no-time-now reason="pacer resets a single timer; see docs/clock.md"
//
//  2. Add a matching entry to the Allowlist var in suppress.go: the file
//     (matched by path suffix) and the exact rule name.
//
// A suppressing comment with no Allowlist entry does nothing — the
// diagnostic still fires. This is deliberate: a bare inline comment is
// invisible in a code review's diff summary unless the reviewer opens
// every changed file, but a change to suppress.go always shows up as a
// reviewed line in the PR. Keep the Allowlist short; each entry is a place
// a determinism guarantee is not being checked by tooling.
package main

import "golang.org/x/tools/go/analysis"

// allAnalyzers is the full set of determinism rules, wired into the
// multichecker in main.go and into the analysistest suite in
// analyzer_test.go.
var allAnalyzers = []*analysis.Analyzer{
	NoTimeNow,
	NoMultiSelect,
	NoMapRangeOrdered,
	NoContainerHeap,
	NoRandV1,
	NoSyncMap,
	NoHashMaphash,
}
