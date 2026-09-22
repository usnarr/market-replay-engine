package main

import "strings"

// hotPath reports whether pkgPath is the merge or fan-out package — the two
// packages the specification calls "the hot path" for concrete-types and
// allocation rules.
func hotPath(pkgPath string) bool {
	return inPathSegment(pkgPath, "internal/merge") || inPathSegment(pkgPath, "internal/fanout")
}

// orderedPath reports whether pkgPath is one of the packages where an
// unordered map iteration could reach output: internal/merge,
// internal/fanout, internal/store, internal/book, cmd/convert, or bench.
// internal/book, cmd/convert and bench are all off the hot path (see
// internal/book/book.go, docs/convert.md and bench/harness.go), so they
// get this rule but not hotPath's any/fmt.Sprint rules. cmd/convert is
// held to it because it must produce byte-identical artifacts, which
// makes its iteration order as output-affecting as the replay path's.
// bench is held to it because the load harness chooses the order venues
// enter the merge tree and the order subscribers register, and both are
// output-affecting for the stream it measures.
func orderedPath(pkgPath string) bool {
	return hotPath(pkgPath) ||
		inPathSegment(pkgPath, "internal/store") ||
		inPathSegment(pkgPath, "internal/book") ||
		inPathSegment(pkgPath, "cmd/convert") ||
		inPathSegment(pkgPath, "bench")
}

// inPathSegment reports whether seg appears as a whole "/"-separated
// segment run of pkgPath, at the start, in the middle, or as the whole
// path. Used instead of strings.Contains alone so "internal/merger" does
// not match the segment "internal/merge".
func inPathSegment(pkgPath, seg string) bool {
	return pkgPath == seg ||
		strings.HasPrefix(pkgPath, seg+"/") ||
		strings.HasSuffix(pkgPath, "/"+seg) ||
		strings.Contains(pkgPath, "/"+seg+"/")
}
