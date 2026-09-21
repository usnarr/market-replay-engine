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
// internal/fanout, internal/store, or internal/book. internal/book is off
// the hot path (see internal/book/book.go), so it gets this rule but not
// hotPath's any/fmt.Sprint rules.
func orderedPath(pkgPath string) bool {
	return hotPath(pkgPath) || inPathSegment(pkgPath, "internal/store") || inPathSegment(pkgPath, "internal/book")
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
