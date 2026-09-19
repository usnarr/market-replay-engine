package main

import (
	"go/ast"
	"go/token"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Suppression is one reviewed exception to a determinism rule. See
// analyzer.go's package doc for how an entry here pairs with an inline
// //lint-determinism:ignore comment.
type Suppression struct {
	File   string // suffix match against the reported file's slash-separated path
	Rule   string
	Reason string
}

// Allowlist is the complete set of suppressions in effect across the
// repository. Empty until a rule needs a deliberate, reviewed exception.
var Allowlist []Suppression

var suppressCommentRE = regexp.MustCompile(`^//\s*lint-determinism:ignore\s+(\S+)\s+reason="([^"]*)"\s*$`)

// report emits a diagnostic for rule at pos unless isSuppressed finds a
// matching inline comment backed by an Allowlist entry.
func report(pass *analysis.Pass, file *ast.File, pos token.Pos, rule, format string, args ...any) {
	if isSuppressed(pass, file, pos, rule) {
		return
	}
	pass.Reportf(pos, format, args...)
}

// isSuppressed reports whether a //lint-determinism:ignore comment for rule
// sits on the line at pos or the line above it, AND a Suppression entry for
// the same file and rule is present in Allowlist. Either alone is not
// enough — see analyzer.go's package doc.
func isSuppressed(pass *analysis.Pass, file *ast.File, pos token.Pos, rule string) bool {
	target := pass.Fset.Position(pos).Line

	var commentRule string
	for _, group := range file.Comments {
		for _, c := range group.List {
			line := pass.Fset.Position(c.Pos()).Line
			if line != target && line != target-1 {
				continue
			}
			m := suppressCommentRE.FindStringSubmatch(c.Text)
			if m != nil {
				commentRule = m[1]
			}
		}
	}
	if commentRule != rule {
		return false
	}

	filename := filepath.ToSlash(pass.Fset.Position(pos).Filename)
	for _, a := range Allowlist {
		if a.Rule == rule && strings.HasSuffix(filename, a.File) {
			return true
		}
	}
	return false
}
