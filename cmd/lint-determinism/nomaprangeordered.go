package main

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const ruleNoMapRangeOrdered = "no-map-range-ordered"

// NoMapRangeOrdered flags any `range` over a map-typed expression in
// internal/merge, internal/fanout, or internal/store. This is deliberately
// conservative: it does not try to prove whether a particular loop's
// output can reach the emitted stream, because that reachability question
// is exactly the kind of thing a future change could get wrong invisibly.
// If a set is needed on an ordered path, CLAUDE.md's answer is a sorted
// slice, not a map -- so a map range in these packages is banned outright,
// not just where this analyzer can prove it matters. A map used only for
// point lookup (m[k]) is not a range statement and is unaffected.
var NoMapRangeOrdered = &analysis.Analyzer{
	Name:     "nomaprangeordered",
	Doc:      "flag range over a map in internal/merge, internal/fanout, and internal/store",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runNoMapRangeOrdered,
}

func runNoMapRangeOrdered(pass *analysis.Pass) (any, error) {
	if !orderedPath(pass.Pkg.Path()) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	fileOf := fileIndex(pass)

	insp.Preorder([]ast.Node{(*ast.RangeStmt)(nil)}, func(n ast.Node) {
		rng := n.(*ast.RangeStmt)

		t := pass.TypesInfo.TypeOf(rng.X)
		if t == nil {
			return
		}
		if _, ok := t.Underlying().(*types.Map); !ok {
			return
		}

		file := fileOf[pass.Fset.Position(rng.Pos()).Filename]
		report(pass, file, rng.Pos(), ruleNoMapRangeOrdered,
			"%s: range over a map has nondeterministic iteration order; sort the keys or use a slice", ruleNoMapRangeOrdered)
	})

	return nil, nil
}
