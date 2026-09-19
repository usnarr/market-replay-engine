package main

import (
	"go/ast"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const ruleNoMultiSelect = "no-multi-select"

// NoMultiSelect flags a select statement with more than one non-default
// communication clause, in internal/merge or internal/fanout. The runtime
// picks pseudo-randomly among several ready clauses, which is exactly the
// kind of nondeterminism a path that decides event ordering cannot have.
var NoMultiSelect = &analysis.Analyzer{
	Name:     "nomultiselect",
	Doc:      "flag select statements with more than one comm clause in the merge and fan-out packages",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runNoMultiSelect,
}

func runNoMultiSelect(pass *analysis.Pass) (any, error) {
	if !hotPath(pass.Pkg.Path()) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	fileOf := fileIndex(pass)

	insp.Preorder([]ast.Node{(*ast.SelectStmt)(nil)}, func(n ast.Node) {
		sel := n.(*ast.SelectStmt)

		comms := 0
		for _, clause := range sel.Body.List {
			if clause.(*ast.CommClause).Comm != nil {
				comms++
			}
		}
		if comms <= 1 {
			return
		}

		file := fileOf[pass.Fset.Position(sel.Pos()).Filename]
		report(pass, file, sel.Pos(), ruleNoMultiSelect,
			"%s: select has %d ready communication clauses; the runtime picks among them pseudo-randomly", ruleNoMultiSelect, comms)
	})

	return nil, nil
}
