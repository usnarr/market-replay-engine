package main

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const ruleNoFmtSprintHotPath = "no-fmt-sprint-hot-path"

// bannedFmtFuncs are the fmt functions banned in the hot path. fmt.Println
// and friends are unaffected -- only the three the specification names.
var bannedFmtFuncs = map[string]bool{
	"Sprint":  true,
	"Sprintf": true,
	"Errorf":  true,
}

// NoFmtSprintHotPath flags fmt.Sprint, fmt.Sprintf, and fmt.Errorf in
// internal/merge or internal/fanout. Each allocates and reflects over its
// arguments; both are unwanted on the hot path.
var NoFmtSprintHotPath = &analysis.Analyzer{
	Name:     "nofmtsprinthotpath",
	Doc:      "flag fmt.Sprint, fmt.Sprintf, and fmt.Errorf in the merge and fan-out packages",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runNoFmtSprintHotPath,
}

func runNoFmtSprintHotPath(pass *analysis.Pass) (any, error) {
	if !hotPath(pass.Pkg.Path()) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	fileOf := fileIndex(pass)

	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !bannedFmtFuncs[sel.Sel.Name] {
			return
		}
		pkgIdent, ok := sel.X.(*ast.Ident)
		if !ok {
			return
		}
		pkgName, ok := pass.TypesInfo.Uses[pkgIdent].(*types.PkgName)
		if !ok || pkgName.Imported().Path() != "fmt" {
			return
		}

		file := fileOf[pass.Fset.Position(call.Pos()).Filename]
		report(pass, file, call.Pos(), ruleNoFmtSprintHotPath,
			"%s: fmt.%s allocates and is banned on the merge/fan-out hot path", ruleNoFmtSprintHotPath, sel.Sel.Name)
	})

	return nil, nil
}
