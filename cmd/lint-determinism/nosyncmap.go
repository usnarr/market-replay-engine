package main

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const ruleNoSyncMap = "no-sync-map"

// NoSyncMap flags any use of sync.Map, anywhere in the repository. Unlike
// the banned-import rules, importing "sync" itself is fine (sync.Mutex,
// sync.WaitGroup, ...); only the Map type is banned, so this checks the
// selector's resolved type rather than the import.
var NoSyncMap = &analysis.Analyzer{
	Name:     "nosyncmap",
	Doc:      "flag use of sync.Map",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runNoSyncMap,
}

func runNoSyncMap(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	fileOf := fileIndex(pass)

	insp.Preorder([]ast.Node{(*ast.SelectorExpr)(nil)}, func(n ast.Node) {
		sel := n.(*ast.SelectorExpr)
		if sel.Sel.Name != "Map" {
			return
		}
		pkgIdent, ok := sel.X.(*ast.Ident)
		if !ok {
			return
		}
		pkgName, ok := pass.TypesInfo.Uses[pkgIdent].(*types.PkgName)
		if !ok || pkgName.Imported().Path() != "sync" {
			return
		}

		file := fileOf[pass.Fset.Position(sel.Pos()).Filename]
		report(pass, file, sel.Pos(), ruleNoSyncMap,
			"%s: sync.Map is banned; use a plain map guarded by a Mutex, or the project's own concurrent structure", ruleNoSyncMap)
	})

	return nil, nil
}
