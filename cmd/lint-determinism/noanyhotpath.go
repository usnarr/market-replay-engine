package main

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const ruleNoAnyHotPath = "no-any-hot-path"

// NoAnyHotPath flags a function parameter or return type of any /
// interface{}, and a type assertion or type switch on a value of that
// type, in internal/merge or internal/fanout. A named interface with
// methods is unaffected -- only the empty interface is banned, since only
// the empty interface forces boxing and a runtime type check.
var NoAnyHotPath = &analysis.Analyzer{
	Name:     "noanyhotpath",
	Doc:      "flag any/interface{} in function signatures, type assertions, and type switches in the merge and fan-out packages",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runNoAnyHotPath,
}

func runNoAnyHotPath(pass *analysis.Pass) (any, error) {
	if !hotPath(pass.Pkg.Path()) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	fileOf := fileIndex(pass)

	flagIfEmptyInterface := func(expr ast.Expr) {
		t := pass.TypesInfo.TypeOf(expr)
		if t == nil {
			return
		}
		// Underlying, not a direct assertion: `any` type-checks as
		// *types.Alias (Go's alias type), and a named interface like
		// `type Reader interface{ Read() int }` type-checks as
		// *types.Named. Underlying() unwraps both down to the
		// *types.Interface actually being used, so a named interface
		// with real methods still correctly falls through unflagged.
		iface, ok := t.Underlying().(*types.Interface)
		if !ok || iface.NumMethods() != 0 {
			return
		}
		file := fileOf[pass.Fset.Position(expr.Pos()).Filename]
		report(pass, file, expr.Pos(), ruleNoAnyHotPath,
			"%s: the empty interface forces boxing and a runtime type check; use a concrete type", ruleNoAnyHotPath)
	}

	checkFuncType := func(ft *ast.FuncType) {
		for _, f := range ft.Params.List {
			flagIfEmptyInterface(f.Type)
		}
		if ft.Results != nil {
			for _, f := range ft.Results.List {
				flagIfEmptyInterface(f.Type)
			}
		}
	}

	insp.Preorder([]ast.Node{
		(*ast.FuncDecl)(nil),
		(*ast.FuncLit)(nil),
		(*ast.TypeAssertExpr)(nil),
		(*ast.TypeSwitchStmt)(nil),
	}, func(n ast.Node) {
		switch v := n.(type) {
		case *ast.FuncDecl:
			checkFuncType(v.Type)

		case *ast.FuncLit:
			checkFuncType(v.Type)

		case *ast.TypeAssertExpr:
			if v.Type == nil {
				// The `v.(type)` form inside a type switch guard; the
				// enclosing TypeSwitchStmt case below checks it once.
				return
			}
			flagIfEmptyInterface(v.X)

		case *ast.TypeSwitchStmt:
			var assert *ast.TypeAssertExpr
			switch a := v.Assign.(type) {
			case *ast.ExprStmt:
				assert, _ = a.X.(*ast.TypeAssertExpr)
			case *ast.AssignStmt:
				if len(a.Rhs) == 1 {
					assert, _ = a.Rhs[0].(*ast.TypeAssertExpr)
				}
			}
			if assert != nil {
				flagIfEmptyInterface(assert.X)
			}
		}
	})

	return nil, nil
}
