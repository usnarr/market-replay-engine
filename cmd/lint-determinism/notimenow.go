package main

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const ruleNoTimeNow = "no-time-now"

// bannedTimeFuncs are the time package functions banned everywhere except
// internal/clock/realclock.go. See CLAUDE.md: "time.Now() appears exactly
// once in the codebase, inside clock.RealClock."
var bannedTimeFuncs = map[string]bool{
	"Now":       true,
	"Since":     true,
	"After":     true,
	"Tick":      true,
	"Sleep":     true,
	"NewTimer":  true,
	"NewTicker": true,
}

// NoTimeNow flags calls to time.Now and the rest of the wall-clock family
// outside internal/clock/realclock.go, the one place the specification
// allows them.
var NoTimeNow = &analysis.Analyzer{
	Name:     "notimenow",
	Doc:      "flag time.Now and related wall-clock calls outside internal/clock/realclock.go",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runNoTimeNow,
}

func runNoTimeNow(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	fileOf := fileIndex(pass)

	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !bannedTimeFuncs[sel.Sel.Name] {
			return
		}
		pkgIdent, ok := sel.X.(*ast.Ident)
		if !ok {
			return
		}
		pkgName, ok := pass.TypesInfo.Uses[pkgIdent].(*types.PkgName)
		if !ok || pkgName.Imported().Path() != "time" {
			return
		}

		file := fileOf[pass.Fset.Position(call.Pos()).Filename]
		if file != nil && isRealClockFile(pass, file) {
			return
		}
		report(pass, file, call.Pos(), ruleNoTimeNow,
			"%s: direct use of time.%s is banned outside internal/clock/realclock.go", ruleNoTimeNow, sel.Sel.Name)
	})

	return nil, nil
}

// isRealClockFile reports whether file is internal/clock/realclock.go, the
// one file the no-time-now rule exempts.
func isRealClockFile(pass *analysis.Pass, file *ast.File) bool {
	name := filepath.ToSlash(pass.Fset.Position(file.Pos()).Filename)
	return strings.HasSuffix(name, "internal/clock/realclock.go")
}

// fileIndex maps each file's absolute path to its *ast.File, so a rule can
// look up the enclosing file for a node found via the shared inspector.
func fileIndex(pass *analysis.Pass) map[string]*ast.File {
	m := make(map[string]*ast.File, len(pass.Files))
	for _, f := range pass.Files {
		m[pass.Fset.Position(f.Pos()).Filename] = f
	}
	return m
}
