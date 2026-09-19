package main

import (
	"strconv"

	"golang.org/x/tools/go/analysis"
)

// newBannedImportAnalyzer builds a rule that flags any import of
// exactly importPath, anywhere in the repository. All three uses below
// ban a specific stdlib package outright, so an AST-level check on the
// import spec is enough -- no type information needed.
func newBannedImportAnalyzer(name, rule, importPath, why string) *analysis.Analyzer {
	return &analysis.Analyzer{
		Name: name,
		Doc:  "flag import of " + importPath,
		Run: func(pass *analysis.Pass) (any, error) {
			for _, file := range pass.Files {
				for _, imp := range file.Imports {
					path, err := strconv.Unquote(imp.Path.Value)
					if err != nil || path != importPath {
						continue
					}
					report(pass, file, imp.Pos(), rule, "%s: import of %s is banned; %s", rule, importPath, why)
				}
			}
			return nil, nil
		},
	}
}

// NoContainerHeap flags any import of container/heap. The project uses a
// fixed-size loser tree for the k-way merge instead.
var NoContainerHeap = newBannedImportAnalyzer("nocontainerheap", ruleNoContainerHeap, "container/heap",
	"use the project's loser tree instead")

// NoRandV1 flags any import of math/rand. math/rand/v2 is unaffected; it is
// a distinct import path.
var NoRandV1 = newBannedImportAnalyzer("norandv1", ruleNoRandV1, "math/rand",
	"use math/rand/v2 instead")

// NoHashMaphash flags any import of hash/maphash.
var NoHashMaphash = newBannedImportAnalyzer("nohashmaphash", ruleNoHashMaphash, "hash/maphash",
	"use a fixed algorithm such as hash/crc32 instead")

const (
	ruleNoContainerHeap = "no-container-heap"
	ruleNoRandV1        = "no-rand-v1"
	ruleNoHashMaphash   = "no-hash-maphash"
)
