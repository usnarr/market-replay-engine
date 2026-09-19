// Command lint-determinism runs the replay project's determinism analyzers
// as a golang.org/x/tools/go/analysis multichecker. See CLAUDE.md's Project
// invariants section for what each rule enforces and why.
package main

import "golang.org/x/tools/go/analysis/multichecker"

func main() {
	multichecker.Main(allAnalyzers...)
}
