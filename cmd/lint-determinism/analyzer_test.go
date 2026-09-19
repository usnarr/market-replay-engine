package main

import (
	"testing"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

// TestAnalyzers runs each determinism rule against its own testdata
// package. Every rule has a bad.go (each violation carries a `// want`
// comment) and a good.go (nothing there should be flagged); some also test
// a scope boundary or a named exception.
func TestAnalyzers(t *testing.T) {
	testdata := analysistest.TestData()

	tests := []struct {
		name     string
		analyzer *analysis.Analyzer
		pattern  string
	}{
		{"flags_time_now_family_outside_realclock", NoTimeNow, "notimenow/..."},
		{"flags_more_than_one_comm_clause_in_hot_path", NoMultiSelect, "nomultiselect/..."},
		{"flags_map_range_in_ordered_packages", NoMapRangeOrdered, "nomaprangeordered/..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			analysistest.Run(t, testdata, tt.analyzer, tt.pattern)
		})
	}
}
