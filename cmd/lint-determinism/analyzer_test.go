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
		{"flags_container_heap_import", NoContainerHeap, "nocontainerheap"},
		{"flags_math_rand_v1_import", NoRandV1, "norandv1"},
		{"flags_sync_map_use", NoSyncMap, "nosyncmap"},
		{"flags_hash_maphash_import", NoHashMaphash, "nohashmaphash"},
		{"flags_fmt_sprint_family_in_hot_path", NoFmtSprintHotPath, "nofmtsprinthotpath/..."},
		{"flags_empty_interface_in_hot_path", NoAnyHotPath, "noanyhotpath/..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			analysistest.Run(t, testdata, tt.analyzer, tt.pattern)
		})
	}
}
