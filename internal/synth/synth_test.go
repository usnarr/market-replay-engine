package synth

import (
	"bytes"
	"os"
	"testing"
)

func TestBuild(t *testing.T) {
	t.Run("the_generated_dataset_is_a_pure_function_of_the_seed_and_shape", func(t *testing.T) {
		shape := [][]int{{50, 30}, {0, 40}, {}}

		first := Build(t, t.TempDir(), shape)
		second := Build(t, t.TempDir(), shape)

		if len(first.Files) != len(second.Files) {
			t.Fatalf("first has %d venues, second has %d", len(first.Files), len(second.Files))
		}
		for v := range first.Files {
			if len(first.Files[v]) != len(second.Files[v]) {
				t.Fatalf("venue %d: first has %d files, second has %d", v, len(first.Files[v]), len(second.Files[v]))
			}
			for d := range first.Files[v] {
				a, err := os.ReadFile(first.Files[v][d])
				if err != nil {
					t.Fatalf("ReadFile(%s) error = %v, want nil", first.Files[v][d], err)
				}
				b, err := os.ReadFile(second.Files[v][d])
				if err != nil {
					t.Fatalf("ReadFile(%s) error = %v, want nil", second.Files[v][d], err)
				}
				if !bytes.Equal(a, b) {
					t.Errorf("venue %d day %d: first and second builds produced different bytes", v, d)
				}
			}
		}
	})

	t.Run("record_count_matches_the_shape", func(t *testing.T) {
		shape := [][]int{{50, 30}, {0, 40}, {}}
		ds := Build(t, t.TempDir(), shape)

		if got, want := ds.RecordCount(), 120; got != want {
			t.Errorf("RecordCount() = %d, want %d", got, want)
		}
	})

	t.Run("the_standard_dataset_is_large_enough_to_exercise_multiple_blocks", func(t *testing.T) {
		// internal/merge's TestDeterminism is what actually reads this
		// dataset back through a Cursor and counts real snapshot
		// pointers (at least 20, as of this package's extraction); this
		// package only owns generation, so it checks the one property
		// it can compute without duplicating that read-back logic.
		ds := Standard(t, t.TempDir())

		if got := ds.RecordCount(); got < 4*1024 {
			t.Errorf("RecordCount() = %d, want at least 4096 (several 1024-record blocks)", got)
		}
	})
}
