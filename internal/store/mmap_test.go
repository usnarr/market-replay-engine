package store

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMmapLifecycle(t *testing.T) {
	t.Run("a_file_is_no_longer_mapped_once_its_reader_is_closed", func(t *testing.T) {
		// Windows refuses to truncate a file that still has a mapped
		// view, with ERROR_USER_MAPPED_FILE, so a Close that fails to
		// unmap is observable here and nowhere else. Deleting is not the
		// check to use: Windows 11 removes the name of a mapped file
		// happily and frees it when the last view goes.
		f := writeFixture(t, 2)
		r, err := Open(f.path)
		if err != nil {
			t.Fatalf("Open() error = %v, want nil", err)
		}
		_ = r.RecordAt(0)

		if err := r.Close(); err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}
		err = os.Truncate(f.path, 0)

		if err != nil {
			t.Errorf("os.Truncate() error = %v, want nil — the file is still mapped", err)
		}
	})

	t.Run("a_file_can_be_removed_once_its_reader_is_closed", func(t *testing.T) {
		f := writeFixture(t, 2)
		r, err := Open(f.path)
		if err != nil {
			t.Fatalf("Open() error = %v, want nil", err)
		}
		_ = r.RecordAt(0)

		if err := r.Close(); err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}
		err = os.Remove(f.path)

		if err != nil {
			t.Errorf("os.Remove() error = %v, want nil — something still holds the file", err)
		}
	})

	t.Run("a_zero_length_file_is_a_format_error_not_a_platform_error", func(t *testing.T) {
		// CreateFileMapping fails on a zero-byte file on Windows. An
		// empty venue partition is normal input, so the backend
		// special-cases it and the format layer reports what is actually
		// wrong: there is no header.
		path := filepath.Join(t.TempDir(), "empty.rpl")
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("creating the empty file: %v", err)
		}

		_, err := Open(path)

		if !errors.Is(err, ErrShortHeader) {
			t.Errorf("Open() error = %v, want %v", err, ErrShortHeader)
		}
	})

	t.Run("a_finalized_file_with_no_records_opens", func(t *testing.T) {
		// Distinct from the case above: this file is a header and
		// nothing else, so it maps and reads as a valid empty partition.
		w, path := newTestWriter(t, 4)
		if err := w.Close(); err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}

		r, err := Open(path)

		if err != nil {
			t.Fatalf("Open() error = %v, want nil", err)
		}
		t.Cleanup(func() { _ = r.Close() })
		if r.Len() != 0 {
			t.Errorf("Len() = %d, want 0", r.Len())
		}
	})

	t.Run("two_readers_can_map_the_same_file", func(t *testing.T) {
		f := writeFixture(t, 2)
		a := openFixture(t, f.path)
		b := openFixture(t, f.path)

		if diff := cmp.Diff(a.RecordAt(0), b.RecordAt(0)); diff != "" {
			t.Errorf("record 0 differs between two mappings (-a +b):\n%s", diff)
		}
	})
}

func TestMmapContentMatchesTheFile(t *testing.T) {
	f := writeFixture(t, 2)
	want, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatalf("reading the fixture: %v", err)
	}
	r := openFixture(t, f.path)

	t.Run("every_record_matches_the_bytes_on_disk", func(t *testing.T) {
		h, err := decodeHeader(want)
		if err != nil {
			t.Fatalf("decodeHeader() error = %v, want nil", err)
		}

		for i := 0; i < r.Len(); i++ {
			if diff := cmp.Diff(recordAtIndex(t, want, h, uint64(i)), r.RecordAt(i)); diff != "" {
				t.Errorf("record %d differs from the file (-file +mapping):\n%s", i, diff)
			}
		}
	})

	t.Run("a_blob_matches_the_bytes_on_disk", func(t *testing.T) {
		h, _ := decodeHeader(want)
		rec := r.RecordAt(2)

		got, err := r.Blob(rec)

		if err != nil {
			t.Fatalf("Blob() error = %v, want nil", err)
		}
		onDisk := want[h.BlobRegionOffset+rec.BlobOffset:][4:rec.BlobLen]
		if diff := cmp.Diff(onDisk, got); diff != "" {
			t.Errorf("blob differs from the file (-file +mapping):\n%s", diff)
		}
	})
}

// TestMmapConcurrentReads gives go test -race shared state to find a
// data race in. A Reader is immutable once Open returns, so it makes no
// ordering assertions of its own beyond every goroutine agreeing on what
// it read.
func TestMmapConcurrentReads(t *testing.T) {
	const goroutines = 16
	f := writeFixture(t, 2)
	r := openFixture(t, f.path)
	want := make([]Record, r.Len())
	for i := range want {
		want[i] = r.RecordAt(i)
	}

	var wg sync.WaitGroup
	got := make([][]Record, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			seen := make([]Record, r.Len())
			for i := range seen {
				seen[i] = r.RecordAt(i)
				_, _ = r.SeekTime(seen[i].ExchangeTs)
				_, _ = r.SnapshotBefore(i)
			}
			for b := 0; b < r.BlockCount(); b++ {
				_ = r.VerifyBlock(b)
			}
			got[g] = seen
		}(g)
	}
	wg.Wait()

	for g := range got {
		if diff := cmp.Diff(want, got[g]); diff != "" {
			t.Errorf("goroutine %d read different records (-want +got):\n%s", g, diff)
		}
	}
}
