package main

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// hexDigestLine is what a pure digest file holds: 64 lowercase hex
// characters and a trailing newline, and nothing else. That is exactly
// the digest half of what sha256sum prints, without the file name, so a
// reader never has to parse around a path that may have moved.
var hexDigestLine = regexp.MustCompile(`^[0-9a-f]{64}\n$`)

func TestConvertWritesAContentHashSidecar(t *testing.T) {
	base := int64(day2024) * nanosPerDay
	src := writeSourceParquet(t, t.TempDir(), "source.parquet", reproducibleRows(base))
	out := t.TempDir()

	paths, err := Convert(src, out, Options{PriceScale: testPriceScale, EpochEvery: 4})

	if err != nil {
		t.Fatalf("Convert() error = %v, want nil", err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			b, err := os.ReadFile(path + HashSuffix)
			if err != nil {
				t.Fatalf("ReadFile(%s) error = %v, want nil", filepath.Base(path)+HashSuffix, err)
			}

			if !hexDigestLine.Match(b) {
				t.Fatalf("the sidecar holds %q, want 64 lowercase hex characters and a newline", b)
			}
			// Recomputed from the artifact's own bytes, not from anything
			// the converter kept: the point of the sidecar is that a third
			// party can check it.
			if got, want := string(b), fileDigest(t, path)+"\n"; got != want {
				t.Errorf("the sidecar holds %q, want %q", got, want)
			}
		})
	}
}

func TestWriteArtifactHash(t *testing.T) {
	t.Run("a_file_that_does_not_exist", func(t *testing.T) {
		_, err := WriteArtifactHash(filepath.Join(t.TempDir(), "absent.bin"))

		if !os.IsNotExist(err) {
			t.Errorf("WriteArtifactHash(absent) error = %v, want a not-exist error", err)
		}
	})

	t.Run("an_empty_file_hashes_to_the_empty_digest", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "empty.bin")
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("WriteFile() error = %v, want nil", err)
		}

		got, err := WriteArtifactHash(path)

		if err != nil {
			t.Fatalf("WriteArtifactHash() error = %v, want nil", err)
		}
		want := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		if got != want {
			t.Errorf("WriteArtifactHash(empty) = %s, want %s", got, want)
		}
	})
}
