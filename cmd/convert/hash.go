package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

// HashSuffix is appended to an artifact's path to name its content-hash
// sidecar.
const HashSuffix = ".hash"

// WriteArtifactHash computes the SHA-256 of the finished file at path
// and writes it to path+HashSuffix as lowercase hex with a trailing
// newline, returning the digest. The sidecar holds the digest and
// nothing else — no file name — so a reader never parses around a path
// that may since have moved.
//
// The digest lives beside the file rather than inside its header. The
// header's nine reserved bytes cannot hold 32, and a digest written into
// the file would have to cover itself. See docs/convert.md.
func WriteArtifactHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if err := os.WriteFile(path+HashSuffix, []byte(digest+"\n"), 0o644); err != nil {
		return "", err
	}
	return digest, nil
}
