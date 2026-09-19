//go:build unix

package store

import (
	"os"
	"syscall"
)

// mapFile maps path read-only and returns its bytes together with the
// function that unmaps them. The returned slice is valid until that
// function runs; reading it afterwards faults rather than panics.
//
// This is stdlib syscall, not golang.org/x/sys. syscall covers Mmap and
// Munmap here in a few lines, and x/sys is only worth promoting to a
// direct dependency for something syscall lacks, such as madvise. See
// docs/format.md.
func mapFile(path string) ([]byte, func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	size := info.Size()
	if size == 0 {
		// An empty venue partition is normal, and mmap of zero bytes
		// fails. Open then rejects the file for having no header, which
		// is a format error rather than a platform one.
		return nil, func() error { return nil }, nil
	}
	if size > int64(maxInt) {
		return nil, nil, ErrRecordCount
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	// The mapping outlives f: a mapping holds its own reference to the
	// file, so closing the descriptor here does not invalidate it.
	return data, func() error { return syscall.Munmap(data) }, nil
}
