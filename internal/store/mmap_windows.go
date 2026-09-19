package store

import (
	"os"
	"syscall"
	"unsafe"
)

// mapFile maps path read-only and returns its bytes together with the
// function that unmaps them. The returned slice is valid until that
// function runs; reading it afterwards raises EXCEPTION_IN_PAGE_ERROR,
// which no program can recover from. See docs/format.md.
//
// unsafe.Slice is unavoidable here: MapViewOfFile hands back a bare
// address. This is not the unsafe-cast the format rules forbid — no Go
// struct is laid over file bytes, and every field is still decoded one
// at a time.
func mapFile(path string) ([]byte, func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	size := info.Size()
	if size == 0 {
		// CreateFileMapping fails on a zero-byte file with
		// ERROR_FILE_INVALID. An empty venue partition is normal, so it
		// must not surface as a platform error: return no bytes and let
		// Open reject the file for having no header.
		_ = f.Close()
		return nil, func() error { return nil }, nil
	}
	if size > int64(maxInt) {
		_ = f.Close()
		return nil, nil, ErrRecordCount
	}

	// A zero max size maps the whole file as it is now. The offset is
	// always zero, so the 64 KiB allocation granularity that
	// MapViewOfFile requires of an offset never comes into play.
	h, err := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, 0, 0, nil)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	addr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		_ = syscall.CloseHandle(h)
		_ = f.Close()
		return nil, nil, err
	}

	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), size)

	// Unmap, then close the mapping handle, then close the file handle,
	// in that order. Windows refuses to delete or reopen a file until
	// every one of those is released, so a test that removes a temp file
	// fails here and nowhere else if the order is wrong.
	closeFn := func() error {
		err := syscall.UnmapViewOfFile(addr)
		if cerr := syscall.CloseHandle(h); cerr != nil && err == nil {
			err = cerr
		}
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
		return err
	}
	return data, closeFn, nil
}
