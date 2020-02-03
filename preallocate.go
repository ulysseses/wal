package wal

import (
	"io"
	"os"
)

// preallocate tries to allocate the space for a given file.
// If the operation is unsupported, no error will be returned.
// Otherwise, the error encountered will be returned.
func preallocate(f *os.File, sizeInBytes int64) error {
	if sizeInBytes == 0 {
		return nil
	}
	return preallocExtend(f, sizeInBytes)
}

func preallocExtendTrunc(f *os.File, sizeInBytes int64) error {
	curOff, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	size, err := f.Seek(sizeInBytes, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err := f.Seek(curOff, io.SeekStart); err != nil {
		return err
	}

	if size < sizeInBytes {
		return nil
	}
	return f.Truncate(sizeInBytes)
}
