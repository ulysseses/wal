// +build linux

package wal

import (
	"os"
	"syscall"
)

func preallocExtend(f *os.File, sizeInBytes uint64) error {
	// use mode = 0 to change size
	err := syscall.Fallocate(int(f.Fd()), 0, 0, sizeInBytes)
	if err != nil {
		errno, ok := err.(syscall.Errno)
		// ENOTSUP -> fallback to preallocExtendTrunc
		// EINTR   -> fallback to preallocExtendTrunc
		if ok && (errno == syscall.ENOTSUP || errno == syscall.EINTR) {
			return preallocExtendTrunc(f, sizeInBytes)
		}
	}
	return err
}
