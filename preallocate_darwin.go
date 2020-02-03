// +build darwin

package wal

import (
	"os"
	"syscall"
	"unsafe"
)

func preallocExtend(f *os.File, sizeInBytes int64) error {
	if err := preallocFixed(f, sizeInBytes); err != nil {
		return err
	}
	return preallocExtendTrunc(f, sizeInBytes)
}

func preallocFixed(f *os.File, sizeInBytes int64) error {
	// Allocate requested space
	fstore := &syscall.Fstore_t{
		Flags:   syscall.F_ALLOCATEALL,
		Posmode: syscall.F_PEOFPOSMODE,
		Length:  sizeInBytes,
	}
	p := uintptr(unsafe.Pointer(fstore))
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_PREALLOCATE), p)
	if errno == 0 || errno == syscall.ENOTSUP {
		return nil
	}

	// handle wrong argument to fallocate syscall
	if errno == syscall.EINVAL {
		// filesystem "st_blocks" are allocated in the units of
		// "Allocation Block Size" (run "diskutil info /" command)
		var stat syscall.Stat_t
		syscall.Fstat(int(f.Fd()), &stat)

		// syscall.Statfs_t.Bsize is the block size.
		var statfs syscall.Statfs_t
		syscall.Fstatfs(int(f.Fd()), &statfs)
		blockSize := int64(statfs.Bsize)

		if stat.Blocks*blockSize >= sizeInBytes {
			// enough blocks are already allocated
			return nil
		}
	}
	return errno
}
