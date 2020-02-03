// +build darwin

package wal

import (
	"os"
	"syscall"
)

// Fsync on OSX flushes the data onto the physical drive, but the drive may not write to the
// persistent media for quite some time. The write may even be performed out-of-order.
// Using F_FULLFSYNC ensures that the physical drive's buffer will also get flushed to the
// media.
func fsync(f *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_FULLFSYNC), 0)
	if errno == 0 {
		return nil
	}
	return errno
}
