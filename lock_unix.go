// +build darwin linux

package wal

import (
	"fmt"
	"os"
	"syscall"
)

var (
	errLocked = fmt.Errorf("file already locked")
)

// lockFileNonBlocking locks the file via the Flock system call. It is performed
// in non-blocking mode, so if it is locked, it immediately returns with syscall.EWOULDBLOCK.
func lockFileNonBlocking(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == syscall.EWOULDBLOCK {
		err = errLocked
	}
	return err
}
