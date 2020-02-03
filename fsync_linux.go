// +build linux

package wal

import "os"

// fsync is a wrapper around os.File's Sync().
func fsync(f *os.File) error {
	return f.Sync()
}
