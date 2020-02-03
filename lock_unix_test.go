// +build darwin linux

package wal

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestLockAndUnlock(t *testing.T) {
	f, err := ioutil.TempFile("", "lock")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			t.Fatal(err)
		}
	}()

	// lock
	if f, err = os.OpenFile(f.Name(), os.O_WRONLY, 0600); err != nil {
		t.Fatal(err)
	}
	if err := lockFileNonBlocking(f); err != nil {
		t.Fatal(err)
	}

	// try locking with another file descriptor
	f2, err := os.OpenFile(f.Name(), os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = lockFileNonBlocking(f2); err != errLocked {
		t.Fatal(err)
	}

	// unlock
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	// lock as another file descriptor
	if err := lockFileNonBlocking(f2); err != nil {
		t.Fatal(err)
	}
	// unlock
	if err := f2.Close(); err != nil {
		t.Fatal(err)
	}
}
