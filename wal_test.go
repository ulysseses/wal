package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"go.uber.org/zap"
)

const (
	testSegmentSize = 100 // bytes
)

func Test_OpenWAL_Coverage(t *testing.T) {
	currInd := 0

	// Create a new WAL.
	baseDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)
	walDir := filepath.Join(baseDir, "wal")
	wal, err := OpenWAL(walDir, testSegmentSize, zap.NewExample())
	if err != nil {
		t.Fatal(err)
	}

	// Create first segment.
	for len(wal.pubSegs) == 0 {
		if _, err := wal.Write(numAndInc(&currInd)); err != nil {
			t.Fatal(err)
		}
	}
	// Write one more record to the 2nd second segment.
	if _, err := wal.Write(numAndInc(&currInd)); err != nil {
		t.Fatal(err)
	}

	// "Finish" for now and close the WAL.
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// Open the same WAL.
	wal2, err := OpenWAL(walDir, testSegmentSize, zap.NewExample())
	if err != nil {
		t.Fatal(err)
	}
	if len(wal2.pubSegs) != 2 {
		t.Fatalf("there weren't 2 published segments as expected; got:\n%#v", wal2.pubSegs)
	}

	// Write until reach 4th segment.
	for len(wal2.pubSegs) == 2 {
		if _, err := wal2.Write(numAndInc(&currInd)); err != nil {
			t.Fatal(err)
		}
	}

	// Write one more record to the 4th segment.
	if _, err := wal2.Write(numAndInc(&currInd)); err != nil {
		t.Fatal(err)
	}

	// Force-publish the 4th segment.
	if err := wal2.cut(); err != nil {
		t.Fatal(err)
	}

	if err := wal2.Close(); err != nil {
		t.Fatal(err)
	}

	// "Accidentally" delete the scratch directory.
	if err := os.RemoveAll(scratchDir(walDir)); err != nil {
		t.Fatal(err)
	}

	// Open the WAL yet again.
	wal3, err := OpenWAL(walDir, testSegmentSize, zap.NewExample())
	if err != nil {
		t.Fatal(err)
	}

	// Write to the scratch and sync.
	if _, err := wal3.Write([]byte(fmt.Sprintf("%d", currInd))); err != nil {
		t.Fatal(err)
	}
	if err := wal3.Sync(); err != nil {
		t.Fatal(err)
	}

	// Every record excluding those presiding in scratch should be visitable.
	i := 0
	test := func(data []byte) error {
		var err error
		s := string(data)
		gotInd, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("on frame %d, got unexpected error: %v", i, err)
		}
		if gotInd != int64(i) {
			return fmt.Errorf("expected %d, but got %d", i, gotInd)
		}
		i++
		return nil
	}
	if err := wal3.Visit(test); err != nil {
		t.Fatal(err)
	}
	if i != currInd {
		t.Fatalf("read %d frames, but wrote %d frames", i, currInd)
	}
}

func Test_WAL_RecoverFromTornWrite(t *testing.T) {
	// Create a new WAL.
	baseDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)
	walDir := filepath.Join(baseDir, "wal")
	wal, err := OpenWAL(walDir, testSegmentSize, zap.NewExample())
	if err != nil {
		t.Fatal(err)
	}

	// Create first segment.
	if _, err := wal.Write([]byte{42}); err != nil {
		t.Fatal(err)
	}
	if err := wal.cut(); err != nil {
		t.Fatal(err)
	}

	// Write two records into the 2nd segment.
	var bytesWritten int64
	n, err := wal.Write([]byte{43})
	if err != nil {
		t.Fatal(err)
	}
	bytesWritten += int64(n)
	n, err = wal.Write([]byte{44})
	if err != nil {
		t.Fatal(err)
	}
	bytesWritten += int64(n)

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// Subtract 1 byte from the 2nd record of the 2nd segment to simulate a torn write.
	_, scratch, err := findSegments(walDir, testSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	fName := segmentFileName(scratchDir(scratch.dir), scratch.seq, scratch.ind)
	f, err := os.OpenFile(fName, os.O_RDWR, privateFileMode)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(bytesWritten - 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Open the WAL again.
	wal2, err := OpenWAL(walDir, testSegmentSize, zap.NewExample())
	if err != nil {
		t.Fatal(err)
	}
	visited := 0
	test := func(data []byte) error {
		fmt.Println(int(data[0]))
		visited++
		return nil
	}
	if err := wal2.Visit(test); err != nil {
		t.Fatal(err)
	}
	if visited != 2 {
		t.Fatalf("expected to visit 2 frames, but actually visited %d frame(s)", visited)
	}
}

func numAndInc(x *int) []byte {
	s := fmt.Sprintf("%d", *x)
	ret := []byte(s)
	*x++
	return ret
}
