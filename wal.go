// Package wal implements a WAL. WAL is short for Write-Ahead-Log. Its purpose is durability.
// Before a write is acknowledged, it is copied to the WAL. If the machine crashes after a write
// to the WAL but before it is acknowledged, we can recover and apply the write by reading it from
// the WAL. If instead the machine crashes before the write to WAL, we can safely retry the write
// without fear of the write being applied twice (exactly once semantics).
//
// A write may be pending in user-space buffers or the page cache. It must be synced to disk for
// it to be made durable. There are multiple ways to do this, though they make a trade-off between
// the level of durability and performance.
// 1. Sync after every write. This is the slowest option but offers the strongest durability.
// 2. Sync after N writes, where N is the batch size. This is faster since it calls sync less
//    frequently, but you can lose at most N writes. However, if < N writes come in, there must
//    be a mechanism to force-sync the batch at some point even though it hasn't reached N yet.
// 3. Sync at a regular T interval, where T is a time interval.  This is faster since it calls
//    sync less frequently, but you can lose un-synced writes. Another downside is that you
//    must find an appropriate interval, dynamically/adaptively or via manual tuning.
// 4. Don't force sync. This is the fastest but most dangerous: the OS will asynchronously write
//    to disk (i.e. choose when to flush the dirty page back to disk).
//
// WAL is agnostic and lets the user decide which strategy is appropriate: call Write() and
// Sync() as one pleases.
//
// Often we want to fetch all written records from an index onwards. To optimize this search
// pattern, WAL writes records to "segment" files that live in a "WAL" directory. Each segment has
// a maximum size, so when one is filled up, WAL will automatically close that file and begin a new
// one. Segment files are formatted as "{seq}-{index}.wal", where seq is the `seq`-th segment file,
// and the first record in that segment is the `index`-th overall record. Thus, we don't have to
// read every segment in the directory to find the appropriate records.
package wal

import (
	"bufio"
	"io"
	"os"

	"go.uber.org/zap"
)

// WAL is a write-ahead-log.
type WAL struct {
	pubSegs   []segment
	scratchRW *segmentReadWriter

	// TODO(ulysseses): replace with a limited pool of readers/writers (for potential parallel access)
	brPub     *bufio.Reader
	brScratch *bufio.Reader
	bwScratch *bufio.Writer

	// dir: move segments from the scratch dir to dir
	dir string
	// sizeHint is an indication of how large the segment file can get (in bytes)
	sizeHint int

	lastInd uint64

	logger *zap.Logger
}

func (wal *WAL) reusePubReader(f *os.File) *bufio.Reader {
	if wal.brPub == nil {
		wal.brPub = bufio.NewReaderSize(f, wal.sizeHint)
	} else {
		wal.brPub.Reset(f)
	}
	return wal.brPub
}

func (wal *WAL) reuseScratchReader(f *os.File) *bufio.Reader {
	if wal.brScratch == nil {
		wal.brScratch = bufio.NewReaderSize(f, wal.sizeHint)
	} else {
		wal.brScratch.Reset(f)
	}
	return wal.brScratch
}

func (wal *WAL) reuseScratchWriter(f *os.File) *bufio.Writer {
	if wal.bwScratch == nil {
		wal.bwScratch = bufio.NewWriterSize(f, wal.sizeHint)
	} else {
		wal.bwScratch.Reset(f)
	}
	return wal.bwScratch
}

// Write to the current segment file, cutting off and starting a new one if necessary.
// To persist on disk, make sure to call Sync at some point.
func (wal *WAL) Write(data []byte) (n int, err error) {
	n, err = wal.scratchRW.frame(data)
	if err == errSegmentSizeReached {
		err = wal.cut()
	}
	if err == nil {
		wal.lastInd++ // keep lastInd up to date
	}
	return n, err
}

// Sync persists accumulated writes from both the user-land buffer and kernel page cache to disk.
func (wal *WAL) Sync() error {
	return wal.scratchRW.sync()
}

// Close closes the WAL. This does NOT sync, so remember to call WAL.Sync()
func (wal *WAL) Close() error {
	return wal.scratchRW.Close()
}

// cut will sync and close the segment file, then create a new one for the next write.
func (wal *WAL) cut() error {
	// publish scratch
	seg, err := wal.scratchRW.publish()
	if err != nil {
		return err
	}
	wal.pubSegs = append(wal.pubSegs, seg)

	// start a new segment
	wal.scratchRW, err = segment{
		seq:      seg.seq + 1,
		ind:      wal.lastInd + 1,
		dir:      wal.dir,
		sizeHint: wal.sizeHint,
	}.createScratch(wal.reuseScratchReader, wal.reuseScratchWriter)
	if err != nil {
		return err
	}

	return nil
}

// Visit visits every frame (published or scratch), deframes it, and applies f to it.
func (wal *WAL) Visit(f func(data []byte) error) error {
	// visit published segments
	for _, seg := range wal.pubSegs {
		segR, err := seg.openPublished(wal.reusePubReader)
		if err != nil {
			return err
		}
		for {
			data, _, err := segR.deframe()
			if err == io.EOF {
				segR.Close()
				break
			}
			if err != nil {
				segR.Close()
				return err
			}
			if err := f(data); err != nil {
				segR.Close()
				return err
			}
		}
	}

	return nil
}

// OpenWAL opens the directory and finds all existing segment files.
func OpenWAL(dir string, sizeHint int, logger *zap.Logger) (*WAL, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, privateDirMode); err != nil {
			return nil, err
		}
		logger.Info("created WAL directory", zap.String("dir", dir))
	}
	if _, err := os.Stat(scratchDir(dir)); os.IsNotExist(err) {
		if err := os.Mkdir(scratchDir(dir), privateDirMode); err != nil {
			return nil, err
		}
		logger.Info("created WAL scratch directory", zap.String("dir", scratchDir(dir)))
	}

	pubSegs, scratch, err := findSegments(dir, sizeHint)
	if err != nil {
		return nil, err
	}

	wal := WAL{
		dir:      dir,
		sizeHint: sizeHint,
		pubSegs:  pubSegs,
		logger:   logger,
	}

	if scratch == nonExistingSegment {
		// Create a new scratch segment.
		if len(pubSegs) > 0 {
			lastSegR, err := pubSegs[len(pubSegs)-1].openPublished(wal.reusePubReader)
			if err != nil {
				return nil, err
			}
			defer lastSegR.Close()
			if err := updateLastInd(&wal, lastSegR); err != nil {
				return nil, err
			}

			wal.scratchRW, err = segment{
				seq:      lastSegR.segment.seq + 1,
				ind:      wal.lastInd + 1,
				dir:      wal.dir,
				sizeHint: wal.sizeHint,
			}.createScratch(wal.reuseScratchReader, wal.reuseScratchWriter)
			return &wal, err
		}
		wal.scratchRW, err = segment{
			dir:      wal.dir,
			sizeHint: wal.sizeHint,
		}.createScratch(wal.reuseScratchReader, wal.reuseScratchWriter)
		return &wal, err
	}

	// Publish the existing scratch segment, truncating partial frames, if any.
	oldScratchRW, err := scratch.openScratch(wal.reuseScratchReader, wal.reuseScratchWriter)
	if err != nil {
		return nil, err
	}

	err = updateLastInd(&wal, &oldScratchRW.segmentReader)
	if _, ok := err.(errorChecksum); ok {
		// errorChecksum can indicate either of two things:
		// 1. the scratch segment file was preallocated but unfinished
		// 2. there was data corruption
		// In both cases, we should just truncate there and continue on.
		wal.logger.Warn("checksum error", zap.Error(err))
	} else if err != nil {
		return nil, err
	}

	pubSeg, err := oldScratchRW.publish()
	if err != nil {
		return nil, err
	}
	wal.pubSegs = append(wal.pubSegs, pubSeg)

	// Then create a new scratch segment.
	newScratchRW, err := segment{
		seq:      pubSeg.seq + 1,
		ind:      wal.lastInd + 1,
		dir:      wal.dir,
		sizeHint: wal.sizeHint,
	}.createScratch(wal.reuseScratchReader, wal.reuseScratchWriter)
	if err != nil {
		return nil, err
	}
	wal.scratchRW = newScratchRW

	return &wal, nil
}

func updateLastInd(wal *WAL, sr *segmentReader) error {
	lastInd, _, err := sr.seekToLastFrame()
	wal.lastInd = lastInd // cache to wal.lastInd
	return err
}
