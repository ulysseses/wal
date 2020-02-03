package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

const (
	// privateFileMode grants owner read/write to a file.
	privateFileMode = 0600

	// privateDirMode grants owner read/write to files in the directory, but also
	// executable permission (so we can cd into it)
	privateDirMode = 0700

	// SegmentSizeBytes is the preallocated size of each segment file.
	// The actual size might actually be larger than this. In general, the
	// default value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	SegmentSizeBytes = 64 * 1000 * 1000

	// SegExt is the segment file extension.
	SegExt = ".seg"

	// ScratchSuffix is the suffix of the scratch directory for the write-ahead-log.
	ScratchSuffix = ".tmp"
)

var (
	errSegmentSizeReached = fmt.Errorf("segment size reached")
	nonExistingSegment    = segment{}
)

type segment struct {
	// seq and ind of the beginning of the segment
	seq, ind uint64

	// dir is the directory of the segment file
	dir string

	// sizeHint is an indication of how large the segment file can get (in bytes)
	sizeHint int
}

func (s segment) openPublished(reuseReader func(*os.File) *bufio.Reader) (*segmentReader, error) {
	f, err := os.OpenFile(segmentFileName(s.dir, s.seq, s.ind), os.O_RDONLY, privateFileMode)
	if err != nil {
		return nil, err
	}
	if err := lockFileNonBlocking(f); err != nil {
		f.Close()
		return nil, err
	}
	br := reuseReader(f)
	sr := segmentReader{
		segment:  s,
		deframer: newDeframer(br),
		f:        f,
		br:       br,
	}
	return &sr, nil
}

func (s segment) _newScratch(
	flag int,
	create bool,
	reuseReader func(*os.File) *bufio.Reader,
	reuseWriter func(*os.File) *bufio.Writer) (*segmentReadWriter, error) {
	dirF, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(segmentFileName(scratchDir(s.dir), s.seq, s.ind), flag, privateFileMode)
	if err != nil {
		dirF.Close()
		return nil, err
	}
	if err := lockFileNonBlocking(f); err != nil {
		dirF.Close()
		f.Close()
		return nil, err
	}
	if create {
		if err := preallocate(f, int64(s.sizeHint)); err != nil {
			dirF.Close()
			f.Close()
			return nil, err
		}
	}
	br := reuseReader(f)
	bw := reuseWriter(f)
	srw := segmentReadWriter{
		segmentReader: segmentReader{
			segment:  s,
			deframer: newDeframer(br),
			f:        f,
			br:       br,
		},
		framer: newFramer(bw),
		bw:     bw,
		dirF:   dirF,
	}
	return &srw, nil
}

func (s segment) openScratch(
	reuseReader func(*os.File) *bufio.Reader,
	reuseWriter func(*os.File) *bufio.Writer,
) (*segmentReadWriter, error) {
	return s._newScratch(os.O_RDWR, false, reuseReader, reuseWriter)
}

func (s segment) createScratch(
	reuseReader func(*os.File) *bufio.Reader,
	reuseWriter func(*os.File) *bufio.Writer,
) (*segmentReadWriter, error) {
	return s._newScratch(os.O_WRONLY|os.O_CREATE, true, reuseReader, reuseWriter)
}

type segmentReader struct {
	segment
	*deframer

	f  *os.File
	br *bufio.Reader
}

func (sr *segmentReader) undo(n int) error {
	unreadBytes := sr.br.Buffered()
	rewind := int64(n + unreadBytes)
	if _, seekErr := sr.f.Seek(-rewind, io.SeekCurrent); seekErr != nil {
		return seekErr
	}
	sr.deframer.undo(int(n))
	sr.br.Reset(sr.f)
	return nil
}

func (sr *segmentReader) seekToLastFrame() (uint64, int64, error) {
	init := true
	var ind uint64
	for {
		var n int
		_, n, err := sr.deframer.deframe()
		if err == io.EOF {
			break
		} else if _, ok := err.(errorChecksum); ok {
			// undo last read
			if err := sr.undo(n); err != nil {
				return 0, 0, err
			}
			break
		} else if _, ok := err.(errorPartialFrame); ok {
			// undo partial frame
			if err := sr.undo(n); err != nil {
				return 0, 0, err
			}
			break
		} else if err != nil {
			return 0, 0, err
		}

		if init {
			init = false
			ind = sr.segment.ind
		} else {
			ind++
		}
	}
	offset, err := sr.f.Seek(0, io.SeekCurrent)
	return ind, offset, err
}

func (sr *segmentReader) Close() error {
	return sr.f.Close()
}

type segmentReadWriter struct {
	segmentReader
	*framer

	bw   *bufio.Writer
	dirF *os.File
}

func (srw *segmentReadWriter) frame(data []byte) (int, error) {
	n, err := srw.framer.frame(data)
	reachedEnd := err == io.EOF ||
		srw.segmentReader.deframer.nBytes+srw.framer.nBytes >= srw.segmentReader.segment.sizeHint
	if reachedEnd {
		return n, errSegmentSizeReached
	}
	return n, err
}

func (srw *segmentReadWriter) sync() error {
	if err := srw.bw.Flush(); err != nil {
		return err
	}
	if err := fsync(srw.f); err != nil {
		return err
	}
	return nil
}

func (srw *segmentReadWriter) publish() (segment, error) {
	// flush just in case we haven't yet
	if err := srw.bw.Flush(); err != nil {
		return segment{}, err
	}

	// truncate to avoid wasting space
	currentOffset, err := srw.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return segment{}, err
	}
	if err := srw.f.Truncate(currentOffset); err != nil {
		return segment{}, err
	}

	// fsync
	if err := fsync(srw.f); err != nil {
		return segment{}, err
	}

	// move from scratch to published directory
	seg := srw.segmentReader.segment
	newName := segmentFileName(seg.dir, seg.seq, seg.ind)
	if err := os.Rename(srw.f.Name(), newName); err != nil {
		return segment{}, err
	}

	// fsync the directory
	if err := fsync(srw.dirF); err != nil {
		return segment{}, err
	}

	// close (and unlock) file
	if err := srw.f.Close(); err != nil {
		return segment{}, err
	}
	if err := srw.dirF.Close(); err != nil {
		return segment{}, err
	}

	// TODO(ulysseses): maybe just let GC deal with this automatically?
	// zero out self to help with GC pressure
	srw.segmentReader.deframer = nil
	srw.segmentReader.f = nil
	srw.segmentReader.br = nil
	srw.framer = nil
	srw.bw = nil
	srw.dirF = nil

	return seg, nil
}

func (srw *segmentReadWriter) Close() error {
	// Flush any remaining in-memory data.
	if err := srw.bw.Flush(); err != nil {
		return err
	}
	return srw.segmentReader.Close()
}

func segmentFileName(dir string, seq, ind uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%016x-%016x%s", seq, ind, SegExt))
}

func getSeqInd(fName string) (seq, ind uint64, err error) {
	re := regexp.MustCompile(`([0-9a-f]{16})-([0-9a-f]{16})` + SegExt)
	matches := re.FindStringSubmatch(fName)
	if matches == nil {
		err = fmt.Errorf("segment file name must be in %%016x-%%016x%s format", SegExt)
		return
	}
	seq, _ = strconv.ParseUint(matches[1], 16, 64)
	ind, _ = strconv.ParseUint(matches[2], 16, 64)
	return
}

func scratchDir(dir string) string {
	return filepath.Clean(dir) + ScratchSuffix
}

func findSegments(dir string, sizeHint int) (pubSegs []segment, scratch segment, err error) {
	scratch = nonExistingSegment

	publishedPaths, scratchPaths, err := getSegmentPaths(dir)
	if err != nil {
		return pubSegs, scratch, err
	}
	if len(scratchPaths) > 1 {
		err := fmt.Errorf("data corruption: there must be at most 1 outstanding scratch")
		return pubSegs, scratch, err
	}

	pubSegs = []segment{}

	// find published segments
	var maxSeq uint64
	init := false
	for _, path := range publishedPaths {
		seq, ind, err := getSeqInd(path)
		if err != nil {
			return pubSegs, scratch, err
		}
		if !init {
			init = true
		} else if seq != maxSeq+1 {
			err := fmt.Errorf("sequences must be contiguous: missing seq %d", maxSeq+1)
			return pubSegs, scratch, err
		}
		maxSeq = seq
		seg := segment{
			seq:      seq,
			ind:      ind,
			dir:      dir,
			sizeHint: sizeHint,
		}
		pubSegs = append(pubSegs, seg)
	}

	if len(scratchPaths) == 1 {
		scratchFile := scratchPaths[0]
		seq, ind, err := getSeqInd(scratchFile)
		if err != nil {
			// subtly ignore error (invalid scratch)
			return pubSegs, nonExistingSegment, nil
		}
		if init && seq != maxSeq+1 {
			return pubSegs, nonExistingSegment, fmt.Errorf(
				"data corruption: outstanding scratch seq must be 1+ largest: got %d", seq)
		}
		maxSeq = seq
		scratch = segment{
			seq:      seq,
			ind:      ind,
			dir:      dir,
			sizeHint: sizeHint,
		}
	}
	return pubSegs, scratch, nil
}

func getSegmentPaths(dir string) (published, scratches []string, err error) {
	walkFunc := func(paths *[]string) filepath.WalkFunc {
		init := false
		return func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if !init {
					init = true
					return nil
				}
				return filepath.SkipDir
			}
			if filepath.Ext(path) == SegExt {
				*paths = append(*paths, path)
			}
			return nil
		}
	}

	if _, err2 := os.Stat(dir); !os.IsNotExist(err2) {
		if err = filepath.Walk(dir, walkFunc(&published)); err != nil {
			return
		}
	}
	if _, err2 := os.Stat(scratchDir(dir)); !os.IsNotExist(err2) {
		if err = filepath.Walk(scratchDir(dir), walkFunc(&scratches)); err != nil {
			return
		}
	}
	sort.Strings(published)
	sort.Strings(scratches)
	return
}
