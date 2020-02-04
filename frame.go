package wal

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

type errorPartialFrame struct {
	n   int
	msg string
}

// Error implements error for errorPartialFrame.
func (err errorPartialFrame) Error() string {
	return fmt.Sprintf("read frame partially (%d bytes): %s", err.n, err.msg)
}

type errorChecksum struct {
	actual, got uint32
	n           int
}

// Error implements error for errorChecksum.
func (err errorChecksum) Error() string {
	return fmt.Sprintf("actual checksum is %d, but got checksum of %d", err.actual, err.got)
}

type framer struct {
	w           io.Writer
	crc         hash.Hash32
	lenFieldBuf [8]byte
	checksumBuf [4]byte
	padBuf      [8]byte
	nBytes      int
}

// frame writes a frame. The frame is encoded as follows:
// 1. 8 bytes:
//   * most significant byte:
//     - msb: 1 means there is padding, 0 means there is no padding
//     - the rest: number of padding bytes (padLen)
//   * least significant 4 bytes: length of the actual data in bytes (actualLen)
// 2. 4 bytes: uint32 checksum
// 3. `actualLen` bytes: the actual data
// 4. `padLen` bytes: padding of up to 8 bytes.
func (f *framer) frame(data []byte) (int, error) {
	lenField, padLen := encodeFrameSize(uint32(len(data)))
	binary.LittleEndian.PutUint64(f.lenFieldBuf[:], lenField)

	f.crc.Write(data)
	checksum := f.crc.Sum32() // rolling
	binary.LittleEndian.PutUint32(f.checksumBuf[:], checksum)

	nn := 0

	n, err := f.w.Write(f.lenFieldBuf[:])
	nn += n
	f.nBytes += n
	if n != 8 {
		return nn, fmt.Errorf("torn write of lenField")
	}
	if err != nil {
		return nn, err
	}
	n, err = f.w.Write(f.checksumBuf[:])
	nn += n
	f.nBytes += n
	if n != 4 {
		return nn, fmt.Errorf("torn write of checksum")
	}
	if err != nil {
		return nn, err
	}
	n, err = f.w.Write(data)
	nn += n
	f.nBytes += n
	if n != len(data) {
		return nn, fmt.Errorf("torn write of data")
	}
	if err != nil {
		return nn, err
	}
	if padLen != 0 {
		n, err = f.w.Write(f.padBuf[:padLen])
		nn += n
		f.nBytes += n
		if n != int(padLen) {
			return nn, fmt.Errorf("torn write of padding")
		}
		if err != nil {
			return nn, err
		}
	}
	return nn, nil
}

func newFramer(w io.Writer) *framer {
	f := framer{
		w:   w,
		crc: crc32.New(crcTable),
	}
	return &f
}

func encodeFrameSize(nBytes uint32) (lenField uint64, padLen uint8) {
	lenField = uint64(nBytes)
	// force 8 byte alignment so length never gets a torn write
	padLen = uint8(8 - (nBytes % 8))
	if padLen != 0 {
		lenField |= uint64(0x80|padLen) << 56
	}
	return
}

func frameSize(dataLen int) int {
	padLen := 8 - (dataLen % 8)
	return 8 + 4 + dataLen + padLen
}

type deframer struct {
	r           io.Reader
	crc         hash.Hash32
	lenFieldBuf [8]byte
	checksumBuf [4]byte
	padBuf      [8]byte
	nBytes      int
}

// deframe parses a frame and returns the un-framed data. If there any issues with
// the checksum, reading, or seeking, an error is emitted.
// The frame is encoded as follows:
// 1. 8 bytes:
//   * most significant 4 bytes:
//     - msb: 1 means there is padding, 0 means there is no padding
//     - the rest: number of padding bytes (padLen)
//   * least significant 4 bytes: length of the actual data in bytes (actualLen)
// 2. 4 bytes: uint32 checksum
// 3. `actualLen` bytes: the actual data
// 4. `padLen` bytes: padding of up to 8 bytes.
func (d *deframer) deframe() ([]byte, int, error) {
	nn := 0
	n, err := d.r.Read(d.lenFieldBuf[:])
	nn += n
	d.nBytes += n
	if err != nil {
		return nil, nn, err
	} else if n != 8 {
		return nil, nn, errorPartialFrame{n: nn, msg: "lenField is torn"}
	}

	nBytes, padLen := decodeFrameSize(d.lenFieldBuf)

	n, err = d.r.Read(d.checksumBuf[:])
	nn += n
	d.nBytes += n
	if err != nil {
		if err == io.EOF {
			return nil, nn, errorPartialFrame{n: nn, msg: "checksum is torn"}
		}
		return nil, nn, err
	} else if n != 4 {
		return nil, nn, errorPartialFrame{n: nn, msg: "checksum is torn"}
	}
	checksum := binary.LittleEndian.Uint32(d.checksumBuf[:])

	data := make([]byte, nBytes)
	n, err = d.r.Read(data)
	nn += n
	d.nBytes += n
	if err != nil {
		if err == io.EOF {
			return nil, nn, errorPartialFrame{n: nn, msg: "data is torn"}
		}
		return nil, nn, err
	} else if n != int(nBytes) {
		return nil, nn, errorPartialFrame{n: nn, msg: "data is torn"}
	}

	d.crc.Write(data) // rolling
	actualChecksum := d.crc.Sum32()
	if actualChecksum != checksum {
		return data, nn, errorChecksum{
			actual: actualChecksum,
			got:    checksum,
			n:      nn,
		}
	}

	if padLen > 0 {
		// not all io.Reader's implement io.Seeker, so we don't rely on Seek() for reading past padding
		n, err = d.r.Read(d.padBuf[:padLen])
		nn += n
		d.nBytes += n
		if err != nil {
			if err == io.EOF {
				return nil, nn, errorPartialFrame{n: nn, msg: "padding is torn"}
			}
			return nil, nn, err
		} else if n != int(padLen) {
			return nil, nn, errorPartialFrame{n: nn, msg: "padding is torn"}
		}
	}

	return data, nn, nil
}

func (d *deframer) undo(n int) {
	d.nBytes -= n
}

func newDeframer(r io.Reader) *deframer {
	d := deframer{
		r:   r,
		crc: crc32.New(crcTable),
	}
	return &d
}

func decodeFrameSize(lenFieldBuf [8]byte) (nBytes uint32, padLen uint8) {
	// assuming little-endian
	lenField := binary.LittleEndian.Uint64(lenFieldBuf[:])

	// lower 4 bytes
	nBytes = uint32(lenField)

	// if msb == 1, get padLen
	if lenField&(1<<63) == (1 << 63) {
		padLen = uint8((lenField ^ (1 << 63)) >> 56)
	}
	return
}
