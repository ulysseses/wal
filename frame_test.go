package wal

import (
	"bytes"
	"io"
	"testing"
)

func Test_SerDe(t *testing.T) {
	t.Run("write and read 3 frames successfully", func(t *testing.T) {
		wantString := "hello world!"
		wantData := []byte(wantString)
		totalFrames := 3

		var rwBuffer bytes.Buffer

		f := newFramer(&rwBuffer)
		for i := 1; i <= totalFrames; i++ {
			if _, err := f.frame(wantData); err != nil {
				t.Fatal(err)
			}
		}

		d := newDeframer(&rwBuffer)
		nFrames := 0
		for {
			gotData, _, err := d.deframe()
			if err == nil {
				nFrames++
				gotString := string(gotData)
				if gotString != wantString {
					t.Fatalf("on frame #%d, got %#v, but wanted %#v", nFrames, gotString, wantString)
				}
			} else if err == io.EOF {
				break
			} else {
				t.Fatal(err)
			}
		}

		if nFrames != totalFrames {
			t.Fatalf("wrote %d frames, but only deframed %d", totalFrames, nFrames)
		}
	})

	t.Run("torn write causes errorPartialFrame", func(t *testing.T) {
		frame, err := getFrameData([]byte("Hello world!"))
		if err != nil {
			t.Fatal(err)
		}

		// purposefully write a partial frame (torn write)
		for i := 1; i < len(frame); i++ {
			d := newDeframer(bytes.NewBuffer(frame[:i]))
			got, _, err := d.deframe()
			if err == nil {
				t.Fatalf("tore off %d bytes from end; not supposed to successfully read a frame; got: %#v", len(frame)-i, string(got))
			}
			if _, ok := err.(errorPartialFrame); !ok {
				t.Fatalf("tore off %d bytes from end; did not get errorPartialFrame; instead, got %v", len(frame)-i, err)
			}
		}
	})

	t.Run("flip checksum bit fails checksum", func(t *testing.T) {
		frame, err := getFrameData([]byte("Hello world!"))
		if err != nil {
			t.Fatal(err)
		}

		// flip the 7th bit of the 3rd checksum byte
		frame[8+3] = frame[8+3] ^ (1 << 6)

		d := newDeframer(bytes.NewBuffer(frame))
		_, _, err = d.deframe()
		if _, ok := err.(errorChecksum); !ok {
			t.Fatalf("supposed to get errorChecksum, not %v", err)
		}
	})

	t.Run("flip data bit fails checksum", func(t *testing.T) {
		frame, err := getFrameData([]byte("Hello world!"))
		if err != nil {
			t.Fatal(err)
		}

		// flip the 5th bit of the 11th data byte
		frame[11+5] = frame[11+5] ^ (1 << 4)

		d := newDeframer(bytes.NewBuffer(frame))
		_, _, err = d.deframe()
		if _, ok := err.(errorChecksum); !ok {
			t.Fatalf("supposed to get errorChecksum, not %v", err)
		}
	})
}

func getFrameData(data []byte) ([]byte, error) {
	var rwBuffer bytes.Buffer
	f := newFramer(&rwBuffer)
	if _, err := f.frame(data); err != nil {
		return nil, err
	}
	return []byte(rwBuffer.String()), nil
}
