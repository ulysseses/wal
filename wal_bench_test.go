package wal

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkWrite_100B_Batch1(b *testing.B) {
	benchmarkWrite(b, 100, 1)
}

func BenchmarkWrite_100B_Batch10(b *testing.B) {
	benchmarkWrite(b, 100, 10)
}

func BenchmarkWrite_100B_Batch100(b *testing.B) {
	benchmarkWrite(b, 100, 100)
}

func BenchmarkWrite_100B_Batch1000(b *testing.B) {
	benchmarkWrite(b, 100, 1000)
}

func BenchmarkWrite_100B_Batch5000(b *testing.B) {
	benchmarkWrite(b, 100, 5000)
}

func BenchmarkWrite_1000B_Batch1(b *testing.B) {
	benchmarkWrite(b, 1000, 1)
}

func BenchmarkWrite_1000B_Batch10(b *testing.B) {
	benchmarkWrite(b, 1000, 10)
}

func BenchmarkWrite_1000B_Batch100(b *testing.B) {
	benchmarkWrite(b, 1000, 100)
}

func BenchmarkWrite_1000B_Batch1000(b *testing.B) {
	benchmarkWrite(b, 1000, 1000)
}

func BenchmarkWrite_1000B_Batch5000(b *testing.B) {
	benchmarkWrite(b, 1000, 5000)
}

func BenchmarkWrite_5000B_Batch1(b *testing.B) {
	benchmarkWrite(b, 5000, 1)
}

func BenchmarkWrite_5000B_Batch10(b *testing.B) {
	benchmarkWrite(b, 5000, 10)
}

func BenchmarkWrite_5000B_Batch100(b *testing.B) {
	benchmarkWrite(b, 5000, 100)
}

func BenchmarkWrite_5000B_Batch1000(b *testing.B) {
	benchmarkWrite(b, 5000, 1000)
}

func BenchmarkWrite_5000B_Batch5000(b *testing.B) {
	benchmarkWrite(b, 5000, 5000)
}

func benchmarkWrite(b *testing.B, nBytes int, batch int) {
	// Create a new WAL.
	baseDir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(baseDir)
	walDir := filepath.Join(baseDir, b.Name())
	wal, err := OpenWAL(walDir, SegmentSizeBytes, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	// Benchmark.
	data := make([]byte, nBytes)
	b.SetBytes(int64(frameSize(len(data))))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := wal.writeNoCut(data); err != nil && err != errSegmentSizeReached {
			b.Fatal(err)
		}
		if i%batch == batch-1 {
			if err := wal.Sync(); err != nil {
				b.Fatal(err)
			}
		}
	}
}
