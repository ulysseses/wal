# WAL
Write-Ahead-Log (WAL) implemented with Go.

[![GoDoc](https://godoc.org/github.com/ulysseses/wal?status.svg)](https://godoc.org/github.com/ulysseses/wal)
![Build](https://github.com/ulysseses/wal/workflows/Build/badge.svg?branch=master)
![Tests](https://github.com/ulysseses/wal/workflows/Tests/badge.svg?branch=master)

## Benchmarks

```bash
$ go test -run=^$ -bench .
goos: darwin
goarch: amd64
pkg: github.com/ulysseses/wal
BenchmarkWrite_100B_Batch1-8       	     156	   6534964 ns/op	   0.02 MB/s
BenchmarkWrite_100B_Batch10-8      	    1850	    631955 ns/op	   0.18 MB/s
BenchmarkWrite_100B_Batch100-8     	   19870	     64432 ns/op	   1.80 MB/s
BenchmarkWrite_100B_Batch1000-8    	  150238	      7196 ns/op	  16.12 MB/s
BenchmarkWrite_100B_Batch5000-8    	  766251	      1523 ns/op	  76.16 MB/s
BenchmarkWrite_1000B_Batch1-8      	     183	   6688114 ns/op	   0.15 MB/s
BenchmarkWrite_1000B_Batch10-8     	    1725	    656703 ns/op	   1.55 MB/s
BenchmarkWrite_1000B_Batch100-8    	   17748	     69383 ns/op	  14.70 MB/s
BenchmarkWrite_1000B_Batch1000-8   	  142172	      7986 ns/op	 127.72 MB/s
BenchmarkWrite_1000B_Batch5000-8   	  414115	      2496 ns/op	 408.59 MB/s
BenchmarkWrite_5000B_Batch1-8      	     150	   6744202 ns/op	   0.74 MB/s
BenchmarkWrite_5000B_Batch10-8     	    1682	    670651 ns/op	   7.49 MB/s
BenchmarkWrite_5000B_Batch100-8    	   16718	     70535 ns/op	  71.17 MB/s
BenchmarkWrite_5000B_Batch1000-8   	   97143	     11837 ns/op	 424.10 MB/s
BenchmarkWrite_5000B_Batch5000-8   	  182594	      5578 ns/op	 900.03 MB/s
PASS
ok  	github.com/ulysseses/wal	27.732s
$ 
```