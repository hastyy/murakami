# Durability I/O Benchmark Plan

This document outlines benchmarks to measure the performance of different file I/O durability strategies on Linux. Run these on your target Linux machine to get accurate reference numbers.

## Overview

We want to compare six different approaches to durable writes:

| # | Strategy | Description |
|---|----------|-------------|
| 1 | Buffered + fsync every N | Write to page cache, fsync every 100 writes |
| 2 | Buffered + fdatasync every N | Write to page cache, fdatasync every 100 writes |
| 3 | O_SYNC | Each write() is durable (data + metadata) |
| 4 | O_DSYNC | Each write() is durable (data only) |
| 5 | App buffer + O_SYNC | Buffer in app, single write() with O_SYNC |
| 6 | App buffer + O_DSYNC | Buffer in app, single write() with O_DSYNC |

**Note on O_DIRECT**: O_DIRECT is orthogonal to durability—it bypasses the page cache but doesn't guarantee durability. Since O_SYNC/O_DSYNC already wait for data to reach persistent storage, adding O_DIRECT:
- Doesn't improve durability
- May slightly reduce latency (no page cache copy)
- Requires strict alignment (512B or 4KB)
- Is more complex to implement correctly

We'll test O_DIRECT + O_DSYNC as a separate variant for comparison.

---

## Pre-requisites

### 1. Check System Capabilities

```bash
# Check filesystem block size
stat -f -c '%S' /path/to/test/dir

# Check disk scheduler and queue depth
cat /sys/block/nvme0n1/queue/scheduler
cat /sys/block/nvme0n1/queue/nr_requests

# Check if O_DIRECT is supported
dd if=/dev/zero of=testfile bs=4096 count=1 oflag=direct 2>&1 && rm testfile
```

### 2. Find O_DIRECT Alignment Requirements

```bash
# Logical block size (minimum alignment)
cat /sys/block/nvme0n1/queue/logical_block_size

# Physical block size (optimal alignment)
cat /sys/block/nvme0n1/queue/physical_block_size

# Minimum I/O size for O_DIRECT
cat /sys/block/nvme0n1/queue/minimum_io_size
```

Typical values:
- Logical block size: 512 bytes
- Physical block size: 4096 bytes (4KB)
- For O_DIRECT, align to **physical block size** for best performance

### 3. Disable Write Caching (Optional, for true durability testing)

```bash
# Check current write cache status
sudo hdparm -W /dev/nvme0n1

# Disable write cache (DANGEROUS - reduces performance significantly)
# sudo hdparm -W 0 /dev/nvme0n1
```

**Warning**: Disabling write cache will dramatically reduce performance but shows true durable write latency.

---

## Benchmark Implementation

Create `io_benchmark_test.go` in the project:

```go
//go:build linux

package iobench

import (
	"bufio"
	"fmt"
	"os"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	syncInterval = 100 // Sync every N writes for batched tests
)

// Record sizes to test (matching Murakami benchmarks)
var recordSizes = []int{64, 256, 512, 1024}

// =============================================================================
// Helper Functions
// =============================================================================

func generateData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i * 7) % 256)
	}
	return data
}

// alignedBuffer allocates a buffer aligned to the given boundary (for O_DIRECT)
func alignedBuffer(size, alignment int) []byte {
	buf := make([]byte, size+alignment)
	offset := alignment - int(uintptr(unsafe.Pointer(&buf[0]))%uintptr(alignment))
	if offset == alignment {
		offset = 0
	}
	return buf[offset : offset+size]
}

// preallocateFile preallocates space for the file (reduces metadata updates)
func preallocateFile(f *os.File, size int64) error {
	return unix.Fallocate(int(f.Fd()), 0, 0, size)
}

// =============================================================================
// Benchmark 1: Buffered + fsync every N
// =============================================================================

func BenchmarkBufferedFsync(b *testing.B) {
	for _, size := range recordSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			f, err := os.CreateTemp(b.TempDir(), "bench")
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			// Preallocate to avoid extent allocation during benchmark
			preallocateFile(f, int64(b.N*size*2))

			data := generateData(size)

			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if _, err := f.Write(data); err != nil {
					b.Fatal(err)
				}
				if (i+1)%syncInterval == 0 {
					if err := f.Sync(); err != nil {
						b.Fatal(err)
					}
				}
			}
			// Final sync
			f.Sync()
		})
	}
}

// =============================================================================
// Benchmark 2: Buffered + fdatasync every N
// =============================================================================

func BenchmarkBufferedFdatasync(b *testing.B) {
	for _, size := range recordSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			f, err := os.CreateTemp(b.TempDir(), "bench")
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			preallocateFile(f, int64(b.N*size*2))
			data := generateData(size)

			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if _, err := f.Write(data); err != nil {
					b.Fatal(err)
				}
				if (i+1)%syncInterval == 0 {
					if err := unix.Fdatasync(int(f.Fd())); err != nil {
						b.Fatal(err)
					}
				}
			}
			unix.Fdatasync(int(f.Fd()))
		})
	}
}

// =============================================================================
// Benchmark 3: O_SYNC (sync every write)
// =============================================================================

func BenchmarkOSync(b *testing.B) {
	for _, size := range recordSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			path := fmt.Sprintf("%s/bench_osync", b.TempDir())
			f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_SYNC, 0644)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			preallocateFile(f, int64(b.N*size*2))
			data := generateData(size)

			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if _, err := f.Write(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// =============================================================================
// Benchmark 4: O_DSYNC (sync data every write)
// =============================================================================

func BenchmarkODsync(b *testing.B) {
	for _, size := range recordSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			path := fmt.Sprintf("%s/bench_odsync", b.TempDir())
			f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_DSYNC, 0644)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			preallocateFile(f, int64(b.N*size*2))
			data := generateData(size)

			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if _, err := f.Write(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// =============================================================================
// Benchmark 5: App Buffer + O_SYNC (fewest syscalls)
// =============================================================================

func BenchmarkAppBufferOSync(b *testing.B) {
	for _, size := range recordSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			path := fmt.Sprintf("%s/bench_appbuf_osync", b.TempDir())
			f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_SYNC, 0644)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			preallocateFile(f, int64(b.N*size*2))

			// Use bufio.Writer for app-level buffering
			bufSize := size * syncInterval
			if bufSize < 4096 {
				bufSize = 4096
			}
			buf := bufio.NewWriterSize(f, bufSize)
			data := generateData(size)

			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if _, err := buf.Write(data); err != nil {
					b.Fatal(err)
				}
				if (i+1)%syncInterval == 0 {
					if err := buf.Flush(); err != nil {
						b.Fatal(err)
					}
					// O_SYNC means Flush() already synced to disk
				}
			}
			buf.Flush()
		})
	}
}

// =============================================================================
// Benchmark 6: App Buffer + O_DSYNC (fewest syscalls, data-only sync)
// =============================================================================

func BenchmarkAppBufferODsync(b *testing.B) {
	for _, size := range recordSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			path := fmt.Sprintf("%s/bench_appbuf_odsync", b.TempDir())
			f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_DSYNC, 0644)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			preallocateFile(f, int64(b.N*size*2))

			bufSize := size * syncInterval
			if bufSize < 4096 {
				bufSize = 4096
			}
			buf := bufio.NewWriterSize(f, bufSize)
			data := generateData(size)

			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if _, err := buf.Write(data); err != nil {
					b.Fatal(err)
				}
				if (i+1)%syncInterval == 0 {
					if err := buf.Flush(); err != nil {
						b.Fatal(err)
					}
				}
			}
			buf.Flush()
		})
	}
}

// =============================================================================
// Benchmark 7: O_DIRECT + O_DSYNC (bypass page cache, durable)
// Requires 4KB alignment on most systems
// =============================================================================

func BenchmarkODirectODsync(b *testing.B) {
	// Only test 4KB-aligned sizes for O_DIRECT
	directSizes := []int{512, 1024, 4096}

	for _, size := range directSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			path := fmt.Sprintf("%s/bench_odirect_odsync", b.TempDir())
			
			// O_DIRECT requires special handling
			f, err := os.OpenFile(path, 
				os.O_CREATE|os.O_RDWR|syscall.O_DIRECT|syscall.O_DSYNC, 0644)
			if err != nil {
				b.Skipf("O_DIRECT not supported: %v", err)
			}
			defer f.Close()

			// Must preallocate for O_DIRECT
			fileSize := int64(b.N * size * 2)
			if fileSize < 1024*1024 {
				fileSize = 1024 * 1024 // Minimum 1MB
			}
			preallocateFile(f, fileSize)

			// O_DIRECT requires aligned buffer (4KB alignment)
			alignedData := alignedBuffer(size, 4096)
			for i := range alignedData {
				alignedData[i] = byte((i * 7) % 256)
			}

			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if _, err := f.Write(alignedData); err != nil {
					b.Fatalf("write failed at iteration %d: %v", i, err)
				}
			}
		})
	}
}

// =============================================================================
// Benchmark 8: Pure fsync latency (baseline)
// =============================================================================

func BenchmarkFsyncLatency(b *testing.B) {
	f, err := os.CreateTemp(b.TempDir(), "bench")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	// Write some data first
	data := generateData(4096)
	f.Write(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := f.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFdatasyncLatency(b *testing.B) {
	f, err := os.CreateTemp(b.TempDir(), "bench")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	data := generateData(4096)
	f.Write(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := unix.Fdatasync(int(f.Fd())); err != nil {
			b.Fatal(err)
		}
	}
}
```

---

## Running the Benchmarks

### 1. Setup

```bash
# Create benchmark directory
mkdir -p /path/to/murakami/cmd/iobench
cd /path/to/murakami/cmd/iobench

# Copy the benchmark code above to io_benchmark_test.go

# Get dependencies
go get golang.org/x/sys/unix
```

### 2. Run All Benchmarks

```bash
# Full benchmark suite (takes several minutes)
go test -bench=. -benchmem -benchtime=2s -count=3 | tee results.txt

# Quick test (shorter duration)
go test -bench=. -benchmem -benchtime=500ms
```

### 3. Run Specific Benchmarks

```bash
# Just fsync latency
go test -bench=BenchmarkFsyncLatency -benchmem -benchtime=2s

# Compare buffered approaches
go test -bench='BenchmarkBuffered' -benchmem -benchtime=2s

# Compare O_SYNC vs O_DSYNC
go test -bench='Benchmark(OSync|ODsync)' -benchmem -benchtime=2s

# App buffer approaches
go test -bench='BenchmarkAppBuffer' -benchmem -benchtime=2s
```

### 4. Profile if Needed

```bash
# CPU profile
go test -bench=BenchmarkBufferedFsync -cpuprofile=cpu.prof -benchtime=5s
go tool pprof cpu.prof

# Trace
go test -bench=BenchmarkBufferedFsync -trace=trace.out -benchtime=1s
go tool trace trace.out
```

---

## Expected Results Template

Fill this in with your actual results:

### System Information

```
CPU: 
Memory:
Storage: (NVMe model, capacity)
Filesystem:
Kernel:
Block size:
```

### Baseline: Sync Latency

| Operation | Latency | IOPS |
|-----------|---------|------|
| fsync | ___ ms | ___ |
| fdatasync | ___ ms | ___ |

### Write Throughput (256B records, sync every 100)

| Strategy | Throughput | Latency/write | Latency/sync |
|----------|------------|---------------|--------------|
| Buffered + fsync | ___ MB/s | ___ ns | ___ ms |
| Buffered + fdatasync | ___ MB/s | ___ ns | ___ ms |
| O_SYNC | ___ MB/s | ___ ms | N/A |
| O_DSYNC | ___ MB/s | ___ ms | N/A |
| App buffer + O_SYNC | ___ MB/s | ___ ns | N/A |
| App buffer + O_DSYNC | ___ MB/s | ___ ns | N/A |
| O_DIRECT + O_DSYNC | ___ MB/s | ___ ms | N/A |

### Scaling by Record Size

| Size | Buffered+fsync | O_DSYNC | App+O_DSYNC |
|------|----------------|---------|-------------|
| 64B | ___ MB/s | ___ MB/s | ___ MB/s |
| 256B | ___ MB/s | ___ MB/s | ___ MB/s |
| 512B | ___ MB/s | ___ MB/s | ___ MB/s |
| 1KB | ___ MB/s | ___ MB/s | ___ MB/s |

---

## Analysis Questions

After running benchmarks, answer these:

1. **fsync vs fdatasync**: Is there a measurable difference on your system?
2. **O_SYNC vs O_DSYNC**: How much faster is O_DSYNC?
3. **Batching benefit**: How much does batching 100 writes improve throughput?
4. **App buffer benefit**: Does app-level buffering + O_DSYNC beat buffered + fdatasync?
5. **O_DIRECT overhead**: Is O_DIRECT + O_DSYNC faster or slower than O_DSYNC alone?
6. **Record size impact**: How does throughput scale with record size?

---

## Comparison with Murakami

After running these benchmarks, compare with Murakami's results:

| Metric | Raw I/O Benchmark | Murakami |
|--------|-------------------|----------|
| Buffered write (256B) | ___ MB/s | 1,507 MB/s |
| Sync every 100 (256B) | ___ MB/s | ~10 MB/s |
| Sync every 1 (256B) | ___ MB/s | 0.05 MB/s |

If Murakami is slower than raw I/O benchmarks, the overhead comes from:
- Record encoding (CRC, headers)
- Index maintenance
- Buffer management
- Lock contention

If Murakami is close to raw I/O, the implementation is efficient.

---

## Notes

### O_DIRECT Gotchas

1. **Alignment**: Buffer, offset, and size must be aligned (usually 512B or 4KB)
2. **Page cache bypass**: Subsequent reads won't benefit from cache
3. **Not always faster**: Page cache is highly optimized; O_DIRECT can be slower
4. **Filesystem support**: Not all filesystems support O_DIRECT equally

### fsync vs fdatasync

- `fsync`: Syncs data + all metadata (size, mtime, ctime, etc.)
- `fdatasync`: Syncs data + only metadata required to retrieve data (size if extended)
- On many modern filesystems, the difference is minimal

### O_SYNC vs O_DSYNC

- `O_SYNC`: Equivalent to write() + fsync() per write
- `O_DSYNC`: Equivalent to write() + fdatasync() per write
- O_DSYNC should be slightly faster (less metadata to sync)

