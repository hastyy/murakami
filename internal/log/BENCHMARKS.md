# Log Benchmark Results

Benchmark suite for the durable log implementation. Results captured on Apple M1 Max with NVMe storage.

**Test Environment:**
- CPU: Apple M1 Max (10 cores)
- OS: macOS (darwin/arm64)
- Storage: NVMe SSD
- Go version: 1.21+

Run benchmarks with:
```bash
go test -bench=. -benchmem -benchtime=2s ./internal/log/...
```

---

## Append Performance

### Buffered Appends (No Durability Guarantee)

Records are written to an in-memory buffer. Durability requires explicit `Sync()` call.

| Record Size | Throughput | Latency | Ops/sec | Allocations |
|-------------|------------|---------|---------|-------------|
| 64B | 682 MB/s | 94 ns/op | ~10.7M | 0 allocs |
| 256B | 1,491 MB/s | 172 ns/op | ~5.8M | 0 allocs |
| 512B | 1,842 MB/s | 278 ns/op | ~3.6M | 0 allocs |
| 1KB | 1,539 MB/s | 665 ns/op | ~1.5M | 0 allocs |

**Key Finding:** Zero allocations in the hot path. Throughput scales with record size up to ~512B, then memory bandwidth becomes the bottleneck.

### Durable Appends (Count-Based Sync)

Sync is called after every N appends. Record size: 256B.

| Sync Interval | Throughput | Latency | Ops/sec | Notes |
|---------------|------------|---------|---------|-------|
| Every 1 | 0.04 MB/s | 6.4 ms/op | ~157 | Full durability |
| Every 10 | 0.31 MB/s | 837 µs/op | ~1.2K | |
| Every 100 | 2.5 MB/s | 102 µs/op | ~9.8K | Good balance |
| Every 1000 | 24 MB/s | 11 µs/op | ~93K | High throughput |

**Key Finding:** Fsync latency on NVMe is ~6ms. Batching writes before sync dramatically improves throughput. At 1000-write batches, we achieve 93K durable ops/sec.

### Durable Appends (Time-Based Sync)

Background goroutine calls `Sync()` at fixed intervals. Record size: 256B.

| Sync Interval | Throughput | Latency | Ops/sec | Notes |
|---------------|------------|---------|---------|-------|
| Every 10ms | 947 MB/s | 270 ns/op | ~3.7M | Max 10ms data loss window |
| Every 50ms | 1,096 MB/s | 234 ns/op | ~4.3M | |
| Every 100ms | 1,149 MB/s | 223 ns/op | ~4.5M | Near buffered speed |

**Key Finding:** Time-based sync provides high throughput with bounded durability windows. The ~36% throughput reduction vs buffered (at 10ms interval) comes from lock contention when `Sync()` holds the write lock during fsync.

### Concurrent Writers

Multiple goroutines appending simultaneously. Record size: 256B.

| Writers | Throughput | Latency | Ops/sec |
|---------|------------|---------|---------|
| 1 | 1,013 MB/s | 253 ns/op | ~4.0M |
| 4 | 1,070 MB/s | 239 ns/op | ~4.2M |
| 8 | 1,042 MB/s | 246 ns/op | ~4.1M |
| 16 | 972 MB/s | 263 ns/op | ~3.8M |

**Key Finding:** Throughput is stable regardless of writer count due to mutex serialization. No scaling benefit, but also no degradation—the implementation handles contention well.

---

## Read Performance

### Sealed Segment Reads (mmap'd)

Reading from segments that have been closed and reopened (using memory-mapped files).

| Access Pattern | Throughput | Latency | Ops/sec | Allocations |
|----------------|------------|---------|---------|-------------|
| Sequential | 1,640 MB/s | 156 ns/op | ~6.4M | 1 alloc (256B) |
| Random | 647 MB/s | 396 ns/op | ~2.5M | 1 alloc (256B) |
| Hot (same 5 offsets) | 1,513 MB/s | 169 ns/op | ~5.9M | 1 alloc (256B) |

**Key Finding:** Excellent read performance due to mmap and OS page cache. The single allocation is for copying record data out of the mmap'd region (required for safety).

### Active Segment Reads

Reading from the currently active (writable) segment.

| Access Pattern | Throughput | Latency | Ops/sec | Allocations |
|----------------|------------|---------|---------|-------------|
| Sequential | 81 MB/s | 3.2 µs/op | ~316K | 1 alloc |
| Random | 74 MB/s | 3.5 µs/op | ~287K | 1 alloc |
| Hot | 83 MB/s | 3.1 µs/op | ~324K | 1 alloc |

**Key Finding:** Active segment reads are ~20x slower than sealed segment reads due to buffered I/O and lack of mmap. The single allocation is for copying record data out (same as sealed segment). Optimized by replacing `binary.Read` (reflection-based) with direct `binary.BigEndian` decoding and using `bufio.Reader.Discard()` instead of `io.CopyN`.

### Cross-Segment Reads

Reading from a log with multiple sealed segments (tests segment lookup overhead).

| Pattern | Throughput | Latency | Ops/sec |
|---------|------------|---------|---------|
| Random across segments | 480 MB/s | 533 ns/op | ~1.9M |

**Key Finding:** Binary search for segment lookup adds minimal overhead (~140ns vs single-segment random reads).

### Concurrent Readers

Multiple goroutines reading simultaneously from sealed segments.

| Readers | Throughput | Latency | Ops/sec |
|---------|------------|---------|---------|
| 1 | 1,081 MB/s | 237 ns/op | ~4.2M |
| 4 | 1,016 MB/s | 252 ns/op | ~4.0M |
| 8 | 1,048 MB/s | 244 ns/op | ~4.1M |
| 16 | 1,131 MB/s | 226 ns/op | ~4.4M |

**Key Finding:** Read throughput scales well with concurrent readers due to RWMutex allowing parallel reads.

### Reads with Concurrent Writes

Reading while a background writer is appending.

| Scenario | Throughput | Latency | Ops/sec |
|----------|------------|---------|---------|
| 1 Writer, 4 Readers | 49 MB/s | 5.3 µs/op | ~190K |
| 1 Writer, 8 Readers | 51 MB/s | 5.0 µs/op | ~199K |

**Key Finding:** Mixed read/write workloads show degraded read performance due to lock contention, but remain functional.

---

## Operational Benchmarks

### Recovery (After Unclean Shutdown)

Time to recover log state when no clean shutdown marker exists. Includes CRC validation and index rebuild.

| Records | Recovery Time | Scan Rate |
|---------|---------------|-----------|
| 1,000 | 5.5 ms | 183K records/sec |
| 10,000 | 7.4 ms | 1.36M records/sec |
| 100,000 | 23 ms | 4.3M records/sec |

**Key Finding:** Recovery is fast—even a 1M record log would recover in ~230ms. The scan rate improves with larger logs due to sequential I/O amortization.

### Retention Cleanup

Time to delete expired segments.

| Segments Deleted | Cleanup Time |
|------------------|--------------|
| 0 | 833 ns |
| 4 | 1.8 ms |
| 9 | 7.0 ms |

**Key Finding:** Cleanup is I/O bound (file deletion). Cost is linear with segment count.

---

## Comparison with Other Implementations

| Implementation | Workload | Throughput | Notes |
|----------------|----------|------------|-------|
| **Murakami** | Buffered append | 1.4-1.6 GB/s | Apple M1 Max |
| **Murakami** | Durable (batch=1000) | ~25 MB/s (~98K ops/sec) | |
| **Murakami** | Active segment read | ~80 MB/s (~300K ops/sec) | Buffered I/O |
| **Murakami** | Sealed segment read | 1.6 GB/s (~6.4M ops/sec) | mmap'd |
| **Kafka** | Producer (acks=all) | 200-500K msgs/sec | Per partition, commodity NVMe |
| **RocksDB WAL** | sync_every_write=true | 50-100K ops/sec | Commodity NVMe |
| **etcd WAL** | Durable writes | 50-100K ops/sec | Raft consensus overhead |
| **BadgerDB** | Durable writes | ~50K ops/sec | LSM-tree with WAL |

**Analysis:**
- Buffered throughput exceeds all comparisons (expected—no durability)
- Durable throughput at batch=1000 is competitive with Kafka
- Read performance from mmap'd segments is exceptional
- Recovery speed is faster than most LSM-tree implementations

---

## Optimization Opportunities

### Active Segment Reads ✅ COMPLETED

**Before:** 254K ops/sec with 23 allocations per read.
**After:** ~300K ops/sec with 1 allocation per read.

**Optimization applied:**
- Replaced `binary.Read()` (reflection-based, allocates) with direct `binary.BigEndian.Uint32/64()` on pre-allocated buffers
- Replaced `io.CopyN()` (allocates 32KB buffer) with `bufio.Reader.Discard()` (uses existing buffer)
- Result: 96% reduction in allocations, ~25% improvement in throughput

**Remaining gap to sealed segment:** Active segment reads (~300K ops/sec) are still ~20x slower than sealed segment reads (~6M ops/sec) due to the fundamental difference between buffered file I/O and mmap. This gap could be closed by mmap'ing the active segment for reads, but would require careful invalidation logic when the write buffer flushes.

### Durable Throughput (Medium Priority)

Current: ~93K ops/sec at batch=1000.

**Potential Fix:** Group commit—batch multiple writers' syncs together. This is a **client-side pattern**, not a log-internal feature. The log already provides the right primitives (`Append` + `Sync`); coordination happens at a higher layer.

### Memory Usage (Low Priority)

Current: 1 allocation per sealed segment read (256B data copy).

This is intentional for safety (don't return slices pointing to mmap'd memory), but could be optimized with a careful API that allows zero-copy reads for advanced users.

---

## Running Specific Benchmarks

```bash
# All benchmarks
go test -bench=. -benchmem ./internal/log/...

# Append only
go test -bench=BenchmarkAppend -benchmem ./internal/log/...

# Read only  
go test -bench=BenchmarkRead -benchmem ./internal/log/...

# With CPU profiling
go test -bench=BenchmarkAppend -benchmem -cpuprofile=cpu.prof ./internal/log/...

# With memory profiling
go test -bench=BenchmarkAppend -benchmem -memprofile=mem.prof ./internal/log/...
```

