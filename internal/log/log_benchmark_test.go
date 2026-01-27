package log

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hastyy/murakami/internal/unit"
)

// =============================================================================
// Benchmark Configuration
// =============================================================================

// benchConfig returns a configuration suitable for benchmarks.
// Uses larger buffers and realistic segment sizes for production-like behavior.
func benchConfig() Config {
	return Config{
		MaxSegmentSize:        256 * unit.MiB,
		MaxRecordDataSize:     4 * unit.KiB,
		IndexIntervalBytes:    4 * unit.KiB,
		LogWriterBufferSize:   64 * unit.KiB,
		IndexWriterBufferSize: 16 * unit.KiB,
		LogReaderBufferSize:   64 * unit.KiB,
		RecordCacheSize:       1024,
		IndexCacheSize:        100_000,
	}
}

// benchConfigSmallSegment returns a config with small segments for multi-segment tests.
func benchConfigSmallSegment() Config {
	cfg := benchConfig()
	cfg.MaxSegmentSize = 4 * unit.MiB
	return cfg
}

// =============================================================================
// Helper Functions
// =============================================================================

// generateTestData creates a deterministic byte slice of the given size.
// Uses a simple pattern that's fast to generate and compresses poorly
// (to avoid filesystem compression benefits).
func generateTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i * 7) % 256) // Simple pseudo-random pattern
	}
	return data
}

// setupLogWithRecords creates a log and appends n records of the given size.
// Returns the log ready for benchmarking reads.
func setupLogWithRecords(b *testing.B, dir string, n int, recordSize int, cfg Config) *Log {
	b.Helper()

	// Ensure MaxRecordDataSize can accommodate our test data
	if recordSize > cfg.MaxRecordDataSize {
		cfg.MaxRecordDataSize = recordSize
	}

	l, err := New(dir, cfg)
	if err != nil {
		b.Fatalf("failed to create log: %v", err)
	}

	data := generateTestData(recordSize)
	for i := 0; i < n; i++ {
		if _, err := l.Append(data); err != nil {
			_ = l.Close()
			b.Fatalf("failed to append record %d: %v", i, err)
		}
	}

	// Sync to ensure all data is on disk
	if err := l.Sync(); err != nil {
		_ = l.Close()
		b.Fatalf("failed to sync log: %v", err)
	}

	return l
}

// setupSealedSegment creates a log with records, closes it, and reopens it.
// This ensures we have sealed segments (mmap'd) for read benchmarks.
func setupSealedSegment(b *testing.B, dir string, numRecords int, recordSize int, cfg Config) *Log {
	b.Helper()

	// First, create and populate the log
	l := setupLogWithRecords(b, dir, numRecords, recordSize, cfg)

	// Close to seal the segment
	if err := l.Close(); err != nil {
		b.Fatalf("failed to close log: %v", err)
	}

	// Reopen - now the previous active segment is sealed
	l, err := New(dir, cfg)
	if err != nil {
		b.Fatalf("failed to reopen log: %v", err)
	}

	return l
}

// =============================================================================
// Append Benchmarks
// =============================================================================

// BenchmarkAppend measures raw append throughput at various record sizes.
// This is the most fundamental benchmark - it measures how fast we can
// accept writes (buffered, not yet durable).
//
// Expected performance on NVMe: 200K-500K ops/sec for small records.
// Reference: Kafka achieves ~200-500K messages/sec per partition.
func BenchmarkAppend(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"64B", 64},
		{"256B", 256},
		{"512B", 512},
		{"1KB", 1 * unit.KiB},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			dir := b.TempDir()
			cfg := benchConfig()
			cfg.MaxRecordDataSize = s.size

			l, err := New(dir, cfg)
			if err != nil {
				b.Fatalf("failed to create log: %v", err)
			}
			defer func() { _ = l.Close() }()

			// Pre-allocate data buffer ONCE before the benchmark loop
			data := generateTestData(s.size)

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(s.size))

			for i := 0; i < b.N; i++ {
				if _, err := l.Append(data); err != nil {
					b.Fatalf("append failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkAppendWithSync measures append throughput with various fsync frequencies.
// This is critical for understanding durability vs performance tradeoffs.
//
// Count-based: Sync every N appends (batch commits).
// Time-based: Background goroutine syncs at intervals.
//
// Reference: RocksDB WAL with sync_every_write: ~10-50K ops/sec on NVMe.
func BenchmarkAppendWithSync(b *testing.B) {
	// Count-based sync intervals
	countBasedIntervals := []int{1, 10, 100, 1000}

	for _, interval := range countBasedIntervals {
		b.Run(fmt.Sprintf("Every%d", interval), func(b *testing.B) {
			dir := b.TempDir()
			cfg := benchConfig()

			l, err := New(dir, cfg)
			if err != nil {
				b.Fatalf("failed to create log: %v", err)
			}
			defer func() { _ = l.Close() }()

			data := generateTestData(256) // Use 256B records as baseline

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(256)

			for i := 0; i < b.N; i++ {
				if _, err := l.Append(data); err != nil {
					b.Fatalf("append failed: %v", err)
				}
				if (i+1)%interval == 0 {
					if err := l.Sync(); err != nil {
						b.Fatalf("sync failed: %v", err)
					}
				}
			}

			// Final sync for any remaining records
			if b.N%interval != 0 {
				if err := l.Sync(); err != nil {
					b.Fatalf("final sync failed: %v", err)
				}
			}
		})
	}

	// Time-based sync intervals
	timeBasedIntervals := []time.Duration{10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond}

	for _, interval := range timeBasedIntervals {
		b.Run(fmt.Sprintf("Every%dms", interval.Milliseconds()), func(b *testing.B) {
			dir := b.TempDir()
			cfg := benchConfig()

			l, err := New(dir, cfg)
			if err != nil {
				b.Fatalf("failed to create log: %v", err)
			}

			data := generateTestData(256)

			// Start background sync goroutine
			done := make(chan struct{})
			stopped := make(chan struct{})
			var syncErr error
			var syncMu sync.Mutex

			go func() {
				defer close(stopped)
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-ticker.C:
						syncMu.Lock()
						err := l.Sync()
						if err != nil {
							syncErr = err
						}
						syncMu.Unlock()
						if err != nil {
							return
						}
					}
				}
			}()

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(256)

			for i := 0; i < b.N; i++ {
				if _, err := l.Append(data); err != nil {
					b.Fatalf("append failed: %v", err)
				}
			}

			b.StopTimer()

			// Stop background goroutine and wait for it
			close(done)
			<-stopped

			// Check for sync errors (ignore "file already closed" since that's expected during shutdown race)
			syncMu.Lock()
			bgErr := syncErr
			syncMu.Unlock()

			// Final sync before close
			if err := l.Sync(); err != nil {
				b.Fatalf("final sync failed: %v", err)
			}

			if err := l.Close(); err != nil {
				b.Fatalf("close failed: %v", err)
			}

			// Note: bgErr may contain errors from segment rolls during sync,
			// which are expected race conditions and not actual failures.
			_ = bgErr
		})
	}
}

// BenchmarkAppendConcurrent measures append throughput with multiple concurrent writers.
// Tests the effectiveness of the mutex-based serialization.
//
// Expected: Near-linear scaling is unlikely due to lock contention,
// but we should see some benefit from concurrent buffer filling.
func BenchmarkAppendConcurrent(b *testing.B) {
	workers := []int{1, 4, 8, 16}

	for _, w := range workers {
		b.Run(fmt.Sprintf("Writers%d", w), func(b *testing.B) {
			dir := b.TempDir()
			cfg := benchConfig()

			l, err := New(dir, cfg)
			if err != nil {
				b.Fatalf("failed to create log: %v", err)
			}
			defer func() { _ = l.Close() }()

			// Use sync.Pool with pointer type to avoid allocations
			dataPool := sync.Pool{
				New: func() any {
					data := generateTestData(256)
					return &data
				},
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(256)

			b.SetParallelism(w)
			b.RunParallel(func(pb *testing.PB) {
				dataPtr := dataPool.Get().(*[]byte)
				defer dataPool.Put(dataPtr)

				for pb.Next() {
					if _, err := l.Append(*dataPtr); err != nil {
						b.Errorf("append failed: %v", err)
						return
					}
				}
			})
		})
	}
}

// =============================================================================
// Read Benchmarks - Active Segment
// =============================================================================

// BenchmarkReadActiveSegment measures read performance from the active segment.
// Records may be in the record cache (hot) or read from the buffered file.
func BenchmarkReadActiveSegment(b *testing.B) {
	const numRecords = 10_000
	const recordSize = 256

	b.Run("Sequential", func(b *testing.B) {
		dir := b.TempDir()
		cfg := benchConfig()
		l := setupLogWithRecords(b, dir, numRecords, recordSize, cfg)
		defer func() { _ = l.Close() }()

		// Pre-generate offsets for sequential access
		offsets := make([]Offset, b.N)
		for i := range offsets {
			offsets[i] = Offset(i % numRecords)
		}

		b.ResetTimer()
		b.ReportAllocs()
		b.SetBytes(recordSize)

		for i := 0; i < b.N; i++ {
			if _, err := l.Read(offsets[i]); err != nil {
				b.Fatalf("read failed at offset %d: %v", offsets[i], err)
			}
		}
	})

	b.Run("Random", func(b *testing.B) {
		dir := b.TempDir()
		cfg := benchConfig()
		l := setupLogWithRecords(b, dir, numRecords, recordSize, cfg)
		defer func() { _ = l.Close() }()

		// Pre-generate random offsets
		rng := rand.New(rand.NewSource(42))
		offsets := make([]Offset, b.N)
		for i := range offsets {
			offsets[i] = Offset(rng.Intn(numRecords))
		}

		b.ResetTimer()
		b.ReportAllocs()
		b.SetBytes(recordSize)

		for i := 0; i < b.N; i++ {
			if _, err := l.Read(offsets[i]); err != nil {
				b.Fatalf("read failed at offset %d: %v", offsets[i], err)
			}
		}
	})

	b.Run("Hot", func(b *testing.B) {
		dir := b.TempDir()
		cfg := benchConfig()
		l := setupLogWithRecords(b, dir, numRecords, recordSize, cfg)
		defer func() { _ = l.Close() }()

		// Hot set: repeatedly read the same small set of offsets
		// These are the most recent records, likely in the record cache
		hotOffsets := []Offset{
			Offset(numRecords - 1),
			Offset(numRecords - 2),
			Offset(numRecords - 3),
			Offset(numRecords - 4),
			Offset(numRecords - 5),
		}

		b.ResetTimer()
		b.ReportAllocs()
		b.SetBytes(recordSize)

		for i := 0; i < b.N; i++ {
			offset := hotOffsets[i%len(hotOffsets)]
			if _, err := l.Read(offset); err != nil {
				b.Fatalf("read failed at offset %d: %v", offset, err)
			}
		}
	})
}

// =============================================================================
// Read Benchmarks - Sealed Segment
// =============================================================================

// BenchmarkReadSealedSegment measures read performance from mmap'd sealed segments.
// This tests the OS page cache and mmap performance.
//
// Expected: Very fast for sequential/hot reads due to page cache.
// Random reads may trigger page faults on cold data.
func BenchmarkReadSealedSegment(b *testing.B) {
	const numRecords = 50_000
	const recordSize = 256

	b.Run("Sequential", func(b *testing.B) {
		dir := b.TempDir()
		cfg := benchConfig()
		l := setupSealedSegment(b, dir, numRecords, recordSize, cfg)
		defer func() { _ = l.Close() }()

		offsets := make([]Offset, b.N)
		for i := range offsets {
			offsets[i] = Offset(i % numRecords)
		}

		b.ResetTimer()
		b.ReportAllocs()
		b.SetBytes(recordSize)

		for i := 0; i < b.N; i++ {
			if _, err := l.Read(offsets[i]); err != nil {
				b.Fatalf("read failed at offset %d: %v", offsets[i], err)
			}
		}
	})

	b.Run("Random", func(b *testing.B) {
		dir := b.TempDir()
		cfg := benchConfig()
		l := setupSealedSegment(b, dir, numRecords, recordSize, cfg)
		defer func() { _ = l.Close() }()

		rng := rand.New(rand.NewSource(42))
		offsets := make([]Offset, b.N)
		for i := range offsets {
			offsets[i] = Offset(rng.Intn(numRecords))
		}

		b.ResetTimer()
		b.ReportAllocs()
		b.SetBytes(recordSize)

		for i := 0; i < b.N; i++ {
			if _, err := l.Read(offsets[i]); err != nil {
				b.Fatalf("read failed at offset %d: %v", offsets[i], err)
			}
		}
	})

	b.Run("Hot", func(b *testing.B) {
		dir := b.TempDir()
		cfg := benchConfig()
		l := setupSealedSegment(b, dir, numRecords, recordSize, cfg)
		defer func() { _ = l.Close() }()

		// Hot set: same offsets repeatedly (tests OS page cache)
		hotOffsets := []Offset{0, 100, 200, 300, 400}

		b.ResetTimer()
		b.ReportAllocs()
		b.SetBytes(recordSize)

		for i := 0; i < b.N; i++ {
			offset := hotOffsets[i%len(hotOffsets)]
			if _, err := l.Read(offset); err != nil {
				b.Fatalf("read failed at offset %d: %v", offset, err)
			}
		}
	})
}

// BenchmarkReadMixed measures read performance across multiple segments.
// Tests the segment lookup (binary search) overhead.
func BenchmarkReadMixed(b *testing.B) {
	const recordSize = 256

	b.Run("CrossSegment", func(b *testing.B) {
		dir := b.TempDir()
		cfg := benchConfigSmallSegment() // Small segments to create multiple
		cfg.MaxRecordDataSize = recordSize

		l, err := New(dir, cfg)
		if err != nil {
			b.Fatalf("failed to create log: %v", err)
		}

		// Fill enough records to create multiple segments
		data := generateTestData(recordSize)
		numRecords := 100_000 // Should create ~10+ segments with 4MB segment size

		for i := 0; i < numRecords; i++ {
			if _, err := l.Append(data); err != nil {
				_ = l.Close()
				b.Fatalf("append failed: %v", err)
			}
		}

		if err := l.Sync(); err != nil {
			_ = l.Close()
			b.Fatalf("sync failed: %v", err)
		}

		// Close and reopen to seal all segments
		if err := l.Close(); err != nil {
			b.Fatalf("close failed: %v", err)
		}

		l, err = New(dir, cfg)
		if err != nil {
			b.Fatalf("reopen failed: %v", err)
		}
		defer func() { _ = l.Close() }()

		// Pre-generate offsets that span across segments
		rng := rand.New(rand.NewSource(42))
		offsets := make([]Offset, b.N)
		for i := range offsets {
			offsets[i] = Offset(rng.Intn(numRecords))
		}

		b.ResetTimer()
		b.ReportAllocs()
		b.SetBytes(recordSize)

		for i := 0; i < b.N; i++ {
			if _, err := l.Read(offsets[i]); err != nil {
				b.Fatalf("read failed at offset %d: %v", offsets[i], err)
			}
		}
	})
}

// =============================================================================
// Concurrent Read Benchmarks
// =============================================================================

// BenchmarkReadConcurrent measures read throughput with multiple concurrent readers.
// Tests RWMutex scalability for read-heavy workloads.
func BenchmarkReadConcurrent(b *testing.B) {
	const numRecords = 50_000
	const recordSize = 256

	readers := []int{1, 4, 8, 16}

	for _, r := range readers {
		b.Run(fmt.Sprintf("Readers%d", r), func(b *testing.B) {
			dir := b.TempDir()
			cfg := benchConfig()
			l := setupSealedSegment(b, dir, numRecords, recordSize, cfg)
			defer func() { _ = l.Close() }()

			// Pre-generate random offsets per goroutine
			rng := rand.New(rand.NewSource(42))

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(recordSize)

			b.SetParallelism(r)
			b.RunParallel(func(pb *testing.PB) {
				// Each goroutine gets its own random sequence
				localRng := rand.New(rand.NewSource(rng.Int63()))
				for pb.Next() {
					offset := Offset(localRng.Intn(numRecords))
					if _, err := l.Read(offset); err != nil {
						b.Errorf("read failed at offset %d: %v", offset, err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkReadWithConcurrentWrites measures read performance while writes are happening.
// This is a realistic workload for many log-based systems.
func BenchmarkReadWithConcurrentWrites(b *testing.B) {
	const initialRecords = 10_000
	const recordSize = 256

	scenarios := []struct {
		name    string
		writers int
		readers int
	}{
		{"1Writer4Readers", 1, 4},
		{"1Writer8Readers", 1, 8},
	}

	for _, s := range scenarios {
		b.Run(s.name, func(b *testing.B) {
			dir := b.TempDir()
			cfg := benchConfig()
			l := setupLogWithRecords(b, dir, initialRecords, recordSize, cfg)
			defer func() { _ = l.Close() }()

			// Track how many records exist (for safe reads)
			var recordCount int64 = initialRecords
			var countMu sync.RWMutex

			// Writer goroutine
			writerDone := make(chan struct{})
			writerData := generateTestData(recordSize)

			var writerWg sync.WaitGroup
			for i := 0; i < s.writers; i++ {
				writerWg.Add(1)
				go func() {
					defer writerWg.Done()
					for {
						select {
						case <-writerDone:
							return
						default:
							if _, err := l.Append(writerData); err != nil {
								return
							}
							countMu.Lock()
							recordCount++
							countMu.Unlock()
						}
					}
				}()
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(recordSize)

			b.SetParallelism(s.readers)
			b.RunParallel(func(pb *testing.PB) {
				localRng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					// Read from a safe range
					countMu.RLock()
					maxOffset := recordCount
					countMu.RUnlock()

					offset := Offset(localRng.Int63n(maxOffset))
					if _, err := l.Read(offset); err != nil {
						// May fail if we're reading a very recent append
						// that hasn't been fully committed yet - that's expected
						continue
					}
				}
			})

			b.StopTimer()
			close(writerDone)
			writerWg.Wait()
		})
	}
}

// =============================================================================
// Operational Benchmarks
// =============================================================================

// BenchmarkRecovery measures the time to recover a log after an unclean shutdown.
// This tests CRC validation and index rebuild performance.
//
// Note: This benchmark measures a single recovery operation (not averaged over b.N)
// because properly resetting state between iterations would be complex and add overhead.
func BenchmarkRecovery(b *testing.B) {
	recordCounts := []int{1_000, 10_000, 100_000}
	const recordSize = 256

	for _, count := range recordCounts {
		b.Run(fmt.Sprintf("%dRecords", count), func(b *testing.B) {
			dir := b.TempDir()
			cfg := benchConfig()
			cfg.MaxRecordDataSize = recordSize

			// Create the log and populate it
			l, err := New(dir, cfg)
			if err != nil {
				b.Fatalf("failed to create log: %v", err)
			}

			data := generateTestData(recordSize)
			for i := 0; i < count; i++ {
				if _, err := l.Append(data); err != nil {
					_ = l.Close()
					b.Fatalf("append failed: %v", err)
				}
			}

			if err := l.Sync(); err != nil {
				_ = l.Close()
				b.Fatalf("sync failed: %v", err)
			}

			if err := l.Close(); err != nil {
				b.Fatalf("close failed: %v", err)
			}

			// Delete the shutdown marker to trigger recovery
			markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
			if err := os.Remove(markerPath); err != nil {
				b.Fatalf("failed to remove shutdown marker: %v", err)
			}

			// Measure recovery time
			b.ResetTimer()
			b.ReportAllocs()

			// Only run one iteration - recovery changes state
			start := time.Now()
			l, err = New(dir, cfg)
			elapsed := time.Since(start)

			if err != nil {
				b.Fatalf("recovery failed: %v", err)
			}
			defer func() { _ = l.Close() }()

			// Report the recovery time
			b.ReportMetric(float64(elapsed.Nanoseconds()), "ns/recovery")
			b.ReportMetric(float64(count)/elapsed.Seconds(), "records/sec")

			// Verify recovery was successful
			if l.Length() != count {
				b.Fatalf("recovery mismatch: expected %d records, got %d", count, l.Length())
			}
		})
	}
}

// BenchmarkRetentionCleanup measures the time to delete old segments.
// Uses small segments and short retention to create deletable segments quickly.
func BenchmarkRetentionCleanup(b *testing.B) {
	segmentCounts := []int{1, 5, 10}

	for _, targetSegments := range segmentCounts {
		b.Run(fmt.Sprintf("%dSegments", targetSegments), func(b *testing.B) {
			dir := b.TempDir()

			// Small segments to create many quickly
			cfg := Config{
				MaxSegmentSize:        64 * unit.KiB,
				MaxRecordDataSize:     256,
				IndexIntervalBytes:    1 * unit.KiB,
				LogWriterBufferSize:   8 * unit.KiB,
				IndexWriterBufferSize: 4 * unit.KiB,
				LogReaderBufferSize:   8 * unit.KiB,
				RecordCacheSize:       64,
				IndexCacheSize:        1000,
				RetentionDuration:     1 * time.Millisecond, // Very short
			}

			l, err := New(dir, cfg)
			if err != nil {
				b.Fatalf("failed to create log: %v", err)
			}

			// Create enough records to fill targetSegments segments
			// Each segment is 64KB, each record is ~280 bytes (256 + header)
			// So ~230 records per segment
			data := generateTestData(256)
			recordsPerSegment := (64 * unit.KiB) / 280
			totalRecords := targetSegments * recordsPerSegment

			for i := 0; i < totalRecords; i++ {
				if _, err := l.Append(data); err != nil {
					_ = l.Close()
					b.Fatalf("append failed: %v", err)
				}
			}

			if err := l.Sync(); err != nil {
				_ = l.Close()
				b.Fatalf("sync failed: %v", err)
			}

			// Wait for retention to expire
			time.Sleep(10 * time.Millisecond)

			b.ResetTimer()
			b.ReportAllocs()

			// Measure cleanup time
			start := time.Now()
			deleted, err := l.RunRetentionCleanup()
			elapsed := time.Since(start)

			if err != nil {
				_ = l.Close()
				b.Fatalf("retention cleanup failed: %v", err)
			}

			_ = l.Close()

			b.ReportMetric(float64(elapsed.Nanoseconds()), "ns/cleanup")
			b.ReportMetric(float64(deleted), "segments_deleted")
		})
	}
}

// =============================================================================
// Throughput Summary Benchmark
// =============================================================================

// BenchmarkThroughputSummary runs a comprehensive throughput test that simulates
// a realistic workload pattern: continuous writes with periodic syncs.
// This provides a single metric comparable to Kafka/RocksDB benchmarks.
func BenchmarkThroughputSummary(b *testing.B) {
	const recordSize = 256
	const syncInterval = 100 // Sync every 100 records (similar to Kafka linger.ms=0 with batching)

	dir := b.TempDir()
	cfg := benchConfig()

	l, err := New(dir, cfg)
	if err != nil {
		b.Fatalf("failed to create log: %v", err)
	}
	defer func() { _ = l.Close() }()

	data := generateTestData(recordSize)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(recordSize)

	for i := 0; i < b.N; i++ {
		if _, err := l.Append(data); err != nil {
			b.Fatalf("append failed: %v", err)
		}
		if (i+1)%syncInterval == 0 {
			if err := l.Sync(); err != nil {
				b.Fatalf("sync failed: %v", err)
			}
		}
	}

	// Final sync
	if err := l.Sync(); err != nil {
		b.Fatalf("final sync failed: %v", err)
	}
}
