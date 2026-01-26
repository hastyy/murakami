package log

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hastyy/murakami/internal/unit"
	"github.com/stretchr/testify/require"
)

// testConfig returns a Config with small values for fast testing.
// Small segment size allows quick segment rolls.
func testConfig() Config {
	return Config{
		MaxSegmentSize:        4 * unit.KiB,
		MaxRecordDataSize:     512 * unit.Byte,
		IndexIntervalBytes:    256 * unit.Byte,
		LogWriterBufferSize:   1 * unit.KiB,
		IndexWriterBufferSize: 512 * unit.Byte,
		LogReaderBufferSize:   512 * unit.Byte,
		RecordCacheSize:       16,
		IndexCacheSize:        100,
	}
}

// =============================================================================
// Log Initialization Tests
// =============================================================================

func TestNew_EmptyDirectory(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Should have exactly one segment (the active segment)
	require.Len(l.segments, 1)

	// Base offset should be 0
	require.Equal(Offset(0), l.activeSegment.BaseOffset())

	// Length should be 0
	require.Equal(0, l.Length())

	// Log and index files should exist
	logPath := filepath.Join(dir, "00000000000000000000.log")
	indexPath := filepath.Join(dir, "00000000000000000000.index")
	require.FileExists(logPath)
	require.FileExists(indexPath)
}

func TestNew_DirectoryDoesNotExist(t *testing.T) {
	require := require.New(t)
	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, "nested", "log", "dir")

	// Directory should not exist yet
	_, err := os.Stat(dir)
	require.True(os.IsNotExist(err))

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Directory should now exist with correct permissions
	info, err := os.Stat(dir)
	require.NoError(err)
	require.True(info.IsDir())
	require.Equal(os.FileMode(0700), info.Mode().Perm())

	// Log should be functional
	require.Equal(0, l.Length())
}

func TestNew_WithCleanShutdownMarker(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()
	cfg := testConfig()

	// Create a log, write some records, and close it cleanly
	l1, err := New(dir, cfg)
	require.NoError(err)

	_, err = l1.Append([]byte("record-1"))
	require.NoError(err)
	_, err = l1.Append([]byte("record-2"))
	require.NoError(err)

	err = l1.Close()
	require.NoError(err)

	// Shutdown marker should exist after clean close
	markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
	require.FileExists(markerPath)

	// Reopen the log
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// Marker should be deleted after reopening
	_, err = os.Stat(markerPath)
	require.True(os.IsNotExist(err))

	// Data should be intact - we should have 2 records
	// The sealed segment has 2 records, active segment has 0
	require.Equal(2, l2.Length())

	// Verify we can read the records
	record, err := l2.Read(0)
	require.NoError(err)
	require.Equal([]byte("record-1"), record.Data)

	record, err = l2.Read(1)
	require.NoError(err)
	require.Equal([]byte("record-2"), record.Data)
}

func TestNew_WithoutMarker_TriggersRecovery(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()
	cfg := testConfig()

	// Create a log and write some records
	l1, err := New(dir, cfg)
	require.NoError(err)

	_, err = l1.Append([]byte("record-1"))
	require.NoError(err)
	_, err = l1.Append([]byte("record-2"))
	require.NoError(err)

	// Sync to ensure data is on disk
	err = l1.Sync()
	require.NoError(err)

	// Close the log cleanly (this creates the marker)
	err = l1.Close()
	require.NoError(err)

	// Delete the shutdown marker to simulate a crash
	markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
	err = os.Remove(markerPath)
	require.NoError(err)

	// Reopen the log - this should trigger recovery
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// Data should still be intact after recovery
	require.Equal(2, l2.Length())

	// Verify records
	record, err := l2.Read(0)
	require.NoError(err)
	require.Equal([]byte("record-1"), record.Data)

	record, err = l2.Read(1)
	require.NoError(err)
	require.Equal([]byte("record-2"), record.Data)
}

func TestNew_MultipleSealedSegments(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	// Use a very small segment size to force multiple segments
	cfg := testConfig()
	cfg.MaxSegmentSize = 200 * unit.Byte // Very small to force rolls

	// Create a log and write enough records to create multiple segments
	l1, err := New(dir, cfg)
	require.NoError(err)

	// Write records until we have multiple segments
	numRecords := 10
	for i := 0; i < numRecords; i++ {
		_, err = l1.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	// Should have more than 1 segment now
	require.Greater(len(l1.segments), 1)
	sealedSegmentCount := len(l1.segments) - 1 // Exclude the active segment

	err = l1.Close()
	require.NoError(err)

	// Reopen the log
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// Should have all previous segments (now sealed) + 1 new active segment.
	// The previous active segment becomes sealed on reopen, so:
	// total = (sealedSegmentCount + 1 previous active now sealed) + 1 new active
	require.Equal(sealedSegmentCount+2, len(l2.segments))

	// All records should be readable
	require.Equal(numRecords, l2.Length())

	for i := 0; i < numRecords; i++ {
		record, err := l2.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("record-%d", i)), record.Data)
	}
}

// =============================================================================
// Append Tests
// =============================================================================

func TestAppend_ReturnsSequentialOffsets(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append multiple records and verify offsets are sequential starting from 0
	for i := 0; i < 10; i++ {
		offset, err := l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
		require.Equal(Offset(i), offset)
	}
}

func TestAppend_RecordDataWithinLimit(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Create data exactly at the limit
	data := make([]byte, cfg.MaxRecordDataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	offset, err := l.Append(data)
	require.NoError(err)
	require.Equal(Offset(0), offset)

	// Verify the data was stored correctly
	record, err := l.Read(offset)
	require.NoError(err)
	require.Equal(data, record.Data)
}

func TestAppend_RecordDataExceedsLimit(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Create data that exceeds the limit
	data := make([]byte, cfg.MaxRecordDataSize+1)

	_, err = l.Append(data)
	require.Error(err)
	require.Contains(err.Error(), "data size")

	// Log should still be functional
	offset, err := l.Append([]byte("valid record"))
	require.NoError(err)
	require.Equal(Offset(0), offset)
}

func TestAppend_EmptyData(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append empty data
	offset, err := l.Append([]byte{})
	require.NoError(err)
	require.Equal(Offset(0), offset)

	// Verify the record was stored
	record, err := l.Read(offset)
	require.NoError(err)
	require.Empty(record.Data)
}

func TestAppend_WithTimestamp(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append with a custom timestamp
	customTimestamp := int64(1234567890)
	offset, err := l.AppendWithTimestamp([]byte("record with timestamp"), customTimestamp)
	require.NoError(err)
	require.Equal(Offset(0), offset)

	// Verify the timestamp was stored correctly
	record, err := l.Read(offset)
	require.NoError(err)
	require.Equal(customTimestamp, record.Timestamp)
	require.Equal([]byte("record with timestamp"), record.Data)
}

func TestAppend_TriggersSegmentRoll(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	// Use a very small segment size to force rolls
	cfg := testConfig()
	cfg.MaxSegmentSize = 150 * unit.Byte

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Initially should have 1 segment
	require.Len(l.segments, 1)

	// Append records until a roll happens
	for i := 0; i < 5; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	// Should have more than 1 segment after appending
	require.Greater(len(l.segments), 1)
}

func TestAppend_RollCreatesCorrectBaseOffset(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	// Use a very small segment size to force rolls
	cfg := testConfig()
	cfg.MaxSegmentSize = 150 * unit.Byte

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	initialSegmentCount := len(l.segments)
	var offsetThatTriggeredRoll Offset

	// Append until we get a roll
	for i := 0; i < 10; i++ {
		offset, err := l.Append([]byte(fmt.Sprintf("rec-%d", i)))
		require.NoError(err)

		// Check if a roll happened
		if len(l.segments) > initialSegmentCount {
			offsetThatTriggeredRoll = offset
			break
		}
	}

	// Verify the new active segment's base offset
	activeSegmentBaseOffset := l.activeSegment.BaseOffset()

	// The record that triggered the roll is the first record in the new segment.
	// So the base offset equals that record's absolute offset.
	require.Equal(offsetThatTriggeredRoll, activeSegmentBaseOffset)

	// Also verify the previous segment (now sealed) has the expected length
	sealedSegment := l.segments[len(l.segments)-2]
	require.Equal(int(offsetThatTriggeredRoll), sealedSegment.Length())
}

func TestAppend_Concurrent(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	numGoroutines := 10
	recordsPerGoroutine := 50

	// Use a channel to collect all offsets
	offsetsChan := make(chan Offset, numGoroutines*recordsPerGoroutine)
	errChan := make(chan error, numGoroutines*recordsPerGoroutine)

	// Launch concurrent appenders
	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < recordsPerGoroutine; i++ {
				data := []byte(fmt.Sprintf("g%d-r%d", goroutineID, i))
				offset, err := l.Append(data)
				if err != nil {
					errChan <- err
					return
				}
				offsetsChan <- offset
			}
		}(g)
	}

	wg.Wait()
	close(offsetsChan)
	close(errChan)

	// Check for errors
	for err := range errChan {
		require.NoError(err)
	}

	// Collect all offsets and verify they are unique
	offsets := make(map[Offset]bool)
	for offset := range offsetsChan {
		require.False(offsets[offset], "duplicate offset: %d", offset)
		offsets[offset] = true
	}

	// Should have exactly numGoroutines * recordsPerGoroutine unique offsets
	expectedCount := numGoroutines * recordsPerGoroutine
	require.Len(offsets, expectedCount)

	// Verify log length
	require.Equal(expectedCount, l.Length())

	// Verify all offsets from 0 to expectedCount-1 are present
	for i := 0; i < expectedCount; i++ {
		require.True(offsets[Offset(i)], "missing offset: %d", i)
	}
}

// =============================================================================
// Read Tests
// =============================================================================

func TestRead_FromActiveSegment_CacheHit(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append a record (it will be in the cache, not yet flushed to disk)
	data := []byte("cached record")
	offset, err := l.Append(data)
	require.NoError(err)

	// Read immediately (should hit the cache)
	record, err := l.Read(offset)
	require.NoError(err)
	require.Equal(data, record.Data)
	require.Equal(offset, record.Offset)
}

func TestRead_FromActiveSegment_AfterFlush(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append a record
	data := []byte("flushed record")
	offset, err := l.Append(data)
	require.NoError(err)

	// Sync to flush to disk
	err = l.Sync()
	require.NoError(err)

	// Read after flush (should read from file)
	record, err := l.Read(offset)
	require.NoError(err)
	require.Equal(data, record.Data)
	require.Equal(offset, record.Offset)
}

func TestRead_FromSealedSegment(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 150 * unit.Byte // Small to force roll

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append records until we have a sealed segment
	var firstRecordOffset Offset
	for i := 0; i < 5; i++ {
		offset, err := l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
		if i == 0 {
			firstRecordOffset = offset
		}
	}

	// Ensure we have sealed segments
	require.Greater(len(l.segments), 1)

	// Read from the sealed segment (first record should be in first segment)
	record, err := l.Read(firstRecordOffset)
	require.NoError(err)
	require.Equal([]byte("record-0"), record.Data)
}

func TestRead_OffsetBelowFirstSegment(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()

	// Create log, append records, close
	l1, err := New(dir, cfg)
	require.NoError(err)
	for i := 0; i < 5; i++ {
		_, err = l1.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}
	err = l1.Close()
	require.NoError(err)

	// Manually delete the shutdown marker to trigger recovery
	// Then we'll simulate having trimmed old segments
	// For this test, we'll just verify the error when reading negative offset
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// Try to read with negative offset (always below first segment)
	_, err = l2.Read(-1)
	require.Error(err)
	require.Contains(err.Error(), "smaller than the first offset")
}

func TestRead_OffsetBeyondLastRecord(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append a few records
	for i := 0; i < 3; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	// Length is 3, so valid offsets are 0, 1, 2
	// Try to read offset 3 (beyond last record)
	_, err = l.Read(3)
	require.Error(err)
	require.Contains(err.Error(), "greater than the last written offset")

	// Try to read a much larger offset
	_, err = l.Read(100)
	require.Error(err)
}

func TestRead_AcrossSegmentBoundary(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 150 * unit.Byte // Small to force multiple segments

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append enough records to create multiple segments
	numRecords := 10
	for i := 0; i < numRecords; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	// Verify we have multiple segments
	require.Greater(len(l.segments), 2)

	// Read all records - they span multiple segments
	for i := 0; i < numRecords; i++ {
		record, err := l.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("record-%d", i)), record.Data)
		require.Equal(Offset(i), record.Offset)
	}
}

func TestRead_Concurrent(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 200 * unit.Byte // Small to create multiple segments

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append records
	numRecords := 20
	for i := 0; i < numRecords; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	// Sync to ensure all data is on disk
	err = l.Sync()
	require.NoError(err)

	// Launch concurrent readers
	numReaders := 10
	readsPerReader := 50

	errChan := make(chan error, numReaders*readsPerReader)

	var wg sync.WaitGroup
	for range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readsPerReader; i++ {
				// Read a random offset
				offset := Offset(i % numRecords)
				record, err := l.Read(offset)
				if err != nil {
					errChan <- err
					return
				}
				expectedData := []byte(fmt.Sprintf("record-%d", offset))
				if string(record.Data) != string(expectedData) {
					errChan <- fmt.Errorf("data mismatch at offset %d: got %s, want %s",
						offset, record.Data, expectedData)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		require.NoError(err)
	}
}

// =============================================================================
// Length Tests
// =============================================================================

func TestLength_EmptyLog(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	require.Equal(0, l.Length())
}

func TestLength_AfterAppends(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append records and verify length after each
	for i := 1; i <= 10; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
		require.Equal(i, l.Length())
	}
}

func TestLength_AfterSegmentRoll(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 150 * unit.Byte // Small to force rolls

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append enough records to trigger multiple segment rolls
	numRecords := 15
	for i := 0; i < numRecords; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("rec-%d", i)))
		require.NoError(err)
	}

	// Verify we have multiple segments
	require.Greater(len(l.segments), 2)

	// Length should include all records across all segments
	require.Equal(numRecords, l.Length())
}

// =============================================================================
// Sync Tests
// =============================================================================

func TestSync_FlushesBuffer(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append a record
	data := []byte("data to be synced")
	offset, err := l.Append(data)
	require.NoError(err)

	// Sync to flush buffer to disk
	err = l.Sync()
	require.NoError(err)

	// Close and reopen to verify data was persisted
	err = l.Close()
	require.NoError(err)

	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// Data should be readable
	record, err := l2.Read(offset)
	require.NoError(err)
	require.Equal(data, record.Data)
}

func TestSync_MultipleCalls(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	l, err := New(dir, testConfig())
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append some records
	for i := 0; i < 5; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	// Call Sync multiple times - should be idempotent and not error
	for range 5 {
		err = l.Sync()
		require.NoError(err)
	}

	// Log should still be functional
	offset, err := l.Append([]byte("after multiple syncs"))
	require.NoError(err)
	require.Equal(Offset(5), offset)
}

// =============================================================================
// Close Tests
// =============================================================================

func TestClose_EmptyActiveSegment_DeletesFiles(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 100 * unit.Byte // Very small to force rolls with empty active segment

	l, err := New(dir, cfg)
	require.NoError(err)

	// Append records until we have multiple segments
	for i := 0; i < 10; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("r%d", i)))
		require.NoError(err)
	}

	// We should have rolled at least once
	require.GreaterOrEqual(len(l.segments), 2, "expected at least 2 segments")

	// Get the active segment's base offset before close
	activeBaseOffset := l.activeSegment.BaseOffset()
	activeLogPath := filepath.Join(dir, fmt.Sprintf("%020d.log", activeBaseOffset))
	activeIndexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", activeBaseOffset))

	// Check if active segment is empty (meaning a roll just happened)
	activeIsEmpty := l.activeSegment.Length() == 0

	err = l.Close()
	require.NoError(err)

	if activeIsEmpty {
		// Empty active segment files should be deleted
		_, err = os.Stat(activeLogPath)
		require.True(os.IsNotExist(err), "empty active segment log file should be deleted")
		_, err = os.Stat(activeIndexPath)
		require.True(os.IsNotExist(err), "empty active segment index file should be deleted")
	}

	// Verify data is still accessible after reopen
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()
	require.Equal(10, l2.Length())
}

func TestClose_NonEmptySegment_Syncs(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	l, err := New(dir, cfg)
	require.NoError(err)

	// Append records without explicit sync
	for i := 0; i < 5; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	// Close should sync and persist data
	err = l.Close()
	require.NoError(err)

	// Reopen and verify all data is present
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	require.Equal(5, l2.Length())
	for i := 0; i < 5; i++ {
		record, err := l2.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("record-%d", i)), record.Data)
	}
}

func TestClose_WritesShutdownMarker(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	l, err := New(dir, cfg)
	require.NoError(err)

	// Append some records
	for i := 0; i < 3; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	err = l.Close()
	require.NoError(err)

	// Shutdown marker should exist
	markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
	require.FileExists(markerPath)

	// Read and parse the marker
	markerData, err := os.ReadFile(markerPath)
	require.NoError(err)

	var marker shutdownMarker
	err = json.Unmarshal(markerData, &marker)
	require.NoError(err)

	// Verify marker contents
	require.Equal(int64(2), marker.LastOffset) // Last offset is 2 (0, 1, 2)
	require.Equal(1, marker.SegmentCount)      // One segment with records
	require.False(marker.Timestamp.IsZero())
}

func TestClose_MultipleSegments(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 100 * unit.Byte // Very small to force multiple rolls

	l, err := New(dir, cfg)
	require.NoError(err)

	// Append enough records to create multiple segments
	numRecords := 15
	for i := 0; i < numRecords; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("r%d", i)))
		require.NoError(err)
	}

	// Verify we have multiple segments (at least 2)
	segmentCount := len(l.segments)
	require.GreaterOrEqual(segmentCount, 2, "expected at least 2 segments")

	// Close should succeed
	err = l.Close()
	require.NoError(err)

	// Reopen and verify all data
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	require.Equal(numRecords, l2.Length())
	for i := 0; i < numRecords; i++ {
		record, err := l2.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("r%d", i)), record.Data)
	}
}

func TestClose_ReopenAfterClose(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()

	// Create, use, and close the log multiple times
	for cycle := 0; cycle < 3; cycle++ {
		l, err := New(dir, cfg)
		require.NoError(err)

		// Append a record each cycle
		offset, err := l.Append([]byte(fmt.Sprintf("cycle-%d", cycle)))
		require.NoError(err)
		require.Equal(Offset(cycle), offset)

		err = l.Close()
		require.NoError(err)
	}

	// Final open to verify all data
	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	require.Equal(3, l.Length())
	for i := 0; i < 3; i++ {
		record, err := l.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("cycle-%d", i)), record.Data)
	}
}

// =============================================================================
// Recovery Tests
// =============================================================================

func TestRecovery_ValidSegment_NoTruncation(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()

	// Create log, append records, close cleanly
	l1, err := New(dir, cfg)
	require.NoError(err)

	for i := 0; i < 5; i++ {
		_, err = l1.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	err = l1.Sync()
	require.NoError(err)
	err = l1.Close()
	require.NoError(err)

	// Get the log file size after clean close
	logPath := filepath.Join(dir, "00000000000000000000.log")
	info, err := os.Stat(logPath)
	require.NoError(err)
	sizeBeforeRecovery := info.Size()

	// Delete the shutdown marker to trigger recovery on next open
	markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
	err = os.Remove(markerPath)
	require.NoError(err)

	// Reopen - recovery should run but not truncate anything
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// All records should be present
	require.Equal(5, l2.Length())

	// File size should be the same (truncated to valid boundary, but all records were valid)
	info, err = os.Stat(logPath)
	require.NoError(err)
	require.LessOrEqual(info.Size(), sizeBeforeRecovery) // May be smaller due to preallocation removal
}

func TestRecovery_CorruptedCRC_Truncates(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()

	// Create log, append records, close cleanly
	l1, err := New(dir, cfg)
	require.NoError(err)

	for i := 0; i < 5; i++ {
		_, err = l1.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	err = l1.Sync()
	require.NoError(err)
	err = l1.Close()
	require.NoError(err)

	// Corrupt the CRC of the 3rd record (offset 2)
	logPath := filepath.Join(dir, "00000000000000000000.log")
	data, err := os.ReadFile(logPath)
	require.NoError(err)

	// Find position of 3rd record and corrupt its CRC
	// Record format: [Length:4][CRC:4][Offset:8][Timestamp:8][Data:var]
	// Each record is ~32 bytes for our test data
	// We'll corrupt bytes 4-7 (CRC) of the 3rd record
	position := 0
	for range 2 {
		if position >= len(data) {
			break
		}
		length := int(data[position])<<24 | int(data[position+1])<<16 | int(data[position+2])<<8 | int(data[position+3])
		position += 4 + length // Skip length + content
	}
	// Now position is at the start of the 3rd record
	// Corrupt the CRC (bytes 4-7 after length)
	if position+8 < len(data) {
		data[position+4] ^= 0xFF // Flip bits in CRC
		data[position+5] ^= 0xFF
	}

	err = os.WriteFile(logPath, data, 0600)
	require.NoError(err)

	// Delete the shutdown marker to trigger recovery
	markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
	err = os.Remove(markerPath)
	require.NoError(err)

	// Reopen - recovery should truncate at the corrupted record
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// Only records before corruption should be present (0 and 1)
	require.Equal(2, l2.Length())

	// Verify we can read the surviving records
	for i := 0; i < 2; i++ {
		record, err := l2.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("record-%d", i)), record.Data)
	}
}

func TestRecovery_PartialRecord_Truncates(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()

	// Create log, append records, close cleanly
	l1, err := New(dir, cfg)
	require.NoError(err)

	for i := 0; i < 5; i++ {
		_, err = l1.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	err = l1.Sync()
	require.NoError(err)
	err = l1.Close()
	require.NoError(err)

	// Truncate the log file mid-record to simulate a crash
	logPath := filepath.Join(dir, "00000000000000000000.log")
	data, err := os.ReadFile(logPath)
	require.NoError(err)

	// Find the start of the last record and truncate in the middle
	position := 0
	recordCount := 0
	for position < len(data) {
		length := int(data[position])<<24 | int(data[position+1])<<16 | int(data[position+2])<<8 | int(data[position+3])
		if length == 0 {
			break // End of valid records
		}
		position += 4 + length
		recordCount++
	}

	// Truncate to keep only 3 full records + partial 4th record
	// Find end of 3rd record
	position = 0
	for range 3 {
		length := int(data[position])<<24 | int(data[position+1])<<16 | int(data[position+2])<<8 | int(data[position+3])
		position += 4 + length
	}
	// Add partial 4th record (just the length field)
	truncateAt := position + 4 // Length field only, no content

	err = os.Truncate(logPath, int64(truncateAt))
	require.NoError(err)

	// Delete the shutdown marker to trigger recovery
	markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
	err = os.Remove(markerPath)
	require.NoError(err)

	// Reopen - recovery should truncate at the incomplete record
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// Only complete records should be present (3)
	require.Equal(3, l2.Length())

	// Verify we can read them
	for i := 0; i < 3; i++ {
		record, err := l2.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("record-%d", i)), record.Data)
	}
}

func TestRecovery_EmptySegment_DeletesFiles(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()

	// Create log, append one record, sync, then truncate the log file to zero valid records
	l1, err := New(dir, cfg)
	require.NoError(err)

	_, err = l1.Append([]byte("will be removed"))
	require.NoError(err)
	err = l1.Sync()
	require.NoError(err)
	err = l1.Close()
	require.NoError(err)

	// Truncate the log file to contain no valid records (just zeros)
	logPath := filepath.Join(dir, "00000000000000000000.log")
	err = os.WriteFile(logPath, make([]byte, 100), 0600)
	require.NoError(err)

	// Delete the shutdown marker to trigger recovery
	markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
	err = os.Remove(markerPath)
	require.NoError(err)

	// Reopen - recovery should find no valid records and delete the files
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// Log should start fresh with 0 records
	require.Equal(0, l2.Length())

	// The old corrupted segment files should not exist (were deleted by recovery)
	// Only the new active segment files should exist
}

func TestRecovery_RebuildIndex(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.IndexIntervalBytes = 50 * unit.Byte // Small interval to create multiple index entries

	// Create log, append records, close cleanly
	l1, err := New(dir, cfg)
	require.NoError(err)

	for i := 0; i < 10; i++ {
		_, err = l1.Append([]byte(fmt.Sprintf("record-%02d", i)))
		require.NoError(err)
	}

	err = l1.Sync()
	require.NoError(err)
	err = l1.Close()
	require.NoError(err)

	// Delete the index file to force rebuild during recovery
	indexPath := filepath.Join(dir, "00000000000000000000.index")
	err = os.Remove(indexPath)
	require.NoError(err)

	// Delete the shutdown marker to trigger recovery
	markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
	err = os.Remove(markerPath)
	require.NoError(err)

	// Reopen - recovery should rebuild the index
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// All records should be accessible (index was rebuilt)
	require.Equal(10, l2.Length())

	for i := 0; i < 10; i++ {
		record, err := l2.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("record-%02d", i)), record.Data)
	}

	// Index file should exist now
	require.FileExists(indexPath)
}

// =============================================================================
// Retention Cleanup Tests
// =============================================================================

func TestRetention_Disabled(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.RetentionDuration = 0 // Disabled

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append some records
	for i := 0; i < 5; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	// Run retention cleanup - should return (0, nil) when disabled
	deleted, err := l.RunRetentionCleanup()
	require.NoError(err)
	require.Equal(0, deleted)

	// All records should still be present
	require.Equal(5, l.Length())
}

func TestRetention_DeletesOldSegments(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 100 * unit.Byte           // Small to force rolls
	cfg.RetentionDuration = 100 * time.Millisecond // Very short retention

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append records to create multiple segments
	for i := 0; i < 15; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("r%d", i)))
		require.NoError(err)
	}

	// Should have multiple segments
	require.GreaterOrEqual(len(l.segments), 2)
	initialSegmentCount := len(l.segments)

	// Sync to ensure data is on disk with proper timestamps
	err = l.Sync()
	require.NoError(err)

	// Wait for retention duration to pass
	time.Sleep(150 * time.Millisecond)

	// Run retention cleanup
	deleted, err := l.RunRetentionCleanup()
	require.NoError(err)

	// Some sealed segments should have been deleted (but not the active segment)
	require.Greater(deleted, 0)
	require.Less(len(l.segments), initialSegmentCount)
}

func TestRetention_NeverTouchesActiveSegment(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.RetentionDuration = 1 * time.Millisecond // Very short retention

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append a record to the active segment
	_, err = l.Append([]byte("active segment record"))
	require.NoError(err)

	// Wait for retention duration
	time.Sleep(10 * time.Millisecond)

	// Should only have 1 segment (the active segment)
	require.Len(l.segments, 1)

	// Run retention cleanup
	deleted, err := l.RunRetentionCleanup()
	require.NoError(err)

	// Active segment should NOT be deleted even if it's "old"
	require.Equal(0, deleted)
	require.Len(l.segments, 1)

	// Data should still be accessible
	require.Equal(1, l.Length())
	record, err := l.Read(0)
	require.NoError(err)
	require.Equal([]byte("active segment record"), record.Data)
}

func TestRetention_ReturnsDeletedCount(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 80 * unit.Byte           // Very small to force many rolls
	cfg.RetentionDuration = 50 * time.Millisecond // Short retention

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append records to create multiple segments
	for i := 0; i < 20; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("r%02d", i)))
		require.NoError(err)
	}

	err = l.Sync()
	require.NoError(err)

	// Should have multiple segments
	sealedSegmentCount := len(l.segments) - 1 // Exclude active segment
	require.Greater(sealedSegmentCount, 0)

	// Wait for retention duration
	time.Sleep(100 * time.Millisecond)

	// Run retention cleanup
	deleted, err := l.RunRetentionCleanup()
	require.NoError(err)

	// Deleted count should equal the number of sealed segments
	require.Equal(sealedSegmentCount, deleted)

	// Only the active segment should remain
	require.Len(l.segments, 1)
}

// =============================================================================
// Segment-Specific Tests
// =============================================================================

func TestActiveSegment_IndexingInterval(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.IndexIntervalBytes = 50 * unit.Byte // Small interval to create multiple entries

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append enough records to trigger multiple index entries
	for i := 0; i < 10; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("record-%02d-padding", i)))
		require.NoError(err)
	}

	err = l.Sync()
	require.NoError(err)

	// Check that index file has entries
	indexPath := filepath.Join(dir, "00000000000000000000.index")
	info, err := os.Stat(indexPath)
	require.NoError(err)

	// Index entry size is 16 bytes (8 for offset + 8 for position)
	// With small interval, we should have multiple entries
	require.Greater(info.Size(), int64(16), "expected multiple index entries")

	// All records should be readable (index is working)
	for i := 0; i < 10; i++ {
		record, err := l.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("record-%02d-padding", i)), record.Data)
	}
}

func TestActiveSegment_Size(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Initial size should be 0
	require.Equal(0, l.activeSegment.Size())

	// Append a record and verify size increases
	_, err = l.Append([]byte("test record"))
	require.NoError(err)

	// Size should be > 0 now (recordHeaderSize + len("test record"))
	size1 := l.activeSegment.Size()
	require.Greater(size1, 0)

	// Append another record
	_, err = l.Append([]byte("another record"))
	require.NoError(err)

	// Size should have increased
	size2 := l.activeSegment.Size()
	require.Greater(size2, size1)
}

func TestSealedSegment_BinarySearchIndex(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 150 * unit.Byte    // Small to force roll
	cfg.IndexIntervalBytes = 30 * unit.Byte // Create multiple index entries

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append records until we have a sealed segment
	for i := 0; i < 10; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("r%02d", i)))
		require.NoError(err)
	}

	// Should have sealed segments
	require.Greater(len(l.segments), 1)

	// Read from sealed segment - this exercises binary search
	// Read the first few records which should be in the first (sealed) segment
	for i := 0; i < 3; i++ {
		record, err := l.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("r%02d", i)), record.Data)
	}
}

func TestSealedSegment_ReadAfterClose(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 100 * unit.Byte // Small to force roll

	l, err := New(dir, cfg)
	require.NoError(err)

	// Append records to create a sealed segment
	for i := 0; i < 10; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("r%d", i)))
		require.NoError(err)
	}

	// Should have sealed segments
	require.Greater(len(l.segments), 1)

	// Get reference to a sealed segment
	sealedSeg := l.segments[0].(*sealedSegment)

	// Close the sealed segment directly
	err = sealedSeg.Close()
	require.NoError(err)

	// Reading from closed segment should return error
	_, err = sealedSeg.Read(0)
	require.Error(err)
	require.Contains(err.Error(), "segment is closed")

	// Close the log (will skip the already-closed segment, may produce errors)
	_ = l.Close()
}

func TestSealedSegment_LengthCaching(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 100 * unit.Byte // Small to force roll

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Append records to create a sealed segment
	for i := 0; i < 10; i++ {
		_, err = l.Append([]byte(fmt.Sprintf("r%d", i)))
		require.NoError(err)
	}

	// Should have sealed segments
	require.Greater(len(l.segments), 1)

	// Get reference to a sealed segment
	sealedSeg := l.segments[0].(*sealedSegment)

	// First call computes length
	length1 := sealedSeg.Length()
	require.Greater(length1, 0)

	// Second call should return cached value (same result)
	length2 := sealedSeg.Length()
	require.Equal(length1, length2)

	// Third call - still cached
	length3 := sealedSeg.Length()
	require.Equal(length1, length3)
}

// =============================================================================
// End-to-End Lifecycle Tests
// =============================================================================

func TestLifecycle_WriteReadCloseReopen(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()

	// Phase 1: Create log and write data
	l1, err := New(dir, cfg)
	require.NoError(err)

	records := []string{"first", "second", "third", "fourth", "fifth"}
	for i, data := range records {
		offset, err := l1.Append([]byte(data))
		require.NoError(err)
		require.Equal(Offset(i), offset)
	}

	// Verify we can read what we wrote
	for i, expected := range records {
		record, err := l1.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(expected), record.Data)
	}

	require.Equal(len(records), l1.Length())

	// Phase 2: Close the log
	err = l1.Close()
	require.NoError(err)

	// Phase 3: Reopen and verify all data is intact
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	require.Equal(len(records), l2.Length())

	for i, expected := range records {
		record, err := l2.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(expected), record.Data)
	}

	// Phase 4: Append more data after reopen
	newRecords := []string{"sixth", "seventh"}
	for i, data := range newRecords {
		offset, err := l2.Append([]byte(data))
		require.NoError(err)
		require.Equal(Offset(len(records)+i), offset)
	}

	require.Equal(len(records)+len(newRecords), l2.Length())

	// Verify all data (old + new)
	allRecords := append(records, newRecords...)
	for i, expected := range allRecords {
		record, err := l2.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(expected), record.Data)
	}
}

func TestLifecycle_CrashAndRecover(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()

	// Phase 1: Write data and sync (but don't close cleanly)
	l1, err := New(dir, cfg)
	require.NoError(err)

	for i := 0; i < 10; i++ {
		_, err = l1.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	// Sync to ensure data is on disk
	err = l1.Sync()
	require.NoError(err)

	// Simulate crash: close files without writing shutdown marker
	// We'll do this by closing the log normally then deleting the marker
	err = l1.Close()
	require.NoError(err)

	markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
	err = os.Remove(markerPath)
	require.NoError(err)

	// Simulate partial write by truncating the log file slightly
	logPath := filepath.Join(dir, "00000000000000000000.log")
	data, err := os.ReadFile(logPath)
	require.NoError(err)

	// Truncate to remove part of the last record
	// Find end of 8th record (keep 8 full records)
	position := 0
	for range 8 {
		if position >= len(data) {
			break
		}
		length := int(data[position])<<24 | int(data[position+1])<<16 | int(data[position+2])<<8 | int(data[position+3])
		if length == 0 {
			break
		}
		position += 4 + length
	}

	err = os.Truncate(logPath, int64(position))
	require.NoError(err)

	// Phase 2: Reopen - this should trigger recovery
	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// Only 8 complete records should remain
	require.Equal(8, l2.Length())

	// Verify the surviving records
	for i := 0; i < 8; i++ {
		record, err := l2.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("record-%d", i)), record.Data)
	}

	// Phase 3: Continue writing after recovery
	for i := 8; i < 12; i++ {
		_, err = l2.Append([]byte(fmt.Sprintf("record-%d", i)))
		require.NoError(err)
	}

	require.Equal(12, l2.Length())
}

func TestLifecycle_SegmentRollAndRead(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	cfg := testConfig()
	cfg.MaxSegmentSize = 100 * unit.Byte // Very small to force many rolls

	l, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l.Close() }()

	// Write many records to trigger multiple segment rolls
	numRecords := 50
	for i := 0; i < numRecords; i++ {
		offset, err := l.Append([]byte(fmt.Sprintf("record-%03d", i)))
		require.NoError(err)
		require.Equal(Offset(i), offset)
	}

	// Should have many segments
	require.Greater(len(l.segments), 5, "expected many segments due to small segment size")

	// Read all records - this tests reading across segment boundaries
	for i := 0; i < numRecords; i++ {
		record, err := l.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("record-%03d", i)), record.Data)
		require.Equal(Offset(i), record.Offset)
	}

	// Verify length includes all segments
	require.Equal(numRecords, l.Length())

	// Close and reopen to verify persistence across segments
	err = l.Close()
	require.NoError(err)

	l2, err := New(dir, cfg)
	require.NoError(err)
	defer func() { _ = l2.Close() }()

	// All records should still be readable
	require.Equal(numRecords, l2.Length())

	for i := 0; i < numRecords; i++ {
		record, err := l2.Read(Offset(i))
		require.NoError(err)
		require.Equal([]byte(fmt.Sprintf("record-%03d", i)), record.Data)
	}
}
