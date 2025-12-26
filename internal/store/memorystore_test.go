package store

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hastyy/murakami/internal/protocol"
	"github.com/hastyy/murakami/internal/store/logtree"
	"github.com/stretchr/testify/require"
)

// newMockKeyGenerator creates a key generator that returns the provided keys in sequence
func newMockKeyGenerator(keys ...string) KeyGenerator {
	var idx atomic.Int32
	return TimestampKeyGenerator(func() logtree.Key {
		currentIdx := int(idx.Add(1)) - 1
		if currentIdx >= len(keys) {
			panic("mock key generator ran out of keys")
		}
		return logtree.Key(keys[currentIdx])
	})
}

func TestMemoryStore_CreateStream(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000")
	store := NewInMemoryStreamStore(keyGen)

	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	err = store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	protoErr, ok := protocol.IsProtocolError(err)
	require.True(ok)
	require.Equal(protocol.ErrCodeStreamExists, protoErr.Code)
}

func TestMemoryStore_CreateStream_Concurrent(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000", "3000")
	store := NewInMemoryStreamStore(keyGen)

	streamNames := []string{"test1", "test2", "test3"}
	var successCount atomic.Int32
	var failureCount atomic.Int32

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for _, streamName := range streamNames {
				err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: streamName})
				if err != nil {
					failureCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	require.Equal(int32(3), successCount.Load())
	require.Equal(int32(27), failureCount.Load())
}

func TestMemoryStore_AppendRecords_HappyPath_NoMillisID(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append first batch of records (key will be "1000" from generator)
	id, err := store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2"), []byte("record3")},
	})
	require.NoError(err)
	require.Equal("1000-2", id) // Last record ID

	// Append second batch (key will be "2000" from generator)
	id, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record4"), []byte("record5")},
	})
	require.NoError(err)
	require.Equal("2000-1", id)

	// Verify records in stream
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 100,
		},
	})
	require.NoError(err)
	require.Len(records, 5)

	// Verify IDs are monotonically increasing
	require.Equal("1000-0", records[0].ID)
	require.Equal("1000-1", records[1].ID)
	require.Equal("1000-2", records[2].ID)
	require.Equal("2000-0", records[3].ID)
	require.Equal("2000-1", records[4].ID)

	// Verify values
	require.Equal([]byte("record1"), records[0].Value)
	require.Equal([]byte("record2"), records[1].Value)
	require.Equal([]byte("record3"), records[2].Value)
	require.Equal([]byte("record4"), records[3].Value)
	require.Equal([]byte("record5"), records[4].Value)
}

func TestMemoryStore_AppendRecords_HappyPath_WithMillisID(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator() // Won't be called
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append with explicit MillisID
	id, err := store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Options: protocol.AppendCommandOptions{
			MillisID: "5000",
		},
		Records: [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)
	require.Equal("5000-1", id)

	// Append another batch with explicit MillisID
	id, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Options: protocol.AppendCommandOptions{
			MillisID: "6000",
		},
		Records: [][]byte{[]byte("record3")},
	})
	require.NoError(err)
	require.Equal("6000-0", id)

	// Verify records
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 100,
		},
	})
	require.NoError(err)
	require.Len(records, 3)
	require.Equal("5000-0", records[0].ID)
	require.Equal("5000-1", records[1].ID)
	require.Equal("6000-0", records[2].ID)
}

func TestMemoryStore_AppendRecords_StreamNotFound(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000")
	store := NewInMemoryStreamStore(keyGen)

	// Try to append to non-existent stream
	_, err := store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "nonexistent",
		Records:    [][]byte{[]byte("record1")},
	})

	protoErr, ok := protocol.IsProtocolError(err)
	require.True(ok)
	require.Equal(protocol.ErrCodeUnknownStream, protoErr.Code)
}

func TestMemoryStore_AppendRecords_NonMonotonicID(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator() // Won't be called since we provide explicit MillisID
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append with MillisID 5000
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Options: protocol.AppendCommandOptions{
			MillisID: "5000",
		},
		Records: [][]byte{[]byte("record1")},
	})
	require.NoError(err)

	// Try to append with MillisID 3000 (less than 5000)
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Options: protocol.AppendCommandOptions{
			MillisID: "3000",
		},
		Records: [][]byte{[]byte("record2")},
	})

	protoErr, ok := protocol.IsProtocolError(err)
	require.True(ok)
	require.Equal(protocol.ErrCodeNonMonotonicID, protoErr.Code)
}

func TestMemoryStore_AppendRecords_SameMillisID_IncrementsSequence(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator() // Won't be called
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append with MillisID 5000
	id, err := store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Options: protocol.AppendCommandOptions{
			MillisID: "5000",
		},
		Records: [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)
	require.Equal("5000-1", id)

	// Append again with same MillisID 5000 (should increment sequence)
	id, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Options: protocol.AppendCommandOptions{
			MillisID: "5000",
		},
		Records: [][]byte{[]byte("record3"), []byte("record4")},
	})
	require.NoError(err)
	require.Equal("5000-3", id)

	// Verify all records have correct sequence numbers
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 100,
		},
	})
	require.NoError(err)
	require.Len(records, 4)
	require.Equal("5000-0", records[0].ID)
	require.Equal("5000-1", records[1].ID)
	require.Equal("5000-2", records[2].ID)
	require.Equal("5000-3", records[3].ID)
}

func TestMemoryStore_AppendRecords_ClosesWatches(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Create watches
	watch1 := make(chan struct{})
	watch2 := make(chan struct{})
	watch3 := make(chan struct{})

	// Add watches to the stream (with proper locking)
	store.mu.RLock()
	stream := store.streams["test"]
	stream.mu.Lock()
	stream.watches = []chan struct{}{watch1, watch2, watch3}
	stream.mu.Unlock()
	store.mu.RUnlock()

	// Append records (should close all watches)
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1")},
	})
	require.NoError(err)

	// Verify all watches are closed
	_, ok := <-watch1
	require.False(ok, "watch1 should be closed")

	_, ok = <-watch2
	require.False(ok, "watch2 should be closed")

	_, ok = <-watch3
	require.False(ok, "watch3 should be closed")

	// Verify watches slice is cleared
	stream.mu.Lock()
	require.Nil(stream.watches)
	stream.mu.Unlock()
}

func TestMemoryStore_AppendRecords_ConcurrentSameStream(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000", "3000", "4000", "5000", "6000", "7000", "8000", "9000", "10000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Phase 1: Concurrent appends using key generator
	var wg sync.WaitGroup
	phase1Appends := 10
	recordsPerAppend := 3

	type appendResult struct {
		idx    int
		lastID string
		err    error
	}
	var phase1Results []appendResult
	var resultsMu sync.Mutex

	for i := 0; i < phase1Appends; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			lastID, err := store.AppendRecords(t.Context(), protocol.AppendCommand{
				StreamName: "test",
				Records:    [][]byte{[]byte("record"), []byte("record"), []byte("record")},
			})

			resultsMu.Lock()
			phase1Results = append(phase1Results, appendResult{idx: idx, lastID: lastID, err: err})
			resultsMu.Unlock()
		}(i)
	}

	wg.Wait()

	// Verify all phase 1 appends succeeded
	require.Len(phase1Results, phase1Appends)
	for _, result := range phase1Results {
		require.NoError(result.err, "phase 1 append %d failed", result.idx)
		require.NotEmpty(result.lastID, "phase 1 append %d returned empty ID", result.idx)
	}

	// Phase 2: Concurrent appends with the SAME explicit MillisID to test sequence incrementing
	phase2Appends := 5
	var phase2Results []appendResult

	for i := 0; i < phase2Appends; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			lastID, err := store.AppendRecords(t.Context(), protocol.AppendCommand{
				StreamName: "test",
				Options: protocol.AppendCommandOptions{
					MillisID: "20000", // All use same MillisID
				},
				Records: [][]byte{[]byte("seq-record"), []byte("seq-record"), []byte("seq-record")},
			})

			resultsMu.Lock()
			phase2Results = append(phase2Results, appendResult{idx: idx, lastID: lastID, err: err})
			resultsMu.Unlock()
		}(i)
	}

	wg.Wait()

	// Verify all phase 2 appends succeeded
	require.Len(phase2Results, phase2Appends)
	for _, result := range phase2Results {
		require.NoError(result.err, "phase 2 append %d failed", result.idx)
		require.NotEmpty(result.lastID, "phase 2 append %d returned empty ID", result.idx)
	}

	// Read all records from the stream to verify monotonicity
	store.mu.RLock()
	stream := store.streams["test"]
	stream.mu.RLock()
	allRecords := stream.log.Read("00000000000000000000", 0, 1000) // Read all records from beginning
	stream.mu.RUnlock()
	store.mu.RUnlock()

	// We expect: (10 appends * 3 records) + (5 appends * 3 records) = 45 records
	expectedTotal := (phase1Appends * recordsPerAppend) + (phase2Appends * recordsPerAppend)
	require.Len(allRecords, expectedTotal)

	// Verify IDs are strictly monotonic
	for i := 1; i < len(allRecords); i++ {
		prevParts := strings.Split(allRecords[i-1].ID, "-")
		currParts := strings.Split(allRecords[i].ID, "-")

		prevMs, _ := strconv.ParseInt(prevParts[0], 10, 64)
		currMs, _ := strconv.ParseInt(currParts[0], 10, 64)

		prevSeq, _ := strconv.Atoi(prevParts[1])
		currSeq, _ := strconv.Atoi(currParts[1])

		// IDs should be strictly monotonic
		if currMs > prevMs {
			// Different millisecond, OK (sequence resets)
			continue
		} else if currMs == prevMs {
			// Same millisecond, sequence must be strictly increasing
			require.Greater(currSeq, prevSeq, "non-monotonic sequence at index %d: %s -> %s", i, allRecords[i-1].ID, allRecords[i].ID)
		} else {
			// Current millisecond is less than previous - this should never happen
			require.Failf("non-monotonic millisecond at index %d: %s -> %s", "", i, allRecords[i-1].ID, allRecords[i].ID)
		}
	}

	// Verify we have multiple records with MillisID "20000" and incrementing sequences
	count20000 := 0
	maxSeq20000 := -1
	minSeq20000 := int(^uint(0) >> 1) // Max int
	for _, record := range allRecords {
		parts := strings.Split(record.ID, "-")
		if parts[0] == "20000" {
			count20000++
			seq, _ := strconv.Atoi(parts[1])
			if seq > maxSeq20000 {
				maxSeq20000 = seq
			}
			if seq < minSeq20000 {
				minSeq20000 = seq
			}
		}
	}
	// We should have 5 appends * 3 records = 15 records with MillisID "20000"
	require.Equal(phase2Appends*recordsPerAppend, count20000, "expected exactly %d records with MillisID 20000", phase2Appends*recordsPerAppend)
	require.Equal(0, minSeq20000, "expected minimum sequence to be 0")
	require.Equal(14, maxSeq20000, "expected maximum sequence to be 14 (0-14 inclusive for 15 records)")
}

func TestMemoryStore_AppendRecords_ConcurrentDifferentStreams(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000", "3000", "4000", "5000", "6000", "7000", "8000", "9000", "10000")
	store := NewInMemoryStreamStore(keyGen)

	// Create multiple streams
	streamNames := []string{"stream1", "stream2", "stream3"}
	for _, name := range streamNames {
		err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: name})
		require.NoError(err)
	}

	var wg sync.WaitGroup
	appendsPerStream := 3
	recordsPerAppend := 2

	type appendResult struct {
		streamName string
		lastID     string
		err        error
	}
	results := make([]appendResult, 0)
	var resultsMu sync.Mutex

	// Launch concurrent appends to different streams
	for _, streamName := range streamNames {
		for i := 0; i < appendsPerStream; i++ {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()

				lastID, err := store.AppendRecords(t.Context(), protocol.AppendCommand{
					StreamName: name,
					Records:    [][]byte{[]byte("record"), []byte("record")},
				})

				resultsMu.Lock()
				results = append(results, appendResult{streamName: name, lastID: lastID, err: err})
				resultsMu.Unlock()
			}(streamName)
		}
	}

	wg.Wait()

	// Verify all appends succeeded
	require.Len(results, len(streamNames)*appendsPerStream)
	for _, result := range results {
		require.NoError(result.err, "append to %s failed", result.streamName)
		require.NotEmpty(result.lastID, "append to %s returned empty ID", result.streamName)
	}

	// Verify each stream has the expected number of records
	for _, streamName := range streamNames {
		store.mu.RLock()
		stream := store.streams[streamName]
		stream.mu.RLock()
		totalRecords := stream.log.Length()
		stream.mu.RUnlock()
		store.mu.RUnlock()

		require.Equal(appendsPerStream*recordsPerAppend, totalRecords, "stream %s has wrong number of records", streamName)
	}
}

func TestMemoryStore_ReadRecords_ReadWholeStream(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000", "3000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append 6 records across 3 batches
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record3"), []byte("record4")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record5"), []byte("record6")},
	})
	require.NoError(err)

	// Read whole stream from beginning
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 100,
			Block: 0,
		},
	})
	require.NoError(err)
	require.Len(records, 6)

	// Verify IDs and order
	expectedIDs := []string{"1000-0", "1000-1", "2000-0", "2000-1", "3000-0", "3000-1"}
	for i, record := range records {
		require.Equal(expectedIDs[i], record.ID)
	}
}

func TestMemoryStore_ReadRecords_ReadFromGivenID(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000", "3000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append 6 records
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record3"), []byte("record4")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record5"), []byte("record6")},
	})
	require.NoError(err)

	// Read from "2000-0" forward
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "2000-0",
			Count: 100,
			Block: 0,
		},
	})
	require.NoError(err)
	require.Len(records, 4)

	// Verify IDs
	expectedIDs := []string{"2000-0", "2000-1", "3000-0", "3000-1"}
	for i, record := range records {
		require.Equal(expectedIDs[i], record.ID)
	}
}

func TestMemoryStore_ReadRecords_ReadWithCountLimit(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000", "3000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append 6 records
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record3"), []byte("record4")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record5"), []byte("record6")},
	})
	require.NoError(err)

	// Read from "1000-1" forward with COUNT = 3
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "1000-1",
			Count: 3,
			Block: 0,
		},
	})
	require.NoError(err)
	require.Len(records, 3)

	// Verify IDs
	expectedIDs := []string{"1000-1", "2000-0", "2000-1"}
	for i, record := range records {
		require.Equal(expectedIDs[i], record.ID)
	}
}

func TestMemoryStore_ReadRecords_BlockingRead_ReceivesFewerThanCount(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Start blocking read in a goroutine
	type readResult struct {
		records []protocol.Record
		err     error
	}
	resultChan := make(chan readResult, 1)

	go func() {
		// Read with COUNT = 3, BLOCK = 30s
		records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
			StreamName: "test",
			Options: protocol.ReadCommandOptions{
				MinID: "0-0",
				Count: 3,
				Block: 30 * time.Second,
			},
		})
		resultChan <- readResult{records: records, err: err}
	}()

	// Wait for the watch to be registered
	for {
		store.mu.RLock()
		stream := store.streams["test"]
		stream.mu.RLock()
		watchCount := len(stream.watches)
		stream.mu.RUnlock()
		store.mu.RUnlock()

		if watchCount > 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Append 2 records (fewer than COUNT = 3)
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	// Wait for the read to complete
	result := <-resultChan

	require.NoError(result.err)
	require.Len(result.records, 2)
	require.Equal("1000-0", result.records[0].ID)
	require.Equal("1000-1", result.records[1].ID)
}

func TestMemoryStore_ReadRecords_BlockingRead_CountLimitsResults(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Start blocking read in a goroutine
	type readResult struct {
		records []protocol.Record
		err     error
	}
	resultChan := make(chan readResult, 1)

	go func() {
		// Read with COUNT = 2, BLOCK = 30s
		records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
			StreamName: "test",
			Options: protocol.ReadCommandOptions{
				MinID: "0-0",
				Count: 2,
				Block: 30 * time.Second,
			},
		})
		resultChan <- readResult{records: records, err: err}
	}()

	// Wait for the watch to be registered
	for {
		store.mu.RLock()
		stream := store.streams["test"]
		stream.mu.RLock()
		watchCount := len(stream.watches)
		stream.mu.RUnlock()
		store.mu.RUnlock()

		if watchCount > 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Append 3 records (more than COUNT = 2)
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2"), []byte("record3")},
	})
	require.NoError(err)

	// Wait for the read to complete
	result := <-resultChan

	// Should receive only 2 records (capped by COUNT)
	require.NoError(result.err)
	require.Len(result.records, 2)
	require.Equal("1000-0", result.records[0].ID)
	require.Equal("1000-1", result.records[1].ID)
}

func TestMemoryStore_ReadRecords_BlockingRead_Timeout(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Read with BLOCK = 200ms, no records will be appended
	start := time.Now()
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 10,
			Block: 200 * time.Millisecond,
		},
	})
	duration := time.Since(start)

	require.NoError(err)
	require.Empty(records)

	// Should wait for at least the full block duration
	require.GreaterOrEqual(duration, 200*time.Millisecond)
}

func TestMemoryStore_ReadRecords_StreamNotFound(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Try to read from non-existent stream
	_, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "nonexistent",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 10,
			Block: 0,
		},
	})

	protoErr, ok := protocol.IsProtocolError(err)
	require.True(ok)
	require.Equal(protocol.ErrCodeUnknownStream, protoErr.Code)
}

func TestMemoryStore_ReadRecords_ContextCancellation(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(t.Context())

	// Start blocking read in a goroutine
	type readResult struct {
		records []protocol.Record
		err     error
	}
	resultChan := make(chan readResult, 1)

	go func() {
		records, err := store.ReadRecords(ctx, protocol.ReadCommand{
			StreamName: "test",
			Options: protocol.ReadCommandOptions{
				MinID: "0-0",
				Count: 10,
				Block: 30 * time.Second,
			},
		})
		resultChan <- readResult{records: records, err: err}
	}()

	// Wait for the watch to be registered
	for {
		store.mu.RLock()
		stream := store.streams["test"]
		stream.mu.RLock()
		watchCount := len(stream.watches)
		stream.mu.RUnlock()
		store.mu.RUnlock()

		if watchCount > 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Cancel the context after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait for the read to complete
	result := <-resultChan

	// Should return context.Canceled error
	require.Error(result.err)
	require.ErrorIs(result.err, context.Canceled)
	require.Nil(result.records)
}

func TestMemoryStore_ReadRecords_StreamDeletedWhileBlocking(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Start blocking read in a goroutine
	type readResult struct {
		records []protocol.Record
		err     error
	}
	resultChan := make(chan readResult, 1)

	go func() {
		records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
			StreamName: "test",
			Options: protocol.ReadCommandOptions{
				MinID: "0-0",
				Count: 10,
				Block: 30 * time.Second,
			},
		})
		resultChan <- readResult{records: records, err: err}
	}()

	// Wait for the watch to be registered
	for {
		store.mu.RLock()
		stream := store.streams["test"]
		stream.mu.RLock()
		watchCount := len(stream.watches)
		stream.mu.RUnlock()
		store.mu.RUnlock()

		if watchCount > 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Delete the stream
	err = store.DeleteStream(t.Context(), protocol.DeleteCommand{StreamName: "test"})
	require.NoError(err)

	// Wait for the read to complete
	result := <-resultChan

	// Should return empty records (stream was deleted)
	require.NoError(result.err)
	require.Empty(result.records)
}

func TestMemoryStore_ReadRecords_EmptyStreamNonBlocking(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Read from empty stream with BLOCK=0 (non-blocking)
	start := time.Now()
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 10,
			Block: 0,
		},
	})
	duration := time.Since(start)

	require.NoError(err)
	require.Empty(records)

	// Should return immediately (well under 100ms)
	require.Less(duration, 100*time.Millisecond)
}

func TestMemoryStore_ReadRecords_MultipleConcurrentReaders(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Start multiple blocking reads in goroutines
	type readResult struct {
		readerID int
		records  []protocol.Record
		err      error
	}
	resultChan := make(chan readResult, 3)
	numReaders := 3

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
				StreamName: "test",
				Options: protocol.ReadCommandOptions{
					MinID: "0-0",
					Count: 10,
					Block: 30 * time.Second,
				},
			})
			resultChan <- readResult{readerID: id, records: records, err: err}
		}(i)
	}

	// Wait for all watches to be registered
	for {
		store.mu.RLock()
		stream := store.streams["test"]
		stream.mu.RLock()
		watchCount := len(stream.watches)
		stream.mu.RUnlock()
		store.mu.RUnlock()

		if watchCount == numReaders {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Append records (should notify all readers)
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	// Collect all results
	results := make([]readResult, 0, numReaders)
	for i := 0; i < numReaders; i++ {
		results = append(results, <-resultChan)
	}

	// All readers should have received the records
	for _, result := range results {
		require.NoError(result.err, "reader %d failed", result.readerID)
		require.Len(result.records, 2, "reader %d didn't receive all records", result.readerID)
		require.Equal("1000-0", result.records[0].ID)
		require.Equal("1000-1", result.records[1].ID)
	}
}

func TestMemoryStore_ReadRecords_MinIDInclusive(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000", "3000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append 6 records
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record3"), []byte("record4")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record5"), []byte("record6")},
	})
	require.NoError(err)

	// Read from exact MIN_ID "2000-1" (should be inclusive)
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "2000-1",
			Count: 100,
			Block: 0,
		},
	})
	require.NoError(err)
	require.Len(records, 3)

	// Verify the first record is exactly "2000-1" (inclusive)
	require.Equal("2000-1", records[0].ID)
	require.Equal("3000-0", records[1].ID)
	require.Equal("3000-1", records[2].ID)
}

func TestMemoryStore_ReadRecords_AllRecordsBeforeMinID(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append records with IDs 1000-0, 1000-1, 2000-0, 2000-1
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record3"), []byte("record4")},
	})
	require.NoError(err)

	// Read from MIN_ID "5000-0" (all records are before this)
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "5000-0",
			Count: 100,
			Block: 0,
		},
	})
	require.NoError(err)
	require.Empty(records)
}

func TestMemoryStore_ReadRecords_AllRecordsBeforeMinID_Blocking(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append records with IDs 1000-0, 1000-1
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	// Start blocking read with MIN_ID after all existing records
	type readResult struct {
		records []protocol.Record
		err     error
	}
	resultChan := make(chan readResult, 1)

	go func() {
		records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
			StreamName: "test",
			Options: protocol.ReadCommandOptions{
				MinID: "1500-0",
				Count: 10,
				Block: 30 * time.Second,
			},
		})
		resultChan <- readResult{records: records, err: err}
	}()

	// Wait for the watch to be registered
	for {
		store.mu.RLock()
		stream := store.streams["test"]
		stream.mu.RLock()
		watchCount := len(stream.watches)
		stream.mu.RUnlock()
		store.mu.RUnlock()

		if watchCount > 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Append new records that meet the MIN_ID criteria
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record3"), []byte("record4")},
	})
	require.NoError(err)

	// Wait for the read to complete
	result := <-resultChan

	require.NoError(result.err)
	require.Len(result.records, 2)
	require.Equal("2000-0", result.records[0].ID)
	require.Equal("2000-1", result.records[1].ID)
}

func TestMemoryStore_TrimStream_Success(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000", "3000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append 6 records
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record3"), []byte("record4")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record5"), []byte("record6")},
	})
	require.NoError(err)

	// Trim before "2000-1" (should remove records before 2000-1)
	err = store.TrimStream(t.Context(), protocol.TrimCommand{
		StreamName: "test",
		Options: protocol.TrimCommandOptions{
			MinID: "2000-1",
		},
	})
	require.NoError(err)

	// Read all remaining records
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 100,
			Block: 0,
		},
	})
	require.NoError(err)
	require.Len(records, 3)
	require.Equal("2000-1", records[0].ID)
	require.Equal("3000-0", records[1].ID)
	require.Equal("3000-1", records[2].ID)
}

func TestMemoryStore_TrimStream_RemovesAllRecords(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000", "2000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append records
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record3"), []byte("record4")},
	})
	require.NoError(err)

	// Trim with MIN_ID after all records
	err = store.TrimStream(t.Context(), protocol.TrimCommand{
		StreamName: "test",
		Options: protocol.TrimCommandOptions{
			MinID: "5000-0",
		},
	})
	require.NoError(err)

	// Read all remaining records (should be empty)
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 100,
			Block: 0,
		},
	})
	require.NoError(err)
	require.Empty(records)
}

func TestMemoryStore_TrimStream_MinIDBeforeAllRecords(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("2000", "3000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append records
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record3"), []byte("record4")},
	})
	require.NoError(err)

	// Trim with MIN_ID before all records (no-op)
	err = store.TrimStream(t.Context(), protocol.TrimCommand{
		StreamName: "test",
		Options: protocol.TrimCommandOptions{
			MinID: "1000-0",
		},
	})
	require.NoError(err)

	// All records should still be there
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 100,
			Block: 0,
		},
	})
	require.NoError(err)
	require.Len(records, 4)
}

func TestMemoryStore_TrimStream_ExactBoundary(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append 5 records with same MillisID (will have sequences 0-4)
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("r1"), []byte("r2"), []byte("r3"), []byte("r4"), []byte("r5")},
	})
	require.NoError(err)

	// Trim at exact boundary "1000-2" (should keep 1000-2, 1000-3, 1000-4)
	err = store.TrimStream(t.Context(), protocol.TrimCommand{
		StreamName: "test",
		Options: protocol.TrimCommandOptions{
			MinID: "1000-2",
		},
	})
	require.NoError(err)

	// Read remaining records
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 100,
			Block: 0,
		},
	})
	require.NoError(err)
	require.Len(records, 3)
	require.Equal("1000-2", records[0].ID)
	require.Equal("1000-3", records[1].ID)
	require.Equal("1000-4", records[2].ID)
}

func TestMemoryStore_TrimStream_StreamNotFound(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Try to trim non-existent stream
	err := store.TrimStream(t.Context(), protocol.TrimCommand{
		StreamName: "nonexistent",
		Options: protocol.TrimCommandOptions{
			MinID: "1000-0",
		},
	})

	protoErr, ok := protocol.IsProtocolError(err)
	require.True(ok)
	require.Equal(protocol.ErrCodeUnknownStream, protoErr.Code)
}

func TestMemoryStore_TrimStream_EmptyStream(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Create empty stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Trim empty stream (should succeed, no-op)
	err = store.TrimStream(t.Context(), protocol.TrimCommand{
		StreamName: "test",
		Options: protocol.TrimCommandOptions{
			MinID: "1000-0",
		},
	})
	require.NoError(err)

	// Stream should still exist and be empty
	records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 100,
			Block: 0,
		},
	})
	require.NoError(err)
	require.Empty(records)
}

func TestMemoryStore_DeleteStream_Success(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator("1000")
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Append some records
	_, err = store.AppendRecords(t.Context(), protocol.AppendCommand{
		StreamName: "test",
		Records:    [][]byte{[]byte("record1"), []byte("record2")},
	})
	require.NoError(err)

	// Delete stream
	err = store.DeleteStream(t.Context(), protocol.DeleteCommand{StreamName: "test"})
	require.NoError(err)

	// Try to read from deleted stream (should fail)
	_, err = store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 10,
			Block: 0,
		},
	})

	protoErr, ok := protocol.IsProtocolError(err)
	require.True(ok)
	require.Equal(protocol.ErrCodeUnknownStream, protoErr.Code)
}

func TestMemoryStore_DeleteStream_StreamNotFound(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Try to delete non-existent stream
	err := store.DeleteStream(t.Context(), protocol.DeleteCommand{StreamName: "nonexistent"})

	protoErr, ok := protocol.IsProtocolError(err)
	require.True(ok)
	require.Equal(protocol.ErrCodeUnknownStream, protoErr.Code)
}

func TestMemoryStore_DeleteStream_ClosesWatches(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Create watches manually
	watch1 := make(chan struct{})
	watch2 := make(chan struct{})
	watch3 := make(chan struct{})

	// Add watches to the stream
	store.mu.RLock()
	stream := store.streams["test"]
	stream.mu.Lock()
	stream.watches = []chan struct{}{watch1, watch2, watch3}
	stream.mu.Unlock()
	store.mu.RUnlock()

	// Delete stream (should close all watches)
	err = store.DeleteStream(t.Context(), protocol.DeleteCommand{StreamName: "test"})
	require.NoError(err)

	// Verify all watches are closed
	_, ok := <-watch1
	require.False(ok, "watch1 should be closed")

	_, ok = <-watch2
	require.False(ok, "watch2 should be closed")

	_, ok = <-watch3
	require.False(ok, "watch3 should be closed")
}

func TestMemoryStore_DeleteStream_WithBlockingReaders(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Start blocking reads in goroutines
	type readResult struct {
		records []protocol.Record
		err     error
	}
	resultChan := make(chan readResult, 2)

	for i := 0; i < 2; i++ {
		go func() {
			records, err := store.ReadRecords(t.Context(), protocol.ReadCommand{
				StreamName: "test",
				Options: protocol.ReadCommandOptions{
					MinID: "0-0",
					Count: 10,
					Block: 30 * time.Second,
				},
			})
			resultChan <- readResult{records: records, err: err}
		}()
	}

	// Wait for watches to be registered
	for {
		store.mu.RLock()
		stream, exists := store.streams["test"]
		if !exists {
			store.mu.RUnlock()
			break
		}
		stream.mu.RLock()
		watchCount := len(stream.watches)
		stream.mu.RUnlock()
		store.mu.RUnlock()

		if watchCount == 2 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Delete the stream
	err = store.DeleteStream(t.Context(), protocol.DeleteCommand{StreamName: "test"})
	require.NoError(err)

	// Both readers should return with empty records
	for i := 0; i < 2; i++ {
		result := <-resultChan
		require.NoError(result.err)
		require.Empty(result.records)
	}
}

func TestMemoryStore_DeleteStream_EmptyStream(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Create empty stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Delete empty stream
	err = store.DeleteStream(t.Context(), protocol.DeleteCommand{StreamName: "test"})
	require.NoError(err)

	// Verify stream is gone
	_, err = store.ReadRecords(t.Context(), protocol.ReadCommand{
		StreamName: "test",
		Options: protocol.ReadCommandOptions{
			MinID: "0-0",
			Count: 10,
			Block: 0,
		},
	})

	protoErr, ok := protocol.IsProtocolError(err)
	require.True(ok)
	require.Equal(protocol.ErrCodeUnknownStream, protoErr.Code)
}

func TestMemoryStore_DeleteStream_ConcurrentDeletes(t *testing.T) {
	require := require.New(t)

	keyGen := newMockKeyGenerator()
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(t.Context(), protocol.CreateCommand{StreamName: "test"})
	require.NoError(err)

	// Try to delete the same stream concurrently
	var wg sync.WaitGroup
	deleteCount := 5
	var successCount atomic.Int32
	var failureCount atomic.Int32

	for range deleteCount {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := store.DeleteStream(t.Context(), protocol.DeleteCommand{StreamName: "test"})
			if err != nil {
				failureCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}()
	}

	wg.Wait()

	// Exactly one delete should succeed
	require.Equal(int32(1), successCount.Load())
	require.Equal(int32(4), failureCount.Load())
}

// Regression test: Ensures that reading after trim correctly handles logical sequence numbers
// that don't match physical array indices in the LogTree.
func TestMemoryStore_ReadAfterTrim_LogicalSequenceNumbers(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	keyGen := newMockKeyGenerator() // Won't be called since we provide explicit MillisID
	store := NewInMemoryStreamStore(keyGen)

	// Create stream
	err := store.CreateStream(ctx, protocol.CreateCommand{StreamName: "test-stream"})
	require.NoError(err)

	// Append 5 records with the same key "1000"
	// These will have IDs: 1000-0, 1000-1, 1000-2, 1000-3, 1000-4
	appendCmd := protocol.AppendCommand{
		StreamName: "test-stream",
		Records:    [][]byte{[]byte("record0"), []byte("record1"), []byte("record2"), []byte("record3"), []byte("record4")},
		Options: protocol.AppendCommandOptions{
			MillisID: "1000",
		},
	}
	lastID, err := store.AppendRecords(ctx, appendCmd)
	require.NoError(err)
	require.Equal("1000-4", lastID)

	// Trim to remove records before "1000-2" (removes 1000-0 and 1000-1)
	trimCmd := protocol.TrimCommand{
		StreamName: "test-stream",
		Options: protocol.TrimCommandOptions{
			MinID: "1000-2",
		},
	}
	err = store.TrimStream(ctx, trimCmd)
	require.NoError(err)

	// Now read starting from "1000-2" with a large limit
	// Should get 3 records (1000-2, 1000-3, 1000-4) because sequence numbers are logical
	readCmd := protocol.ReadCommand{
		StreamName: "test-stream",
		Options: protocol.ReadCommandOptions{
			MinID: "1000-2",
			Count: 10,
			Block: 0,
		},
	}
	records, err := store.ReadRecords(ctx, readCmd)
	require.NoError(err)

	// Verify we get all 3 remaining records with correct logical sequence numbers
	require.Equal(3, len(records), "Expected 3 records (1000-2, 1000-3, 1000-4) but got %d", len(records))
	require.Equal("1000-2", records[0].ID)
	require.Equal("1000-3", records[1].ID)
	require.Equal("1000-4", records[2].ID)
}
