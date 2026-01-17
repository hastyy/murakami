package log_old

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/hastyy/murakami/internal/unit"
)

// Placeholder buffer for record marshaling - replace with a pool later
var buf = make([]byte, 4+4+8+4+16*unit.KiB)

type TestIndexEntry struct {
	RelativeOffset int64
	Position       int64
}

// Things to consider:
// - Reads should not block writes because we only append on write, so at most we can just readlock to get the current cache size and last known offset
// and then just use that to limit the search space.
// - Reads should not force the writer to flush; instead, we could keep a small records cache with the records in the buffer, and when it flushes
// (we need to wrap the bufio.Writer to detect this confidently) we can clear that cache.
type TestActiveSegment struct {
	baseOffset          int64
	nextOffset          int64
	bytePosition        int64
	lastIndexedPosition int64

	logFile   *os.File
	indexFile *os.File

	logReadFile *os.File
	indexCache  []TestIndexEntry

	logWriter   *bufio.Writer
	indexWriter *bufio.Writer

	mu sync.RWMutex
}

// Rename to MustNewActiveSegment due to panic on error? Or just expose the errors through return value?
func NewTestActiveSegment(dir string, baseOffset int64) *TestActiveSegment {
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		panic(err)
	}

	logFilePath := fmt.Sprintf("%s/%020d.log", dir, baseOffset)
	indexFilePath := fmt.Sprintf("%s/%020d.index", dir, baseOffset)

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}

	// TODO: pre-allocate the log file with unix.Fallocate

	indexFile, err := os.OpenFile(indexFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}

	logReadFile, err := os.OpenFile(logFilePath, os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}

	indexCache := make([]TestIndexEntry, 500_000)

	return &TestActiveSegment{
		baseOffset:  baseOffset,
		logFile:     logFile,
		indexFile:   indexFile,
		logWriter:   bufio.NewWriterSize(logFile, 64*unit.KiB),
		indexWriter: bufio.NewWriterSize(indexFile, 16*unit.KiB),
		logReadFile: logReadFile,
		indexCache:  indexCache,
	}
}

func (s *TestActiveSegment) Append(data []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := s.nextOffset
	position := s.bytePosition

	record := NewTestRecord(offset, data)
	//buf := make([]byte, 4+record.BinaryLength())	// TODO: get it from a pool?

	n, err := record.MarshalBinary(buf)
	if err != nil {
		return 0, err
	}

	_, err = s.logWriter.Write(buf[:n])
	if err != nil {
		return 0, err
	}

	s.bytePosition += int64(n)
	s.nextOffset++

	// turn into config
	if position-s.lastIndexedPosition >= 4*unit.KiB {
		entry := TestIndexEntry{
			RelativeOffset: offset,
			Position:       position,
		}
		s.indexCache = append(s.indexCache, entry)

		// We cast the int64s to uint32s because we know they'll be in range due to limits.
		// We're only using int64 to represent offset and position because that aligns with the stdlib types.
		err = binary.Write(s.indexWriter, binary.BigEndian, uint32(entry.RelativeOffset))
		if err != nil {
			return 0, err
		}
		err = binary.Write(s.indexWriter, binary.BigEndian, uint32(entry.Position))
		if err != nil {
			return 0, err
		}

		s.lastIndexedPosition = position
	}

	return offset, nil
}

func (s *TestActiveSegment) Read(offset int64, count int) ([]TestRecord, error) {
	// Snapshot the state limits under read lock. This gives us a consistent view
	// of what's "known good" at this point. Everything after this snapshot is
	// being appended concurrently, but everything before is immutable.
	s.mu.RLock()
	indexCacheLen := len(s.indexCache)
	lastOffset := s.nextOffset - 1 // nextOffset is the next one to be written
	// Create a slice that references the existing entries up to indexCacheLen
	// This is safe because we're only reading, and appends only add to the end.
	cacheSlice := s.indexCache[:indexCacheLen:indexCacheLen] // [start:end:cap] to prevent accidental growth
	s.mu.RUnlock()

	startPosition := s.findNearestOffsetFromCache(offset, cacheSlice)

	_, err := s.logReadFile.Seek(startPosition, io.SeekStart)
	if err != nil {
		return nil, err
	}

	records := make([]TestRecord, 0, count)
	for len(records) < count {
		record, err := TestUnmarshalBinary(s.logReadFile)
		if err != nil {
			return nil, err
		}

		// Limit results to the snapshot - don't include records being appended concurrently
		if record.Offset > lastOffset {
			break
		}

		if record.Offset < offset {
			continue
		}

		records = append(records, record)
	}

	return records, nil
}

func (s *TestActiveSegment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sync()
}

// should probably panic if any of these calls error?
func (s *TestActiveSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.sync()
	if err != nil {
		return err
	}

	err = s.logFile.Close()
	if err != nil {
		return err
	}

	err = s.indexFile.Close()
	if err != nil {
		return err
	}

	return nil
}

// should probably panic if any of these calls error?
func (s *TestActiveSegment) sync() error {
	err := s.logWriter.Flush()
	if err != nil {
		return err
	}

	err = s.indexWriter.Flush()
	if err != nil {
		return err
	}

	err = s.logFile.Sync()
	if err != nil {
		return err
	}

	err = s.indexFile.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (s *TestActiveSegment) findNearestOffsetFromCache(offset int64, cache []TestIndexEntry) int64 {
	if len(cache) == 0 {
		// Return zero value if cache is empty
		return 0
	}

	/*
		Cache: [{100 0} {200 1000} {300 2000} {400 3000} {500 4000}]

		Searching for offset 200:
		Index: 1, Found: true
		Exact match: {RelativeOffset:200 Position:1000}

		Searching for offset 250:
		Index: 2, Found: false
		Would insert between [1] and [2]
		Nearest before: {RelativeOffset:200 Position:1000}
		Nearest after: {RelativeOffset:300 Position:2000}

		Searching for offset 100:
		Index: 0, Found: true
		Exact match: {RelativeOffset:100 Position:0}

		Searching for offset 550:
		Index: 5, Found: false
		Would insert at end (target > all elements)

		Searching for offset 50:
		Index: 0, Found: false
		Would insert at start (target < all elements)
	*/
	idx, found := slices.BinarySearchFunc(cache, offset, func(x TestIndexEntry, y int64) int {
		return int(x.RelativeOffset - y)
	})

	// If found, return the exact match
	if found {
		return cache[idx].Position
	}

	// If not found, idx is the insertion point (where it would go)
	// We want the largest entry <= offset, which is at idx-1
	if idx == 0 {
		// Target is smaller than all entries, return first entry
		return cache[0].Position
	}

	// Return the entry just before the insertion point (largest <= offset)
	return cache[idx-1].Position
}
