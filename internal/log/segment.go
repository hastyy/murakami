package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"syscall"
)

const (
	// Permissions for the log directory and files within it.
	directoryPermissions = 0700 // drwx------ (owner: read+write+execute, others: nothing)
	filePermissions      = 0600 // -rw------- (owner: read+write, others: nothing)
)

// readSegment is a read-only representation of a segment of the log.
type readSegment interface {
	// Reads a record from the segment at the given offset.
	Read(Offset) (Record, error)

	// Returns the base offset of the segment.
	BaseOffset() Offset

	// Returns the number of records in the segment.
	Length() int

	// Closes the segment.
	Close() error
}

// activeSegment is a read/write representation of the latest segment of the log.
// A log will only contain one active segment at a time, which is always the last one in the segments slice.
// Each time we (re)start the log, a new active segment is created and all other segments that might already exist
// are considered sealed even if they haven't been explicitly closed (e.g. after a crash).
//
// Each offset and length is relative to the current segment and not the whole log.
type activeSegment struct {
	cfg        Config // read-only
	baseOffset int64  // read-only

	// <start of state that needs to be synchronized>
	nextOffset          int64 // protected by mu
	bytePosition        int64 // protected by mu
	lastIndexedPosition int64 // protected by mu
	flushedPosition     int64 // protected by mu; byte position up to which data is on disk

	logFile   *os.File // protected by mu
	indexFile *os.File // protected by mu

	logWriter   *bufio.Writer // protected by mu
	indexWriter *bufio.Writer // protected by mu

	logMmap              []byte // mmap'd view of the preallocated log file for fast reads
	recordEncodingBuffer []byte // protected by mu
	indexEncodingBuffer  []byte // protected by mu

	recordCache *recordCache // built-in concurrency control
	indexCache  indexCache   // protected by mu
	// <end of state that needs to be synchronized>

	mu sync.RWMutex
}

func newActiveSegment(logDir string, baseOffset Offset, cfg Config) (*activeSegment, error) {
	// Make sure the log directory exists
	err := os.MkdirAll(logDir, directoryPermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	// Get the absolute file paths for the log and index files.
	logFilePath := logFilePath(logDir, baseOffset)
	indexFilePath := indexFilePath(logDir, baseOffset)

	// Open the log file.
	// Note: We don't use O_APPEND because after preallocation (Truncate), O_APPEND would
	// write at the end of the preallocated space instead of position 0.
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	// Open the index file.
	indexFile, err := os.OpenFile(indexFilePath, os.O_CREATE|os.O_WRONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	// Try to preallocate disk space for the log file.
	err = preallocateDiskSpace(logFile, int64(cfg.MaxSegmentSize))
	if err != nil {
		return nil, fmt.Errorf("failed to preallocate log file: %w", err)
	}

	// Open a read-only file descriptor for mmap.
	logReadFile, err := os.OpenFile(logFilePath, os.O_RDONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log read file: %w", err)
	}

	// mmap the preallocated log file for fast reads.
	// The file is already preallocated to MaxSegmentSize, so we map the entire region.
	logMmap, err := syscall.Mmap(
		int(logReadFile.Fd()),
		0,
		cfg.MaxSegmentSize,
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		_ = logReadFile.Close()
		return nil, fmt.Errorf("failed to mmap log file: %w", err)
	}

	// Close the read file descriptor - the mmap keeps the mapping alive.
	if err := logReadFile.Close(); err != nil {
		_ = syscall.Munmap(logMmap)
		return nil, fmt.Errorf("failed to close log read file after mmap: %w", err)
	}

	// Create the record cache to hold a copy of the records still in the buffered writer's internal buffer.
	recordCache := newRecordCache(cfg.RecordCacheSize)

	// Create the segment struct first so we can reference it in the callback.
	seg := &activeSegment{
		cfg:        cfg,
		baseOffset: baseOffset,
		logFile:    logFile,
		indexFile:  indexFile,
		logMmap:    logMmap,
		// logWriter and indexWriter are set below
		recordEncodingBuffer: make([]byte, recordHeaderSize+cfg.MaxRecordDataSize),
		indexEncodingBuffer:  make([]byte, indexEntrySize),
		recordCache:          recordCache,
		indexCache:           newIndexCache(cfg.IndexCacheSize),
	}

	// flushAwareWriter wraps the logFile to detect when its .Write() method is called
	// (meaning the buffered writer has flushed). On flush, we:
	// 1. Clear the record cache (flushed records are now in mmap)
	// 2. Advance flushedPosition so reads can use mmap
	flushAwareWriter := newFlushAwareWriter(logFile, func(bytesWritten int) {
		// This is called while holding mu (from Append's flush or Sync),
		// so we can safely update flushedPosition without additional locking.
		seg.flushedPosition += int64(bytesWritten)
		seg.recordCache.Clear()
	})

	// Create the buffered writers for the log and index files.
	seg.logWriter = bufio.NewWriterSize(flushAwareWriter, cfg.LogWriterBufferSize)
	seg.indexWriter = bufio.NewWriterSize(indexFile, cfg.IndexWriterBufferSize)

	return seg, nil
}

// Append appends a record to the active segment.
// It returns the offset of the record and any error that occurred.
// When Append successfully returns it just means the record was accepted, not that it was made durable yet.
// To guarantee durability, the caller must call Sync() after Append().
func (s *activeSegment) Append(data []byte, unixTimestamp int64) (Offset, error) {
	// Each call to Append() must be fully serialized.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the offset and initial byte position for the new record.
	offset := s.nextOffset
	position := s.bytePosition

	// Create the record and cache it since it'll land in the logWriter's internal buffer first.
	r := newRecord(offset, unixTimestamp, data)
	s.recordCache.Add(r, position)

	// Encode the record into the buffer.
	n, err := encodeRecord(r, s.recordEncodingBuffer)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}
	recordBytes := s.recordEncodingBuffer[:n]

	// Write the record to the logWriter.
	// If the whole record doesn't fit in the buffer, flush it first to avoid writing
	// part of the record in the .log file and another part in the buffer.
	// It's best to have each record fully on one side and avoid fragmentation.
	// This also guarantees atomicity to each append operation.
	if s.logWriter.Available() < len(recordBytes) {
		err := s.logWriter.Flush()
		if err != nil {
			return 0, fmt.Errorf("failed to flush log writer: %w", err)
		}
	}
	_, err = s.logWriter.Write(recordBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to write record: %w", err)
	}

	// Advance byte position and offset.
	s.bytePosition += int64(n)
	s.nextOffset++

	if s.shouldIndex(position) {
		// Create the index entry and cache it since all index looks in the active segment
		// are done in through the in-memory index instead of reading the .index file.
		indexEntry := newIndexEntry(offset, position)
		s.indexCache = append(s.indexCache, indexEntry)

		// Encode the index entry into the buffer.
		_, err = encodeIndexEntry(indexEntry, s.indexEncodingBuffer)
		if err != nil {
			return 0, fmt.Errorf("failed to encode index entry: %w", err)
		}

		// Write the entry to the index buffered writer.
		// We don't expect the buffer to ever flush during normal operation, so we don't check if it will.
		_, err = s.indexWriter.Write(s.indexEncodingBuffer) // no need to slice because the buffer already has the correct size
		if err != nil {
			return 0, fmt.Errorf("failed to write index entry: %w", err)
		}

		// Advance the last indexed position.
		s.lastIndexedPosition = position
	}

	return offset, nil
}

// Read reads a record from the active segment at the given offset.
// It returns the record and any error that occurred.
// It first tries to locate the record in the record cache (unflushed data).
// If not found, it reads from the mmap'd region (flushed data).
func (s *activeSegment) Read(offset Offset) (Record, error) {
	// Snapshot the state limits under read lock. This gives us a consistent view
	// of what's "known" at this point. Everything after this snapshot is
	// being appended concurrently, but everything before is immutable.
	s.mu.RLock()
	lastWrittenOffset := s.nextOffset - 1
	indexCacheLen := len(s.indexCache)
	s.mu.RUnlock()

	// Check if the offset is greater than the last written offset.
	// If it is, return an error.
	// Otherwise we know we must have the record matching this offset, because:
	// 1. the log called us, meaning if any segment has it is us, and
	// 2. the offset is within written range (offset <= lastWrittenOffset).
	if offset > lastWrittenOffset {
		return Record{}, fmt.Errorf("offset %d is greater than the last written offset %d", offset, lastWrittenOffset)
	}

	// Try to read from the record cache (unflushed records still in write buffer).
	recordEntry, found := s.recordCache.Get(offset)
	if found {
		return recordEntry.record, nil
	}

	// Record is not in cache - it must be flushed to disk.
	// Re-read flushedPosition to get the most up-to-date value.
	// This is necessary because a flush could have occurred between our initial
	// snapshot and the cache lookup, which would have cleared the cache and
	// advanced flushedPosition.
	s.mu.RLock()
	flushedPos := s.flushedPosition
	s.mu.RUnlock()

	// Use the index cache to find the starting position.
	indexCacheSnapshot := s.indexCache[:indexCacheLen]
	indexEntry, _ := indexCacheSnapshot.Find(offset)

	return s.readRecordFromMmap(offset, indexEntry.Position, flushedPos)
}

// BaseOffset returns the base offset of the segment.
func (s *activeSegment) BaseOffset() Offset {
	// No need to lock here because baseOffset is read-only.
	return s.baseOffset
}

// Length returns the number of records in the segment.
// Length is relative to the current segment and not the whole log.
func (s *activeSegment) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return int(s.nextOffset)
}

// Size returns the size of the segment in bytes.
func (s *activeSegment) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return int(s.bytePosition)
}

// Sync flushes and syncs the active segment to disk.
func (s *activeSegment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sync()
}

// Close closes the active segment by flushing and syncing the records to disk and closing the files.
func (s *activeSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error

	if err := s.sync(); err != nil {
		errs = append(errs, err)
	}

	// Unmap the log mmap.
	if s.logMmap != nil {
		if err := syscall.Munmap(s.logMmap); err != nil {
			errs = append(errs, fmt.Errorf("failed to munmap log: %w", err))
		}
		s.logMmap = nil
	}

	if err := s.logFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close log file: %w", err))
	}

	if err := s.indexFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close index file: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// shouldIndex checks if we should index the current record based on the index interval.
// If the position is 0, we index the first record.
// We then continue adding index entries at the byte interval specified by the config.
func (s *activeSegment) shouldIndex(position int64) bool {
	return position == 0 || (position-s.lastIndexedPosition >= int64(s.cfg.IndexIntervalBytes))
}

// readRecordFromMmap reads a record from the mmap'd region at the given offset.
// It receives a start position which might match the start of the target record
// or some other record before it.
// Pre-condition: offset corresponds to a flushed record (not in record cache).
func (s *activeSegment) readRecordFromMmap(offset Offset, startPosition int64, flushedPosition int64) (Record, error) {
	position := startPosition

	for position < flushedPosition {
		// Read the record length.
		recordLength := int64(binary.BigEndian.Uint32(s.logMmap[position : position+recordLengthSize]))

		// Reading length 0 means we've reached the end of valid records.
		if recordLength == 0 {
			break
		}
		position += recordLengthSize

		// Read the record CRC.
		recordCRC := binary.BigEndian.Uint32(s.logMmap[position : position+recordCRCSize])
		position += recordCRCSize

		// Read the record offset.
		recordOffset := int64(binary.BigEndian.Uint64(s.logMmap[position : position+recordOffsetSize]))
		position += recordOffsetSize

		// This should never really happen.
		if recordOffset > offset {
			return Record{}, fmt.Errorf("unexpected case: found record offset %d greater than the target offset %d while doing a sequential read", recordOffset, offset)
		}

		// Skip the remaining record bytes if this is not the target record (timestamp + data).
		if recordOffset < offset {
			position += recordLength - recordCRCSize - recordOffsetSize
			continue
		}

		// We found the target record. Now read the remaining fields.
		recordTimestamp := int64(binary.BigEndian.Uint64(s.logMmap[position : position+recordTimestampSize]))
		position += recordTimestampSize

		dataSize := recordDataSize(recordLength)

		// Verify CRC over the binary fields it covers (offset, timestamp, data).
		crcStart := position - recordOffsetSize - recordTimestampSize
		crcEnd := position + dataSize
		crc := crc32.ChecksumIEEE(s.logMmap[crcStart:crcEnd])
		if crc != recordCRC {
			return Record{}, fmt.Errorf("record CRC mismatch: %d != %d", crc, recordCRC)
		}

		// Copy the record data into a new buffer.
		// This avoids returning a slice that points directly to the mmap'd region.
		data := make([]byte, dataSize)
		copy(data, s.logMmap[position:position+dataSize])

		return newRecord(recordOffset, recordTimestamp, data), nil
	}

	return Record{}, errors.New("unexpected end of segment file")
}

// sync flushes and syncs the active segment to disk.
func (s *activeSegment) sync() error {
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

// sealedSegment is a read-only representation of a sealed segment of the log.
// Sealed segments are immutable and can be shared between multiple readers.
// A segment becomes sealed after explicitly closing an active segment (be it because of a roll or shutdown),
// or when we restart the log after a crash.
type sealedSegment struct {
	baseOffset Offset // read-only after construction

	length int // cached record count, lazily computed on first Length() call; protected by mu

	logData   []byte // mmap'd .log file; protected by mu
	indexData []byte // mmap'd .index file; protected by mu

	logFile   *os.File // protected by mu
	indexFile *os.File // protected by mu

	mu sync.RWMutex
}

func newSealedSegment(logDir string, baseOffset Offset) (*sealedSegment, error) {
	// Get the absolute file paths for the log and index files.
	logFilePath := logFilePath(logDir, baseOffset)
	indexFilePath := indexFilePath(logDir, baseOffset)

	// Open the log file.
	logFile, err := os.OpenFile(logFilePath, os.O_RDONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	// Open the index file.
	indexFile, err := os.OpenFile(indexFilePath, os.O_RDONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	//
	logStat, err := logFile.Stat()
	if err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, fmt.Errorf("stat log file: %w", err)
	}

	//
	indexStat, err := indexFile.Stat()
	if err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, fmt.Errorf("stat index file: %w", err)
	}

	//
	var logData []byte
	if logData, err = syscall.Mmap(
		int(logFile.Fd()),   // file descriptor
		0,                   // offset
		int(logStat.Size()), // length
		syscall.PROT_READ,   // read-only
		syscall.MAP_SHARED,  // shared mapping
	); err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, fmt.Errorf("mmap log file: %w", err)
	}

	//
	var indexData []byte
	if indexData, err = syscall.Mmap(
		int(indexFile.Fd()),   // file descriptor
		0,                     // offset
		int(indexStat.Size()), // length
		syscall.PROT_READ,     // read-only
		syscall.MAP_SHARED,    // shared mapping
	); err != nil {
		if logData != nil {
			_ = syscall.Munmap(logData)
		}
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, fmt.Errorf("mmap index file: %w", err)
	}

	// Give hints to OS about access patterns (Linux-specific optimization).
	adviseMemoryAccessPatterns(logData, indexData)

	return &sealedSegment{
		baseOffset: baseOffset,
		logData:    logData,
		indexData:  indexData,
		logFile:    logFile,
		indexFile:  indexFile,
	}, nil
}

// Read reads a record from the sealed segment at the given relative offset.
// It uses the mmap'd index to find the starting position and scans the log data.
func (s *sealedSegment) Read(offset Offset) (Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if segment has been closed.
	if s.logData == nil {
		return Record{}, errors.New("segment is closed")
	}

	// Find the starting byte position using the index.
	position := s.findStartingBytePosition(offset)

	for position < int64(len(s.logData)) {
		// Read the record length.
		recordLength := int64(binary.BigEndian.Uint32(s.logData[position : position+recordLengthSize]))

		// Reading length 0 means we've already reached the end of the file
		// and we're reading into the remaining pre-allocated zero'd bytes (linux preallocate)
		// or the kernel is giving us zero bytes due to truncate() (other platforms).
		if recordLength == 0 {
			break
		}
		position += recordLengthSize

		// Read the record CRC.
		recordCRC := binary.BigEndian.Uint32(s.logData[position : position+recordCRCSize])
		position += recordCRCSize

		// Read the record offset.
		recordOffset := int64(binary.BigEndian.Uint64(s.logData[position : position+recordOffsetSize]))
		position += recordOffsetSize

		// This should never really happen.
		if recordOffset > offset {
			return Record{}, fmt.Errorf("unexpected case: found record offset %d greater than the target offset %d while doing a sequential read", recordOffset, offset)
		}

		// Skip the remaining record bytes if this is not the target record (timestamp + data).
		if recordOffset < offset {
			position += recordLength - recordCRCSize - recordOffsetSize
			continue
		}

		// We found the target record. Now read the remaining fields.
		recordTimestamp := int64(binary.BigEndian.Uint64(s.logData[position : position+recordTimestampSize]))
		position += recordTimestampSize

		dataSize := recordDataSize(recordLength)

		// Verify CRC over the binary fields it covers (offset, timestamp, data).
		crcStart := position - recordOffsetSize - recordTimestampSize
		crcEnd := position + dataSize
		crc := crc32.ChecksumIEEE(s.logData[crcStart:crcEnd])
		if crc != recordCRC {
			return Record{}, fmt.Errorf("record CRC mismatch: %d != %d", crc, recordCRC)
		}

		// Copy the record data into a new buffer.
		// This avoids returning a slice that points directly to the mmap'd region.
		data := make([]byte, dataSize)
		copy(data, s.logData[position:position+dataSize])

		return newRecord(recordOffset, recordTimestamp, data), nil
	}

	return Record{}, errors.New("unexpected end of segment file")
}

// BaseOffset returns the base offset of the segment.
func (s *sealedSegment) BaseOffset() Offset {
	return s.baseOffset
}

// Length returns the number of records in the segment.
// The value is lazily computed on the first call and cached for subsequent calls.
func (s *sealedSegment) Length() int {
	// Try read lock first for the common case where length is already computed.
	s.mu.RLock()
	if s.length > 0 {
		length := s.length
		s.mu.RUnlock()
		return length
	}
	s.mu.RUnlock()

	// Need to compute - acquire write lock.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have computed it).
	if s.length > 0 {
		return s.length
	}

	// Compute the length by scanning from the last index entry.
	s.length = s.computeLength()
	return s.length
}

// computeLength scans the segment from the last index entry to count all records.
func (s *sealedSegment) computeLength() int {
	// Handle empty index case.
	if len(s.indexData) == 0 {
		return s.countRecordsFromPosition(0, 0)
	}

	// Get the last index entry.
	lastIndexEntryPosition := len(s.indexData) - indexEntrySize
	lastIndexEntryBytes := s.indexData[lastIndexEntryPosition : lastIndexEntryPosition+indexEntrySize]
	indexEntry, err := decodeIndexEntry(lastIndexEntryBytes)
	if err != nil {
		// If we can't decode, fall back to scanning from the beginning.
		return s.countRecordsFromPosition(0, 0)
	}

	// Count records starting from the last indexed position.
	return s.countRecordsFromPosition(indexEntry.Position, int(indexEntry.RelativeOffset))
}

// countRecordsFromPosition counts records starting from a given byte position and initial offset.
func (s *sealedSegment) countRecordsFromPosition(startPosition int64, initialCount int) int {
	count := initialCount
	position := startPosition

	for position < int64(len(s.logData)) {
		// Read the record length.
		recordLength := int64(binary.BigEndian.Uint32(s.logData[position : position+recordLengthSize]))

		// Reading length 0 means we've reached the end of valid records.
		if recordLength == 0 {
			break
		}

		// Skip to the next record.
		position += recordLengthSize + recordLength
		count++
	}

	return count
}

// Close unmaps the memory-mapped files and closes the file handles.
func (s *sealedSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error

	// Unmap the log data.
	if s.logData != nil {
		if err := syscall.Munmap(s.logData); err != nil {
			errs = append(errs, fmt.Errorf("failed to unmap log data: %w", err))
		}
		s.logData = nil
	}

	// Unmap the index data.
	if s.indexData != nil {
		if err := syscall.Munmap(s.indexData); err != nil {
			errs = append(errs, fmt.Errorf("failed to unmap index data: %w", err))
		}
		s.indexData = nil
	}

	// Close the log file.
	if s.logFile != nil {
		if err := s.logFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close log file: %w", err))
		}
		s.logFile = nil
	}

	// Close the index file.
	if s.indexFile != nil {
		if err := s.indexFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close index file: %w", err))
		}
		s.indexFile = nil
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// findStartingBytePosition finds the starting byte position of the record at the given offset
// or the closest position before the given offset using binary search on the index.
func (s *sealedSegment) findStartingBytePosition(offset Offset) int64 {
	numIndexEntries := len(s.indexData) / indexEntrySize

	// Handle empty index case.
	if numIndexEntries == 0 {
		return 0
	}

	// Binary search to find the entry with the largest offset <= target offset.
	idx := binarySearchIndex(s.indexData, numIndexEntries, offset)

	// Read the index entry at the found position.
	entryStart := idx * indexEntrySize
	entry, err := decodeIndexEntry(s.indexData[entryStart : entryStart+indexEntrySize])
	if err != nil {
		// Not expected to happen, but fall back to start of file.
		return 0
	}

	return entry.Position
}

// binarySearchIndex performs binary search on the index data to find the entry
// with the largest RelativeOffset that is <= the target offset.
func binarySearchIndex(indexData []byte, numEntries int, target Offset) int {
	low, high := 0, numEntries-1
	result := 0

	for low <= high {
		mid := (low + high) / 2
		entryStart := mid * indexEntrySize
		entry, err := decodeIndexEntry(indexData[entryStart : entryStart+indexEntrySize])
		if err != nil {
			// Not expected to happen.
			break
		}

		if entry.RelativeOffset <= target {
			result = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return result
}

// segmentBasePath returns the base path of a segment (without extension).
func segmentBasePath(logDir string, baseOffset Offset) string {
	return fmt.Sprintf("%s/%020d", logDir, baseOffset)
}

// logFilePath returns the absolute file path of the log file for a segment.
func logFilePath(logDir string, baseOffset Offset) string {
	return fmt.Sprintf("%s.log", segmentBasePath(logDir, baseOffset))
}

// indexFilePath returns the absolute file path of the index file for a segment.
func indexFilePath(logDir string, baseOffset Offset) string {
	return fmt.Sprintf("%s.index", segmentBasePath(logDir, baseOffset))
}

// preallocateDiskSpace tries to preallocate disk space for a file.
// It uses the platform-specific preallocation function if available,
// otherwise it falls back to truncating the file (sparse file instead of true preallocation).
// This techniques allows for more predicatable performance characteristics.
func preallocateDiskSpace(file *os.File, size int64) error {
	// Try platform-specific preallocation (Linux fallocate)
	if err := preallocateDiskSpacePlatform(file, size); err == nil {
		return nil
	}

	// Fallback: truncate (works on all platforms)
	// Creates sparse file - not "true" preallocation but sufficient
	return file.Truncate(size)
}

// flushAwareWriter is a writer that sits in between a bufio.Writer and its
// underlying writer.
// Because bufio.Writer only writes to the underlying writer when Flush() is called,
// flushAwareWriter.Write() is called on every Flush(), which presents an opportunity to
// perform additional actions, such as clearing a cache and tracking flushed bytes.
type flushAwareWriter struct {
	w       io.Writer
	onFlush func(bytesWritten int)
}

// newFlushAwareWriter creates a new flushAwareWriter.
func newFlushAwareWriter(w io.Writer, onFlush func(bytesWritten int)) *flushAwareWriter {
	return &flushAwareWriter{
		w:       w,
		onFlush: onFlush,
	}
}

// Write just proxies to the underlying writer and calls the onFlush after a successful write.
func (w *flushAwareWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	if err != nil {
		return n, err
	}

	w.onFlush(n)
	return n, nil
}
