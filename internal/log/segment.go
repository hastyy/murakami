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

	logFile     *os.File // protected by mu
	indexFile   *os.File // protected by mu
	logReadFile *os.File // protected by logReadFileMu

	logWriter   *bufio.Writer // protected by mu
	indexWriter *bufio.Writer // protected by mu

	recordEncodingBuffer []byte // protected by mu
	recordDecodingBuffer []byte // protected by logReadFileMu
	indexEncodingBuffer  []byte // protected by mu

	recordCache *recordCache // built-in concurrenncy control
	indexCache  indexCache   // protected by mu
	// <end of state that needs to be synchronized>

	mu            sync.RWMutex
	logReadFileMu sync.Mutex
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
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	// Open the index file.
	indexFile, err := os.OpenFile(indexFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	// Try to preallocate disk space for the log file.
	err = preallocateDiskSpace(logFile, int64(cfg.MaxSegmentSize))
	if err != nil {
		return nil, fmt.Errorf("failed to preallocate log file: %w", err)
	}

	//
	logReadFile, err := os.OpenFile(logFilePath, os.O_RDONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log read file: %w", err)
	}

	// Create the record cache to hold a copy of the records still in the buffered writer's internal buffer.
	recordCache := newRecordCache(cfg.RecordCacheSize)

	// flushAwareWriter wraps the logFile to detect when its .Write() method is called
	// (meaning the buffered writer has flushed) and calls the recordCache.Clear() method
	// to keep the buffer and the cache in sync.
	flushAwareWriter := newFlushAwareWriter(logFile, recordCache.Clear)

	// Create the buffered writers for the log and index files.
	logWriter := bufio.NewWriterSize(flushAwareWriter, cfg.LogWriterBufferSize)
	indexWriter := bufio.NewWriterSize(indexFile, cfg.IndexWriterBufferSize)

	// Calculate the maximum size of a record in the log file.
	maxRecordSize := recordLengthSize + recordCRCSize + recordOffsetSize + recordTimestampSize + cfg.MaxRecordDataSize

	// Create the encoding buffers for the log and index files.
	// We use a fixed-size buffer for the encoding to avoid unnecessary allocations.
	recordEncodingBuffer := make([]byte, maxRecordSize)
	recordDecodingBuffer := make([]byte, maxRecordSize)
	indexEncodingBuffer := make([]byte, indexEntrySize)

	// Create the index cache to hold all the index entries for the active segment.
	indexCache := newIndexCache(cfg.IndexCacheSize)

	return &activeSegment{
		cfg:                  cfg,
		baseOffset:           baseOffset,
		logFile:              logFile,
		indexFile:            indexFile,
		logReadFile:          logReadFile,
		logWriter:            logWriter,
		indexWriter:          indexWriter,
		recordEncodingBuffer: recordEncodingBuffer,
		recordDecodingBuffer: recordDecodingBuffer,
		indexEncodingBuffer:  indexEncodingBuffer,
		recordCache:          recordCache,
		indexCache:           indexCache,
	}, nil
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
// It first tries to locate the record in the record cache.
// If not found, it reads the index and then looks for the record in the read file.
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

	// Try to read from the record cache.
	recordEntry, found := s.recordCache.Get(offset)
	if found {
		return recordEntry.record, nil
	}

	// At this point we know the record we're looking for needs to be in the file
	// because the offset is >= than our baseOffset and < than any offset in the record cache
	indexCacheSnapshot := s.indexCache[:indexCacheLen]
	indexEntry, _ := indexCacheSnapshot.Find(offset)

	return s.readRecordFromFile(offset, indexEntry.Position)
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

	err = s.logReadFile.Close()
	if err != nil {
		return err
	}

	return nil
}

// shouldIndex checks if we should index the current record based on the index interval.
// If the position is 0, we index the first record.
// We then continue adding index entries at the byte interval specified by the config.
func (s *activeSegment) shouldIndex(position int64) bool {
	return position == 0 || (position-s.lastIndexedPosition >= int64(s.cfg.IndexIntervalBytes))
}

// readRecordFromFile reads a record from the read file at the given offset.
// It receives a start position in the file which might match the start of the target record
// or some other record before it.
// Pre-condition: offset is >= than our baseOffset and < than any offset in the record cache.
func (s *activeSegment) readRecordFromFile(offset Offset, startPosition int64) (Record, error) {
	// Lock the read file to prevent concurrent reads.
	s.logReadFileMu.Lock()
	defer s.logReadFileMu.Unlock()

	// Seek to the position of the index entry in the read file.
	_, err := s.logReadFile.Seek(startPosition, io.SeekStart)
	if err != nil {
		return Record{}, fmt.Errorf("failed to seek to position %d in read file: %w", startPosition, err)
	}

	// TODO: introduce buffered io reader?
	for {
		var recordLength int64
		err := binary.Read(s.logReadFile, binary.BigEndian, &recordLength)
		if err != nil {
			if err == io.EOF {
				break
			}
			return Record{}, fmt.Errorf("failed to read record length: %w", err)
		}

		// Reading length 0 means we've already reached the end of the file
		// and we're reading into the remaining pre-allocated zero'd bytes (linux preallocate)
		// or the kernel is giving us zero bytes due to truncate() (other platforms).
		// This will happen when the segment was closed before we've used all the available space
		// which can happen frequently if we roll segments because the next record doesn't fit
		// anymore even though there's space left in the .log file.
		if recordLength == 0 {
			break
		}

		var recordCRC uint32
		err = binary.Read(s.logReadFile, binary.BigEndian, &recordCRC)
		if err != nil {
			return Record{}, fmt.Errorf("failed to read record CRC: %w", err)
		}

		// Read the record offset.
		// Keep it in the decoding buffer to calculate the CRC later on.
		// This avoids allocating a new buffer.
		_, err = io.ReadFull(s.logReadFile, s.recordDecodingBuffer[:recordOffsetSize])
		if err != nil {
			return Record{}, fmt.Errorf("failed to read record offset: %w", err)
		}
		recordOffset := int64(binary.BigEndian.Uint64(s.recordDecodingBuffer[:recordOffsetSize]))

		// This should never really happen.
		if recordOffset > offset {
			return Record{}, fmt.Errorf("unexpected case: found record offset %d greater than the target offset %d while doing a sequential read", recordOffset, offset)
		}

		// Skip the remaining record bytes.
		if recordOffset < offset {
			err = skipBytes(s.logReadFile, recordLength-recordCRCSize-recordOffsetSize)
			if err != nil {
				return Record{}, fmt.Errorf("failed to skip remaining record bytes: %w", err)
			}
			continue
		}

		position := recordOffsetSize

		// Read the record timestamp.
		// Keep it in the decoding buffer to calculate the CRC later on.
		// This avoids allocating a new buffer.
		_, err = io.ReadFull(s.logReadFile, s.recordDecodingBuffer[position:position+recordTimestampSize])
		if err != nil {
			return Record{}, fmt.Errorf("failed to read record timestamp: %w", err)
		}
		recordTimestamp := int64(binary.BigEndian.Uint64(s.recordDecodingBuffer[position : position+recordTimestampSize]))

		position += recordTimestampSize

		// Read the record data.
		// Keep it in the decoding buffer to calculate the CRC later on.
		// This avoids allocating a new buffer.
		dataSize := int(recordLength - recordCRCSize - recordOffsetSize - recordTimestampSize)
		_, err = io.ReadFull(s.logReadFile, s.recordDecodingBuffer[position:position+dataSize])
		if err != nil {
			return Record{}, fmt.Errorf("failed to read record data: %w", err)
		}

		// Calculate the CRC over the binary fields it covers and check the result against the stored CRC.
		crc := crc32.ChecksumIEEE(s.recordDecodingBuffer[:position+dataSize])
		if crc != recordCRC {
			return Record{}, fmt.Errorf("record CRC mismatch: %d != %d", crc, recordCRC)
		}

		// Copy the record data into a new buffer.
		// This avoids returning a slice to the caller that would point to the decoding buffer.
		data := make([]byte, dataSize)
		copy(data, s.recordDecodingBuffer[position:position+dataSize])

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
type sealedSegment struct{}

func newSealedSegment(logDir string, baseOffset Offset) (*sealedSegment, error) {
	return nil, errors.New("not implemented")
}

func (s *sealedSegment) Read(offset Offset) (Record, error) {
	return Record{}, errors.New("not implemented")
}

func (s *sealedSegment) BaseOffset() Offset {
	panic("sealedSegment.BaseOffset() not implemented")
}

func (s *sealedSegment) Length() int {
	panic("sealedSegment.Length() not implemented")
}

func (s *sealedSegment) Close() error {
	return errors.New("not implemented")
}

// segmentAbsoluteFilename returns the absolute filename of a segment.
func segmentAbsoluteFilename(logDir string, baseOffset Offset) string {
	return fmt.Sprintf("%s/%020d.log", logDir, baseOffset)
}

// logFilePath returns the absolute file path of the log file for a segment.
func logFilePath(logDir string, baseOffset Offset) string {
	return fmt.Sprintf("%s.log", segmentAbsoluteFilename(logDir, baseOffset))
}

// indexFilePath returns the absolute file path of the index file for a segment.
func indexFilePath(logDir string, baseOffset Offset) string {
	return fmt.Sprintf("%s.index", segmentAbsoluteFilename(logDir, baseOffset))
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

// skipBytes consumes the next n bytes from the reader without processing them.
func skipBytes(r io.Reader, n int64) error {
	_, err := io.CopyN(io.Discard, r, n)
	return err
}

// flushAwareWriter is a writer that sits in between a bufio.Writer and its
// underlying writer.
// Because bufio.Writer only writes to the underlying writer when Flush() is called,
// flushAwareWriter.Write() is called on every Flush(), which presents an opportunity to
// perform additional actions, such as clearing a cache.
type flushAwareWriter struct {
	w       io.Writer
	onFlush func()
}

// newFlushAwareWriter creates a new flushAwareWriter.
func newFlushAwareWriter(w io.Writer, onFlush func()) *flushAwareWriter {
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

	w.onFlush()
	return n, nil
}
