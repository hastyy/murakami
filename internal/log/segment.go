package log

import (
	"bufio"
	"errors"
	"fmt"
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
		indexEncodingBuffer:  indexEncodingBuffer,
		recordCache:          recordCache,
		indexCache:           indexCache,
	}, nil
}

func (s *activeSegment) Append(data []byte, unixTimestamp int64) (Offset, error) {
	return 0, errors.New("not implemented")
}

func (s *activeSegment) Read(offset Offset) (Record, error) {
	return Record{}, errors.New("not implemented")
}

func (s *activeSegment) BaseOffset() Offset {
	panic("sealedSegment.BaseOffset() not implemented")
}

func (s *activeSegment) Length() int {
	panic("sealedSegment.Length() not implemented")
}

// Size returns the size of the segment in bytes.
func (s *activeSegment) Size() int {
	panic("sealedSegment.Size() not implemented")
}

func (s *activeSegment) Sync() error {
	return errors.New("not implemented")
}

func (s *activeSegment) Close() error {
	return errors.New("not implemented")
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
