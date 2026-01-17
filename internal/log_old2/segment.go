package log_old2

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
	"syscall"
)

type readSegment interface {
	BaseOffset() Offset
	Length() int64
	Read(Offset) ([]byte, error)
	Close() error
}

type sealedSegment struct {
	baseOffset Offset
	length     int64

	logData   []byte // mmap'd .log file
	indexData []byte // mmap'd .index file

	logFile   *os.File
	indexFile *os.File

	// TODO: probably needs a RWLock to sync read and close operations
	// if so, add to all read methos, even private ones for the io.Reader
}

func newSealedSegment(dir string, baseOffset Offset) (*sealedSegment, error) {
	//
	logFilePath := fmt.Sprintf("%s/%020d.log", dir, baseOffset)
	indexFilePath := fmt.Sprintf("%s/%020d.index", dir, baseOffset)

	//
	logFile, err := os.OpenFile(logFilePath, os.O_RDONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	//
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

	// Give hints to OS about access patterns (Linux-specific optimization)
	adviseMemoryAccessPatterns(logData, indexData)

	s := &sealedSegment{
		baseOffset: baseOffset,
		logData:    logData,
		indexData:  indexData,
		logFile:    logFile,
		indexFile:  indexFile,
	}

	//
	s.setLength()

	return s, nil
}

func (s *sealedSegment) setLength() error {
	//
	lastIndexEntryPosition := len(s.indexData) - indexEntrySize
	lastIndexEntryBytes := s.indexData[lastIndexEntryPosition : lastIndexEntryPosition+indexEntrySize]
	indexEntry, err := decodeIndexEntry(lastIndexEntryBytes)
	if err != nil {
		return err
	}

	//
	s.length = indexEntry.RelativeOffset
	for reader := bytes.NewReader(s.logData[indexEntry.Position:]); reader.Len() > 0; s.length++ {
		//
		var recordLength uint32
		err := binary.Read(reader, binary.BigEndian, &recordLength)
		if err != nil {
			if s.length == indexEntry.RelativeOffset {
				return fmt.Errorf("failed to read first record from last indexed position: %w", err)
			}
			if err == io.EOF {
				// previously read record was the last one
				break
			}
			return fmt.Errorf("failed to read record length: %w", err)
		}

		// Reading length 0 means we've already reached the end of the file
		// and we're reading into the remaining pre-allocated zero'd bytes (linux preallocate)
		// or the kernel is giving us zero bytes due to truncate() (other platforms).
		// This will happen when the segment was closed before we've used all the available space
		// which can happen frequently if we roll segments because the next record doesn't fit
		// anymore even though there's space left in the .log file.
		if recordLength == 0 {
			if s.length == indexEntry.RelativeOffset {
				return errors.New("failed to read first record from last indexed position: read length 0")
			}
			break
		}

		// skip the actual record bytes (CRC + offset + timestamp + data)
		err = skipBytes(reader, int64(recordLength))
		if err != nil {
			return fmt.Errorf("failed to skip record: %w", err)
		}
	}

	return nil
}

func (s *sealedSegment) BaseOffset() Offset {
	return s.baseOffset
}

func (s *sealedSegment) Length() int64 {
	return s.length
}

func (s *sealedSegment) Read(offset Offset) ([]byte, error) {
	//
	position := s.findStartingBytePosition(offset)

	for {
		//
		recordPosition := position

		//
		recordLength := int64(binary.BigEndian.Uint32(s.logData[position : position+recordLengthSize]))
		if recordLength == 0 {
			break
		}
		position += recordLengthSize

		recordCRC := binary.BigEndian.Uint32(s.logData[position : position+recordCRCSize])
		position += recordCRCSize

		recordOffset := int64(binary.BigEndian.Uint32(s.logData[position : position+recordOffsetSize]))
		position += recordOffsetSize

		// This should never really happen.
		if recordOffset > offset {
			return nil, fmt.Errorf("unexpected case: record offset %d is greater than the target offset %d", recordOffset, offset)
		}

		if recordOffset < offset {
			// Skip the remaining record bytes.
			position += int64(recordLength - recordCRCSize - recordOffsetSize)
			continue
		}

		crc := crc32.ChecksumIEEE(s.logData[recordPosition+recordLengthSize+recordCRCSize : recordPosition+recordLengthSize+recordLength])
		if crc != recordCRC {
			return nil, fmt.Errorf("record CRC mismatch: %d != %d", crc, recordCRC)
		}

		//recordTimestamp := int64(binary.BigEndian.Uint32(s.logData[position : position+recordTimestampSize]))
		position += recordTimestampSize

		dataSize := recordLength - recordCRCSize - recordOffsetSize - recordTimestampSize
		data := make([]byte, dataSize)

		copy(data, s.logData[position:position+dataSize])

		return data, nil
	}

	return nil, errors.New("unexpected end of segment file")
}

/*
func (s *sealedSegment) Read(offset Offset) (io.Reader, error) {
	//
	position := s.findStartingBytePosition(offset)
	for {
		//
		recordPosition := position

		//
		recordLength := binary.BigEndian.Uint32(s.indexData[position : position+recordLengthSize])
		if recordLength == 0 {
			break
		}
		position += recordLengthSize

		// Skip the record CRC.
		position += recordCRCSize

		//
		recordOffset := int64(binary.BigEndian.Uint32(s.indexData[position : position+recordOffsetSize]))
		position += recordOffsetSize

		// This should never really happen.
		if recordOffset > offset {
			return nil, fmt.Errorf("unexpected case: record offset %d is greater than the target offset %d", recordOffset, offset)
		}

		if recordOffset < offset {
			// Skip the remaining record bytes.
			position += int64(recordLength - recordCRCSize - recordOffsetSize)
			continue
		}

		return newSealedSegmentReader(s, recordPosition), nil
	}

	return nil, errors.New("unexpected end of segment file")
}

func (s *sealedSegment) readRecordAtPosition(p []byte, position int64) (length int, err error) {
	recordLength := int64(binary.BigEndian.Uint32(s.indexData[position : position+recordLengthSize]))
	if recordLength == 0 {
		return 0, io.EOF
	}
	position += recordLengthSize

	recordPosition := position

	recordCRC := binary.BigEndian.Uint32(s.indexData[position : position+recordCRCSize])
	position += recordCRCSize

	// recordOffset := int64(binary.BigEndian.Uint32(s.indexData[position : position+recordOffsetSize]))
	position += recordOffsetSize

	// recordTimestamp := int64(binary.BigEndian.Uint32(s.indexData[position : position+recordTimestampSize]))
	position += recordTimestampSize

	dataSize := recordLength - recordCRCSize - recordOffsetSize - recordTimestampSize
	if len(p) < int(dataSize) {
		return 0, io.ErrShortBuffer
	}

	crc := crc32.ChecksumIEEE(s.logData[recordPosition+recordCRCSize : recordPosition+recordLength])
	if crc != recordCRC {
		return 0, fmt.Errorf("record CRC mismatch: %d != %d", crc, recordCRC)
	}

	copy(p, s.logData[position:position+dataSize])
	//position += dataSize

	return int(recordLength), nil

}

type sealedSegmentReader struct {
	segment  *sealedSegment
	position int64
}

func newSealedSegmentReader(segment *sealedSegment, position int64) *sealedSegmentReader {
	return &sealedSegmentReader{
		segment:  segment,
		position: position,
	}
}

func (r *sealedSegmentReader) Read(p []byte) (int, error) {
	recordLength, err := r.segment.readRecordAtPosition(p, r.position)
	if err != nil {
		return 0, err
	}
	r.position += int64(recordLength)
	return recordLength, nil
}
*/

// findStartingBytePosition finds the starting byte position of the record at the given offset
// or a close one before the given offset.
func (s *sealedSegment) findStartingBytePosition(offset Offset) int64 {
	numIndexEntries := len(s.indexData) / indexEntrySize

	idx := sort.Search(numIndexEntries, func(i int) bool {
		entryStart := i * indexEntrySize
		entry, err := decodeIndexEntry(s.indexData[entryStart : entryStart+indexEntrySize])
		if err != nil {
			// not expected to happen
			panic(fmt.Errorf("failed to decode index entry: %w", err))
		}
		return entry.RelativeOffset >= offset
	})

	if idx == numIndexEntries {
		idx = numIndexEntries - 1
	}

	//
	entryStart := idx * indexEntrySize
	entry, err := decodeIndexEntry(s.indexData[entryStart : entryStart+indexEntrySize])
	if err != nil {
		// not expected to happen
		panic(fmt.Errorf("failed to decode index entry: %w", err))
	}

	return entry.Position
}

func (s *sealedSegment) Close() error {
	return errors.New("not implemented")
}

type activeSegment struct {
	cfg        Config // read-only
	baseOffset int64  // read-only

	// <start of state that needs to be synchronized>
	nextOffset          int64 // protected by mu
	bytePosition        int64 // protected by mu
	lastIndexedPosition int64 // protected by mu

	logFile   *os.File // protected by mu
	indexFile *os.File // protected by mu

	logWriter   *bufio.Writer // protected by mu
	indexWriter *bufio.Writer // protected by mu

	recordEncodingBuffer []byte // protected by mu
	indexEncodingBuffer  []byte // protected by mu

	logReadFile *os.File      // protected by logReadFileMu
	logReader   *bufio.Reader // protected by logReadFileMu
	recordCache *recordCache  // built-in concurrenncy control
	indexCache  indexCache    // protected by mu
	// <end of state that needs to be synchronized>

	mu            sync.RWMutex
	logReadFileMu sync.Mutex
}

func newActiveSegment(dir string, baseOffset Offset, cfg Config) (*activeSegment, error) {
	// Make sure the log directory exists
	err := os.MkdirAll(dir, directoryPermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	//
	logFilePath := fmt.Sprintf("%s/%020d.log", dir, baseOffset)
	indexFilePath := fmt.Sprintf("%s/%020d.index", dir, baseOffset)

	//
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	//
	indexFile, err := os.OpenFile(indexFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	//
	err = preallocateDiskSpace(logFile, cfg.MaxSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("failed to preallocate log file: %w", err)
	}

	//
	logReadFile, err := os.OpenFile(logFilePath, os.O_RDONLY, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log read file: %w", err)
	}

	//
	recordCache := newRecordCache(cfg.RecordCacheSize)
	indexCache := make(indexCache, 0, cfg.IndexCacheSize)

	// flushAwareWriter wraps the logFile to detect when its .Write() method is called
	// (meaning the buffered writer has flushed) and calls the recordCache.Clear() method.
	flushAwareWriter := newFlushAwareWriter(logFile, recordCache.Clear)

	//
	logWriter := bufio.NewWriterSize(flushAwareWriter, cfg.LogWriterBufferSize)
	indexWriter := bufio.NewWriterSize(indexFile, cfg.IndexWriterBufferSize)
	logReader := bufio.NewReaderSize(logReadFile, cfg.LogReaderBufferSize)

	//
	recordEncodingBuffer := make([]byte, recordLengthSize+recordCRCSize+recordOffsetSize+recordTimestampSize+cfg.MaxRecordDataSize)
	indexEncodingBuffer := make([]byte, indexEntrySize)

	return &activeSegment{
		cfg:                  cfg,
		baseOffset:           baseOffset,
		logFile:              logFile,
		indexFile:            indexFile,
		logWriter:            logWriter,
		indexWriter:          indexWriter,
		recordEncodingBuffer: recordEncodingBuffer,
		indexEncodingBuffer:  indexEncodingBuffer,
		logReadFile:          logReadFile,
		logReader:            logReader,
		recordCache:          recordCache,
		indexCache:           indexCache,
	}, nil
}

func (s *activeSegment) BaseOffset() Offset {
	return s.baseOffset
}

func (s *activeSegment) Length() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.nextOffset
}

func (s *activeSegment) Read(offset Offset) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (s *activeSegment) Last() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	record, position, found := s.recordCache.Get(s.nextOffset - 1)
	if found {
		return record.Data, nil
	}

	_, err := s.logReadFile.Seek(s.lastIndexedPosition, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to position %d: %w", position, err)
	}

	// TODO: scan until the end (length = 0 or EOF , return last successfully decoded record)
	return nil, errors.New("not implemented")
}

/*
func (s *activeSegment) Read(offset Offset) (io.Reader, error) {
	// Snapshot the state limits under read lock. This gives us a consistent view
	// of what's "known" at this point. Everything after this snapshot is
	// being appended concurrently, but everything before is immutable.
	s.mu.RLock()
	lastOffset := s.nextOffset - 1 // nextOffset is the next one to be written
	indexCacheLen := len(s.indexCache)
	s.mu.RUnlock()

	if offset > lastOffset {
		return nil, fmt.Errorf("offset %d is greater than the last offset %d", offset, lastOffset)
	}

	//
	record, position, found := s.recordCache.Get(offset)
	if found {
		// TODO: return new reader with record?
		return nil, errors.New("not implemented")
	}

	// at this point we know the record we're looking for needs to be in the file
	// because the offset is >= than our baseOffset and < than any offset in the record cache
	indexEntry, found := s.indexCache[:indexCacheLen].Find(offset)
	if found {

	}

	return nil, fmt.Errorf("not implemented")
}

func (s *activeSegment) readRecordAtPosition(p []byte, position int64) (length int, err error) {
	s.mu.RLock()
	lastOffset := s.nextOffset - 1
	s.mu.RUnlock()

	_, err = s.logReadFile.Seek(position, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to position %d: %w", position, err)
	}
}
*/

func (s *activeSegment) Append(data []byte) (Offset, error) {
	// Each call to Append() must be fully serialized.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the offset and initial byte position (on the .log file) for the new record.
	offset := s.nextOffset
	position := s.bytePosition

	// Create the record and cache it since it'll land in the logWriter's internal buffer first.
	record := newRecord(offset, data)
	s.recordCache.Add(record, position)

	// Encode the record into the buffer.
	n, err := encodeRecord(record, s.recordEncodingBuffer)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}

	// Write the record to the logWriter.
	// If the whole record doesn't fit in the buffer, flush it first to avoid writing
	// part of the record in the .log file and another part in the buffer.
	// It's best to have each record fully on one side and avoid fragmentation.
	// This also guarantees atomicity to each append operation.
	recordBytes := s.recordEncodingBuffer[:n]
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
		//
		indexEntry := newIndexEntry(offset, position)
		s.indexCache = append(s.indexCache, indexEntry)

		//
		_, err = encodeIndexEntry(indexEntry, s.indexEncodingBuffer)
		if err != nil {
			return 0, fmt.Errorf("failed to encode index entry: %w", err)
		}

		//
		_, err = s.indexWriter.Write(s.indexEncodingBuffer) // no need to slice because the buffer already has the correct size
		if err != nil {
			return 0, fmt.Errorf("failed to write index entry: %w", err)
		}

		//
		s.lastIndexedPosition = position
	}

	return offset, nil
}

// shouldIndex checks if we should index the current record based on the index interval.
// If the position is 0, we index the first record.
// We then continue adding index entries at the byte interval specified by the config.
func (s *activeSegment) shouldIndex(position int64) bool {
	return position == 0 || (position-s.lastIndexedPosition >= s.cfg.IndexIntervalBytes)
}

func (s *activeSegment) Close() error {
	return errors.New("not implemented")
}

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
