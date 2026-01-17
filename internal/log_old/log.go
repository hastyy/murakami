package log_old

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/hastyy/murakami/internal/unit"
)

type Offset = int64

const (
	DirectoryPermissions = 0700
	FilePermissions      = 0600

	RecordLengthSize    = 4
	RecordCRCSize       = 4
	RecordOffsetSize    = 8
	RecordTimestampSize = 8

	IndexEntrySize = 8

	initialSegmentSliceCapacity = 16
)

type Config struct {
	MaxSegmentSize     int64
	MaxRecordDataSize  int64
	IndexIntervalBytes int64

	// EnableBufferedWrites bool
	LogWriterBufferSize   int
	IndexWriterBufferSize int
	LogReaderBufferSize   int

	RecordCacheSize int
	IndexCacheSize  int
}

type Segment interface {
	BaseOffset() Offset
	Read(Offset) ([]byte, error)
}

type Log struct {
	segments      []Segment
	activeSegment *ActiveSegment
}

func NewLog(dataDir, name string) (*Log, error) {
	// should list the segments in the directory (can be empty, especially if directory doesn't exist)
	// should sort all segment file names (offset order)
	// should add each of these to the closed segments list
	// should then start a new active segment with the next offset
	// should also perform the recovery step at some point, probably before initializing the active segment
	// should then delete the clean shutdown marker

	cfg := Config{
		MaxSegmentSize:        1. * unit.GiB,
		MaxRecordDataSize:     1 * unit.KiB,
		IndexIntervalBytes:    4 * unit.KiB,
		LogWriterBufferSize:   64 * unit.KiB,
		LogReaderBufferSize:   16 * unit.KiB,
		IndexWriterBufferSize: 16 * unit.KiB,
		RecordCacheSize:       256,
		IndexCacheSize:        500_000,
	}

	logDir := fmt.Sprintf("%s/%s", dataDir, name)

	// Get all .log files, sorted lexicographically
	segmentFiles, err := filepath.Glob(filepath.Join(logDir, "*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to get segment files: %w", err)
	}

	segments := make([]Segment, 0, min(initialSegmentSliceCapacity, len(segmentFiles)*2))

	for _, segmentFile := range segmentFiles {
		filename := filepath.Base(segmentFile)
		baseOffsetStr := strings.TrimSuffix(filename, ".log")

		baseOffset, err := strconv.ParseInt(baseOffsetStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid base offset in filename %s: %w", filename, err)
		}

		sealedSegment, err := NewSealedSegment(logDir, baseOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to create sealed segment: %w", err)
		}

		segments = append(segments, sealedSegment)
	}

	var activeSegmentBaseOffset Offset
	if len(segments) > 0 {
		lastSealedSegment := segments[len(segments)-1].(*SealedSegment)
		lastRecord, err := lastSealedSegment.findLastRecord()
		if err != nil {
			return nil, fmt.Errorf("failed to find last record: %w", err)
		}
		activeSegmentBaseOffset = lastRecord.Offset + 1
	}

	activeSegment, err := NewActiveSegment(logDir, activeSegmentBaseOffset, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create active segment: %w", err)
	}

	segments = append(segments, activeSegment)

	return &Log{
		segments:      segments,
		activeSegment: activeSegment,
	}, nil
}

// TODO: rolling segment logic
func (l *Log) Append(data []byte) (Offset, error) {
	relativeOffset, err := l.activeSegment.Append(data)
	if err != nil {
		return 0, fmt.Errorf("failed to append to active segment: %w", err)
	}

	return l.activeSegment.BaseOffset() + relativeOffset, nil
}

func (l *Log) Read(offset Offset) ([]byte, error) {
	idx, found := slices.BinarySearchFunc(l.segments, offset, func(curr Segment, target Offset) int {
		return int(curr.BaseOffset() - target)
	})

	if found {
		segment := l.segments[idx]
		return segment.Read(offset - segment.BaseOffset())
	}

	if idx == 0 {
		// 'impossible' case
		return nil, fmt.Errorf("offset %d is smaller than the first offset %d", offset, l.segments[0].BaseOffset())
	}

	if idx == len(l.segments) {
		return nil, fmt.Errorf("offset %d is greater than the last offset %d", offset, l.segments[len(l.segments)-1].BaseOffset())
	}

	prevSegment := l.segments[idx-1]
	return prevSegment.Read(offset - prevSegment.BaseOffset())
}

func (l *Log) Close() error {
	// close active segment
	// perform clean shutdown
	return nil
}

type ActiveSegment struct {
	baseOffset int64  // read-only
	cfg        Config // read-only

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
	recordCache *RecordCache  // built-in concurrenncy control
	indexCache  IndexCache    // protected by mu
	// <end of state that needs to be synchronized>

	mu            sync.RWMutex
	logReadFileMu sync.Mutex
}

func NewActiveSegment(dir string, baseOffset Offset, cfg Config) (*ActiveSegment, error) {
	// Make sure the log directory exists
	err := os.MkdirAll(dir, DirectoryPermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	logFilePath := fmt.Sprintf("%s/%020d.log", dir, baseOffset)
	indexFilePath := fmt.Sprintf("%s/%020d.index", dir, baseOffset)

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, FilePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	indexFile, err := os.OpenFile(indexFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, FilePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	err = preallocateDiskSpace(logFile, cfg.MaxSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("failed to preallocate log file: %w", err)
	}

	logReadFile, err := os.OpenFile(logFilePath, os.O_RDONLY, FilePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log read file: %w", err)
	}

	logReader := bufio.NewReaderSize(logReadFile, cfg.LogReaderBufferSize)

	recordCache := NewRecordCache(cfg.RecordCacheSize)
	indexCache := make([]IndexEntry, 0, cfg.IndexCacheSize)

	flushAwareWriter := newFlushAwareWriter(logFile, func() {
		recordCache.Clear()
	})

	logWriter := bufio.NewWriterSize(flushAwareWriter, cfg.LogWriterBufferSize)
	indexWriter := bufio.NewWriterSize(indexFile, cfg.IndexWriterBufferSize)

	maxRecordSize := RecordLengthSize + RecordCRCSize + RecordOffsetSize + cfg.MaxRecordDataSize
	recordEncodingBuffer := make([]byte, maxRecordSize)
	indexEncodingBuffer := make([]byte, IndexEntrySize)

	return &ActiveSegment{
		baseOffset:           baseOffset,
		cfg:                  cfg,
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

func preallocateDiskSpace(file *os.File, size int64) error {
	// Try platform-specific preallocation (Linux fallocate)
	if err := preallocateDiskSpacePlatform(file, size); err == nil {
		return nil
	}

	// Fallback: truncate (works on all platforms)
	// Creates sparse file - not "true" preallocation but sufficient
	return file.Truncate(size)
}

func (s *ActiveSegment) BaseOffset() Offset {
	return s.baseOffset
}

func (s *ActiveSegment) Append(data []byte) (Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := s.nextOffset
	position := s.bytePosition

	record := NewRecord(offset, data)
	s.recordCache.Add(record)

	n, err := record.MarshalBinary(s.recordEncodingBuffer)
	if err != nil {
		return 0, err
	}

	recordBytes := s.recordEncodingBuffer[:n]
	_, err = s.logWriter.Write(recordBytes)
	if err != nil {
		return 0, err
	}

	s.bytePosition += int64(n)
	s.nextOffset++

	if position-s.lastIndexedPosition >= s.cfg.IndexIntervalBytes {
		ie := NewIndexEntry(offset, position)
		s.indexCache = append(s.indexCache, ie)

		_, err = ie.MarshalBinary(s.indexEncodingBuffer)
		if err != nil {
			return 0, err
		}

		_, err = s.indexWriter.Write(s.indexEncodingBuffer)
		if err != nil {
			return 0, err
		}

		s.lastIndexedPosition = position
	}

	return offset, nil
}

func (s *ActiveSegment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sync()
}

func (s *ActiveSegment) sync() error {
	err := s.logFile.Sync()
	if err != nil {
		return err
	}

	err = s.indexFile.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (s *ActiveSegment) Read(offset Offset) ([]byte, error) {
	s.mu.RLock()
	lastOffset := s.nextOffset - 1
	cacheSlice := s.indexCache[:len(s.indexCache)]
	s.mu.RUnlock()

	if offset > lastOffset {
		return nil, fmt.Errorf("offset %d is greater than the last offset %d", offset, lastOffset)
	}

	record, found := s.recordCache.Get(offset)
	if found {
		return record.Data, nil
	}

	indexEntry := cacheSlice.Find(offset)

	s.logReadFileMu.Lock()
	defer s.logReadFileMu.Unlock()
	_, err := s.logReadFile.Seek(indexEntry.Position, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to index entry position %d: %w", indexEntry.Position, err)
	}

	for currOffset := indexEntry.RelativeOffset; currOffset < offset; currOffset++ {
		var recordLength uint32
		err := binary.Read(s.logReader, binary.BigEndian, &recordLength)
		if err != nil {
			return nil, fmt.Errorf("failed to read record length: %w", err)
		}

		var recordCRC uint32
		err = binary.Read(s.logReader, binary.BigEndian, &recordCRC)
		if err != nil {
			return nil, fmt.Errorf("failed to read record CRC: %w", err)
		}

		var recordOffset int64
		err = binary.Read(s.logReader, binary.BigEndian, &recordOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to read record offset: %w", err)
		}

		if recordOffset == offset {
			var timestamp int64
			err = binary.Read(s.logReader, binary.BigEndian, &timestamp)
			if err != nil {
				return nil, fmt.Errorf("failed to read record timestamp: %w", err)
			}

			data := make([]byte, recordLength-RecordCRCSize-RecordOffsetSize-RecordTimestampSize)
			_, err = io.ReadFull(s.logReader, data)
			if err != nil {
				return nil, fmt.Errorf("failed to read record data: %w", err)
			}

			crc := crc32.ChecksumIEEE(data)
			if recordCRC != crc {
				return nil, fmt.Errorf("record CRC mismatch: %d != %d", crc, crc)
			}

			return data, nil
		} else {
			// _ = s.logReader.Skip(int(recordLength-RecordCRCSize-RecordOffsetSize))
			// _ = s.logReader.Next(int(recordLength - RecordCRCSize - RecordOffsetSize))
			_, err = s.logReader.Read(make([]byte, int(recordLength-RecordCRCSize-RecordOffsetSize)))
			if err != nil {
				return nil, fmt.Errorf("failed to read record: %w", err)
			}
		}
	}

	return nil, fmt.Errorf("record not found for offset %d", offset)
}

type Record struct {
	Offset    Offset
	Timestamp int64
	Data      []byte
}

func NewRecord(recordOffset Offset, data []byte) Record {
	return Record{
		Offset: recordOffset,
		Data:   data,
	}
}

func (r Record) MarshalBinary(buf []byte) (int, error) {
	binLen := r.BinaryLength()
	binSize := RecordLengthSize + binLen

	// Write record binary length prefix
	binary.BigEndian.PutUint32(buf[0:RecordLengthSize], uint32(binLen))

	// Skip CRC (which will go on buf[4:8]) and write everything else first
	pos := RecordLengthSize + RecordCRCSize
	binary.BigEndian.PutUint64(buf[pos:pos+RecordOffsetSize], uint64(r.Offset))
	pos += RecordOffsetSize
	binary.BigEndian.PutUint64(buf[pos:pos+RecordTimestampSize], uint64(r.Timestamp))
	pos += RecordTimestampSize
	copy(buf[pos:], r.Data)

	// Compute and write CRC of [offset][timestamp][data]
	crc := crc32.ChecksumIEEE(buf[RecordLengthSize+RecordCRCSize : binSize])
	binary.BigEndian.PutUint32(buf[RecordLengthSize:RecordLengthSize+RecordCRCSize], crc)

	return binSize, nil
}

func (r Record) BinaryLength() int {
	return RecordCRCSize + RecordOffsetSize + RecordTimestampSize + len(r.Data)
}

type IndexEntry struct {
	RelativeOffset Offset
	Position       int64
}

func NewIndexEntry(relativeOffset Offset, position int64) IndexEntry {
	return IndexEntry{
		RelativeOffset: relativeOffset,
		Position:       position,
	}
}

func (e IndexEntry) MarshalBinary(buf []byte) (int, error) {
	binary.BigEndian.PutUint32(buf[0:4], uint32(e.RelativeOffset))
	binary.BigEndian.PutUint32(buf[4:8], uint32(e.Position))

	return IndexEntrySize, nil
}

func RecordUnmarshalBinary(r io.Reader) (Record, error) {
	var length uint32
	err := binary.Read(r, binary.BigEndian, &length)
	if err != nil {
		return Record{}, err
	}

	var storedCRC uint32
	err = binary.Read(r, binary.BigEndian, &storedCRC)
	if err != nil {
		return Record{}, err
	}

	var offset int64
	err = binary.Read(r, binary.BigEndian, &offset)
	if err != nil {
		return Record{}, err
	}

	var timestamp int64
	err = binary.Read(r, binary.BigEndian, &timestamp)
	if err != nil {
		return Record{}, err
	}

	recordData := make([]byte, length-RecordCRCSize-RecordOffsetSize-RecordTimestampSize)
	_, err = io.ReadFull(r, recordData)
	if err != nil {
		return Record{}, err
	}

	crc := crc32.ChecksumIEEE(recordData)
	if crc != storedCRC {
		return Record{}, fmt.Errorf("record CRC mismatch: %d != %d", crc, storedCRC)
	}

	return Record{
		Offset:    offset,
		Timestamp: timestamp,
		Data:      recordData,
	}, nil
}

type RecordCache struct {
	records []Record
	mu      sync.RWMutex
}

func NewRecordCache(size int) *RecordCache {
	return &RecordCache{
		records: make([]Record, 0, size),
	}
}

func (c *RecordCache) Add(record Record) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = append(c.records, record)
}

// compareRecordByOffset compares a Record with a target offset for binary search.
// Extracted as a package-level function to allow compiler optimizations and reuse.
func compareRecordByOffset(curr Record, target Offset) int {
	return int(curr.Offset - target)
}

func (c *RecordCache) Get(offset Offset) (Record, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	idx, found := slices.BinarySearchFunc(c.records, offset, compareRecordByOffset)

	if found {
		return c.records[idx], true
	}

	return Record{}, false
}

func (c *RecordCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = c.records[:0]
}

// compareIndexEntryByOffset compares an IndexEntry with a target offset for binary search.
// Extracted as a package-level function to allow compiler optimizations and reuse.
func compareIndexEntryByOffset(curr IndexEntry, target Offset) int {
	return int(curr.RelativeOffset - target)
}

type IndexCache []IndexEntry

func (c IndexCache) Find(offset Offset) IndexEntry {
	if len(c) == 0 {
		return IndexEntry{}
	}
	idx, found := slices.BinarySearchFunc(c, offset, compareIndexEntryByOffset)

	if found {
		return c[idx]
	}

	if idx == 0 {
		// Should never happen - only considering it for logic completement
		panic(fmt.Errorf("offset %d is smaller than the smallest offset in the cache", offset))
	}

	// safe even if idx == len(c), meaning the offset is greater than all offsets in the cache
	// because when we try to read after the last entry we will EOF
	return c[idx-1]
}

type flushAwareWriter struct {
	w       io.Writer
	onFlush func()
}

func newFlushAwareWriter(w io.Writer, onFlush func()) *flushAwareWriter {
	return &flushAwareWriter{
		w:       w,
		onFlush: onFlush,
	}
}

func (w *flushAwareWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	if err != nil {
		return n, err
	}

	w.onFlush()
	return n, nil
}

type SealedSegment struct {
	baseOffset Offset

	logData   []byte // mmap'd .log file
	indexData []byte // mmap'd .index file

	logFile   *os.File
	indexFile *os.File
}

func NewSealedSegment(dir string, baseOffset Offset) (*SealedSegment, error) {
	logFilePath := fmt.Sprintf("%s/%020d.log", dir, baseOffset)
	indexFilePath := fmt.Sprintf("%s/%020d.index", dir, baseOffset)

	logFile, err := os.OpenFile(logFilePath, os.O_RDONLY, FilePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	indexFile, err := os.OpenFile(indexFilePath, os.O_RDONLY, FilePermissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	logStat, err := logFile.Stat()
	if err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, fmt.Errorf("stat log file: %w", err)
	}

	indexStat, err := indexFile.Stat()
	if err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, fmt.Errorf("stat index file: %w", err)
	}

	logData, err := syscall.Mmap(
		int(logFile.Fd()),
		0,                   // offset
		int(logStat.Size()), // length
		syscall.PROT_READ,   // read-only
		syscall.MAP_SHARED,  // shared mapping
	)
	if err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, fmt.Errorf("mmap log file: %w", err)
	}

	indexData, err := syscall.Mmap(
		int(indexFile.Fd()),
		0,                     // offset
		int(indexStat.Size()), // length
		syscall.PROT_READ,     // read-only
		syscall.MAP_SHARED,    // shared mapping
	)
	if err != nil {
		if logData != nil {
			_ = syscall.Munmap(logData)
		}
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, fmt.Errorf("mmap index file: %w", err)
	}

	// Give hints to OS about access patterns (Linux-specific optimization)
	adviseMemoryAccessPatterns(logData, indexData)

	return &SealedSegment{
		baseOffset: baseOffset,
		logData:    logData,
		indexData:  indexData,
		logFile:    logFile,
		indexFile:  indexFile,
	}, nil
}

func (s *SealedSegment) BaseOffset() Offset {
	return s.baseOffset
}

func (s *SealedSegment) Read(offset Offset) ([]byte, error) {
	return nil, nil
}

func (s *SealedSegment) findLastRecord() (Record, error) {
	lastIndexPosition := len(s.indexData) - IndexEntrySize
	lastIndexEntry := s.indexData[lastIndexPosition : lastIndexPosition+IndexEntrySize]

	// lastRelativeOffset := int64(binary.BigEndian.Uint32(lastIndexEntry[0:4]))
	_ = int64(binary.BigEndian.Uint32(lastIndexEntry[0:4]))
	lastPosition := int64(binary.BigEndian.Uint32(lastIndexEntry[4:8]))

	reader := bytes.NewBuffer(s.logData[lastPosition:])
	record, err := RecordUnmarshalBinary(reader)
	if err != nil {
		return Record{}, fmt.Errorf("failed to unmarshal last indexed record: %w", err)
	}

	for {
		var recordLength uint32
		err := binary.Read(reader, binary.BigEndian, &recordLength)
		if err != nil {
			if err == io.EOF {
				return record, nil
			}
			return record, fmt.Errorf("failed to read record length: %w", err)
		}

		if recordLength == 0 {
			return record, nil
		}

		// _ = reader.Next(int(recordLength))
		var recordCRC uint32
		err = binary.Read(reader, binary.BigEndian, &recordCRC)
		if err != nil {
			return Record{}, fmt.Errorf("failed to read record CRC: %w", err)
		}

		var recordOffset int64
		err = binary.Read(reader, binary.BigEndian, &recordOffset)
		if err != nil {
			return Record{}, fmt.Errorf("failed to read record offset: %w", err)
		}

		var recordTimestamp int64
		err = binary.Read(reader, binary.BigEndian, &recordTimestamp)
		if err != nil {
			return Record{}, fmt.Errorf("failed to read record timestamp: %w", err)
		}

		recordData := make([]byte, recordLength-RecordCRCSize-RecordOffsetSize-RecordTimestampSize)
		_, err = io.ReadFull(reader, recordData)
		if err != nil {
			return Record{}, fmt.Errorf("failed to read record data: %w", err)
		}

		crc := crc32.ChecksumIEEE(recordData)
		if crc != recordCRC {
			return Record{}, fmt.Errorf("record CRC mismatch: %d != %d", crc, recordCRC)
		}

		record = Record{
			Offset:    recordOffset,
			Timestamp: recordTimestamp,
			Data:      recordData,
		}
	}
}

/*
TODO:
- Find the tail of the log so we use that as the base offset for the next active segment
- Implement Read method
*/
