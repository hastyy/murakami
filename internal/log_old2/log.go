package log_old2

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/hastyy/murakami/internal/unit"
)

type Offset = int64

const (
	//
	directoryPermissions = 0700
	filePermissions      = 0600

	//
	recordLengthSize    = 4
	recordCRCSize       = 4
	recordOffsetSize    = 8
	recordTimestampSize = 8

	//
	indexEntrySize = 8

	//
	minSegSliceCap = 16
)

type Config struct {
	MaxSegmentSize     int64
	MaxRecordDataSize  int64
	IndexIntervalBytes int64

	LogWriterBufferSize   int
	IndexWriterBufferSize int
	LogReaderBufferSize   int

	RecordCacheSize int
	IndexCacheSize  int
}

type Log struct {
	cfg           Config // read-only
	dir           string // read-only
	segments      []readSegment
	activeSegment *activeSegment

	mu sync.RWMutex
}

func New(dataDir, name string) (*Log, error) {
	//
	logDir := fmt.Sprintf("%s/%s", dataDir, name)

	// TODO: Move to New() params.
	// We are setting a static configuration for now.
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

	// Get all existing .log file paths, sorted lexicographically.
	logFilePaths, err := filepath.Glob(filepath.Join(logDir, "*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to get log file paths: %w", err)
	}

	//
	segmentsCap := max(minSegSliceCap, len(logFilePaths)*2)
	segments := make([]readSegment, 0, segmentsCap)

	//
	for _, logFilePath := range logFilePaths {
		//
		filename := filepath.Base(logFilePath)
		baseOffsetStr := strings.TrimSuffix(filename, ".log")
		baseOffset, err := strconv.ParseInt(baseOffsetStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid base offset in filename %s: %w", filename, err)
		}

		//
		sealedSegment, err := newSealedSegment(logDir, baseOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to create sealed segment %s: %w", baseOffsetStr, err)
		}

		segments = append(segments, sealedSegment)
	}

	// TODO: perform clean shutdown check
	// There's probably and edge case if len(segments) == 0 at this point. If that's the case then we shouldn't have a marker
	// because most likely this log hasn't existed before. If we have a marker and no segments it means there was a clean shutdown of a completely
	// empty log. In that case we should just remove the marker.
	// Think the 'problematic' case is really if we have segments and no marker because that means there was no clean shutdown.
	// I think in that case it's enough to perform repair on the last segment?

	//
	var activeSegmentBaseOffset Offset
	if len(segments) > 0 {
		lastSealedSegment := segments[len(segments)-1].(*sealedSegment)
		activeSegmentBaseOffset = lastSealedSegment.BaseOffset() + lastSealedSegment.Length()
	}

	//
	activeSegment, err := newActiveSegment(logDir, activeSegmentBaseOffset, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create active segment: %w", err)
	}

	//
	segments = append(segments, activeSegment)

	return &Log{
		cfg:           cfg,
		dir:           logDir,
		segments:      segments,
		activeSegment: activeSegment,
	}, nil
}

func (l *Log) Append(data []byte) (Offset, error) {
	//
	recordSize := recordLengthSize + recordCRCSize + recordOffsetSize + recordTimestampSize + int64(len(data))
	if len(data) > int(l.cfg.MaxRecordDataSize) {
		return 0, fmt.Errorf("data size %d is greater than the max record data size %d", len(data), l.cfg.MaxRecordDataSize)
	}

	// Each call to Append() must be fully serialized.
	l.mu.Lock()
	defer l.mu.Unlock()

	//
	if l.activeSegment.Length()+recordSize >= l.cfg.MaxSegmentSize {
		err := l.rollActiveSegment()
		if err != nil {
			return 0, fmt.Errorf("failed to roll active segment: %w", err)
		}
	}

	//
	relativeOffset, err := l.activeSegment.Append(data)
	if err != nil {
		return 0, fmt.Errorf("failed to append to active segment: %w", err)
	}

	// Return the absolute offset of the record.
	return l.activeSegment.BaseOffset() + relativeOffset, nil
}

func (l *Log) rollActiveSegment() error {
	// First we close the current active segment so it becomes sealed
	// i.e. turned into it's stable, read-only version.
	// This also flushes and buffers and syncs files to disk.
	_ = l.activeSegment.Close()

	// We now make the sealed segment counterpart of the segment we just closed
	// by using the same base offset.
	sealedSegment, err := newSealedSegment(l.dir, l.activeSegment.BaseOffset())
	if err != nil {
		return fmt.Errorf("failed to create sealed segment: %w", err)
	}

	// We replace the active segment at the tail of the segments slice with the new sealed segment.
	l.segments[len(l.segments)-1] = sealedSegment

	// We now use the length of the recently closed segment as the base offset for the new active segment.
	nextBaseOffset := l.activeSegment.Length() // potential bug here?

	// We create a new active segment.
	activeSegment, err := newActiveSegment(l.dir, nextBaseOffset, l.cfg)
	if err != nil {
		return fmt.Errorf("failed to create new active segment: %w", err)
	}

	// And finally append it to the tail of the segments slice
	// and also save the direct reference to it.
	l.segments = append(l.segments, activeSegment)
	l.activeSegment = activeSegment

	return nil
}

func (l *Log) Length() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.activeSegment.BaseOffset() + l.activeSegment.Length()
}

func (l *Log) Read(offset Offset) ([]byte, error) {
	// It is safe to access l.segments[0] because we know it has at least one segment.
	// We don't need to lock here because the segments slice is immutable and each segment base offset is immutable as well.
	firstSegment := l.segments[0]
	smallestOffset := firstSegment.BaseOffset()

	// Check if offset is within left range of the log.
	if offset < smallestOffset {
		return nil, fmt.Errorf("offset %d is smaller than the first offset %d", offset, smallestOffset)
	}

	// Needs to be read lock from here on because Appends might interfere with Reads.
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the segment that contains the offset.
	// If the offset is not found, the segment that is the closest to the left is returned. This is
	// safe because (idx=0, found=false) is not a possible case at this point because we've guaranteed offset is within the left range of the log.
	idx, found := slices.BinarySearchFunc(l.segments, offset, compareReadSegmentByOffset)

	var targetSegment readSegment
	if found {
		targetSegment = l.segments[idx]
	} else {
		// If the result of binary search was (idx=len(l.segments), found=false) then it is still possible
		// that the offset is greater than the last recorded offset.
		// But same thing if the result was (idx=len(l.segments)-1, found=true).
		targetSegment = l.segments[idx-1]
	}

	return targetSegment.Read(offset - targetSegment.BaseOffset())
}

func (l *Log) Last() ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.activeSegment.Last()
}

/*
func (l *Log) Read(offset Offset) (io.Reader, error) {
	// It is safe to access l.segments[0] because we know it has at least one segment.
	// We don't need to lock here because the segments slice is immutable and each segment base offset is immutable as well.
	firstSegment := l.segments[0]
	smallestOffset := firstSegment.BaseOffset()

	// Check if offset is within left range of the log.
	if offset < smallestOffset {
		return nil, fmt.Errorf("offset %d is smaller than the first offset %d", offset, smallestOffset)
	}

	// Needs to be read lock from here on because Appends might interfere with Reads.
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the segment that contains the offset.
	// If the offset is not found, the segment that is the closest to the left is returned. This is
	// safe because (idx=0, found=false) is not a possible case at this point because we've guaranteed offset is within the left range of the log.
	idx, found := slices.BinarySearchFunc(l.segments, offset, compareReadSegmentByOffset)

	var targetSegment readSegment
	var segmentIdx int
	if found {
		targetSegment = l.segments[idx]
		segmentIdx = idx
	} else {
		// If the result of binary search was (idx=len(l.segments), found=false) then it is still possible
		// that the offset is greater than the last recorded offset.
		// But same thing if the result was (idx=len(l.segments)-1, found=true).
		targetSegment = l.segments[idx-1]
		segmentIdx = idx - 1
	}

	segmentReader, err := targetSegment.Read(offset - targetSegment.BaseOffset())
	if err != nil {
		return nil, err
	}

	return newLogReader(l, segmentIdx, segmentReader), nil
}
*/

func (l *Log) Close() error {
	return fmt.Errorf("not implemented")
}

// compareReadSegmentByOffset compares a readSegment with a target offset for binary search.
func compareReadSegmentByOffset(curr readSegment, target Offset) int {
	return int(curr.BaseOffset() - target)
}

/*
type logReader struct {
	log        *Log
	segmentIdx int
	currReader io.Reader
}

func newLogReader(log *Log, segmentIdx int, segmentReader io.Reader) *logReader {
	return &logReader{
		log:        log,
		segmentIdx: segmentIdx,
		currReader: segmentReader,
	}
}

func (r *logReader) Read(p []byte) (int, error) {
	n, err := r.currReader.Read(p)
	if err != nil {
		if err == io.EOF {
			r.log.mu.RLock()
			if r.segmentIdx == len(r.log.segments)-1 {
				r.log.mu.RUnlock()
				return 0, io.EOF
			}
			nextSegment := r.log.segments[r.segmentIdx+1]
			r.log.mu.RUnlock()
			nextSegmentReader, err := nextSegment.Read(0)
			if err != nil {
				return 0, err
			}
			r.currReader = nextSegmentReader
			return r.Read(p)
		}
		return 0, err
	}
	return n, nil
}
*/
