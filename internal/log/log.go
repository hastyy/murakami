package log

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/hastyy/murakami/internal/unit"
)

const (
	// Size of record fields in bytes.
	// These are the fields that are written to the log file.
	recordLengthSize    = 4
	recordCRCSize       = 4
	recordOffsetSize    = 8
	recordTimestampSize = 8

	// Minimum capacity for the segments slice.
	minSegSliceCap = 16
)

// Offset is the offset of a record in the log.
type Offset = int64

// Config is the configuration for the log.
type Config struct {
	// Maximum size of a segment in bytes.
	MaxSegmentSize int
	// Maximum size of a record in bytes (excluding offset, timestamp and additional metadata).
	MaxRecordDataSize int
	// Interval in bytes at which index entries are written.
	IndexIntervalBytes int

	// Buffer size for the log writer (buffered IO).
	LogWriterBufferSize int

	// Buffer size for the index writer (buffered IO).
	IndexWriterBufferSize int

	// Buffer size for the log reader (buffered IO).
	LogReaderBufferSize int

	// Size of the record cache.
	RecordCacheSize int

	// Size of the index cache.
	IndexCacheSize int
}

// Log is an append-only, totally-ordered sequence of records ordered by time (append order).
// Records are appended to the end of the log, and reads proceed left-to-right.
// Each entry is assigned a unique sequential log entry number (the offset).
// We represent the log as a sequence of segments, each being its own self-contained sequence of records.
// This technique allows us to perform faster recovery, eliminate old chunks of data (from the left side),
// more efficient reading and makes the log easier to replicate.
type Log struct {
	dir string // read-only
	cfg Config // read-only

	segments      []readSegment
	activeSegment *activeSegment

	mu sync.RWMutex
}

// New takes the log directory and attemps to create a new log.
// It initializes all log state, guaranteeing that it at least contains an active segment.
// If the log already existed (i.e. if there was at least one file in the directory) then it will attempt to recover the log state.
// If necessary, recovery and repair will be performed on the found last segment.
func New(dir string) (*Log, error) {
	// TODO: Move to New() params. We are setting a static configuration for now.
	cfg := Config{
		MaxSegmentSize:        1. * unit.GiB,
		MaxRecordDataSize:     1 * unit.KiB,
		IndexIntervalBytes:    4 * unit.KiB,
		LogWriterBufferSize:   64 * unit.KiB,
		IndexWriterBufferSize: 16 * unit.KiB,
		LogReaderBufferSize:   16 * unit.KiB,
		RecordCacheSize:       256,
		IndexCacheSize:        500_000,
	}

	// Get all existing .log file paths, sorted lexicographically.
	logFilePaths, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to get log file paths: %w", err)
	}

	// Initialize the segments slice.
	segmentsCap := max(minSegSliceCap, len(logFilePaths)*2)
	segments := make([]readSegment, 0, segmentsCap)

	// Each segment file corresponds to a sealed segment.
	for _, logFilePath := range logFilePaths {
		// Get the base offset from the filename.
		filename := filepath.Base(logFilePath)
		baseOffsetStr := strings.TrimSuffix(filename, ".log")
		baseOffset, err := strconv.ParseInt(baseOffsetStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid base offset in filename %s: %w", filename, err)
		}

		// Create the sealed segment.
		sealedSegment, err := newSealedSegment(dir, baseOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to create sealed segment %s: %w", baseOffsetStr, err)
		}

		// Append the sealed segment to the segments slice.
		segments = append(segments, sealedSegment)
	}

	// TODO: perform clean shutdown check
	// There's probably and edge case if len(segments) == 0 at this point. If that's the case then we shouldn't have a marker
	// because most likely this log hasn't existed before. If we have a marker and no segments it means there was a clean shutdown of a compeltely
	// empty log. In that case we should just remove the marker.
	// Think the 'problematic' case is really if we have segments and no marker because that means there was no clean shutdown.
	// I think in that case it's enough to perform repair on the last segment?

	// We need to determine the base offset for the new active segment based on the last sealed segment.
	// If there are no sealed segments, then we use 0 as the base offset.
	var activeSegmentBaseOffset Offset
	if len(segments) > 0 {
		lastSealedSegment := segments[len(segments)-1].(*sealedSegment)
		activeSegmentBaseOffset = lastSealedSegment.BaseOffset() + int64(lastSealedSegment.Length())
	}

	// Create the active segment.
	activeSegment, err := newActiveSegment(dir, activeSegmentBaseOffset, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create active segment: %w", err)
	}

	// Also append the active segment to the segments slice.
	segments = append(segments, activeSegment)

	return &Log{
		dir:           dir,
		cfg:           cfg,
		segments:      segments,
		activeSegment: activeSegment,
	}, nil
}

// Append appends a record to the log.
// The record is appended to the active segment and the offset of the record is returned.
// When Append successfully returns it just means the record was accepted, not that it was made durable yet.
// To guarantee durability, the caller must call Sync() after Append().
func (l *Log) Append(data []byte) (Offset, error) {
	return l.AppendWithTimestamp(data, 0)
}

// AppendWithTimestamp is equivalent to Append() but it also sets the timestamp of the record.
func (l *Log) AppendWithTimestamp(data []byte, unixTimestamp int64) (Offset, error) {
	// Check if the data size is within the allowed limits.
	if len(data) > l.cfg.MaxRecordDataSize {
		return 0, fmt.Errorf("data size %d is greater than the max record data size %d", len(data), l.cfg.MaxRecordDataSize)
	}

	// Calculate the size of the record on disk.
	recordSize := recordLengthSize + recordCRCSize + recordOffsetSize + recordTimestampSize + len(data)

	// Each call to Append() must be fully serialized.
	l.mu.Lock()
	defer l.mu.Unlock()

	// Before actually appending the record, we need to check if the current active segment can still fit the record.
	// If it can't, we need to roll the active segment even if it's not full yet, leaving some wasted space at the end
	// of the segment file in case we have preallocated the full segment file in advance.
	if l.activeSegment.Size()+recordSize >= l.cfg.MaxSegmentSize {
		err := l.rollActiveSegment()
		if err != nil {
			return 0, fmt.Errorf("failed to roll active segment: %w", err)
		}
	}

	// Append the record to the active segment.
	relativeOffset, err := l.activeSegment.Append(data, unixTimestamp)
	if err != nil {
		return 0, fmt.Errorf("failed to append to active segment: %w", err)
	}

	// Return the absolute offset of the record.
	return l.activeSegment.BaseOffset() + relativeOffset, nil
}

// Read reads a record from the log at the given offset.
func (l *Log) Read(offset Offset) (Record, error) {
	// It is safe to access l.segments[0] because we know it has at least one segment.
	firstSegment := l.segments[0]
	smallestOffset := firstSegment.BaseOffset()

	// Check if offset is within left range of the log.
	if offset < smallestOffset {
		return Record{}, fmt.Errorf("offset %d is smaller than the first offset %d", offset, smallestOffset)
	}

	// Needs to be read lock from here on because Appends might interfere with Reads.
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the segment that contains the offset.
	// If the offset is not found, the segment that is the closest to the left is returned.
	// This is safe because (idx=0, found=false) is not a possible case at this point because
	// we've guaranteed offset is within the left range of the log.
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

// Length returns the number of records in the segment.
func (l *Log) Length() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Needs to be read lock from here on because segment might roll between getting the base offset and the length.
	return int(l.activeSegment.BaseOffset()) + l.activeSegment.Length()
}

// Sync flushes and syncs the newest records to disk.
func (l *Log) Sync() error {
	return l.activeSegment.Sync()
}

// Close closes the log by closing all the segments.
// It returns an error if any of the segments failed to close.
// The last segment to be closed is the active segment. Closing it will also flush and sync the records to disk.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	errs := make([]error, 0, len(l.segments))
	for _, segment := range l.segments {
		err := segment.Close()
		errs = append(errs, err)
	}

	return fmt.Errorf("failed to close segments: %w", errors.Join(errs...))
}

// rollActiveSegment rolls the active segment by closing it and creating a new one.
func (l *Log) rollActiveSegment() error {
	// First we close the current active segment so it becomes sealed
	// i.e. turned into it's stable, read-only version.
	// This also flushes and buffers and syncs files to disk.
	l.activeSegment.Close()

	// Get the base offset of the recently closed segment to open the new sealed segment.
	currBaseOffset := l.activeSegment.BaseOffset()

	// We now use the length of the recently closed segment as the base offset for the new active segment.
	nextBaseOffset := currBaseOffset + int64(l.activeSegment.Length())

	// We now make the sealed segment counterpart of the segment we just closed
	// by using the same base offset.
	sealedSegment, err := newSealedSegment(l.dir, currBaseOffset)
	if err != nil {
		return fmt.Errorf("failed to create sealed segment: %w", err)
	}

	// We replace the active segment at the tail of the segments slice with the new sealed segment.
	l.segments[len(l.segments)-1] = sealedSegment

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

// compareReadSegmentByOffset compares a readSegment with a target offset for binary search.
func compareReadSegmentByOffset(curr readSegment, target Offset) int {
	return int(curr.BaseOffset() - target)
}
