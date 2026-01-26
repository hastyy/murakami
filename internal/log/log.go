package log

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hastyy/murakami/internal/unit"
)

const (
	// Size of record fields in bytes.
	// These are the fields that are written to the log file.
	//
	// Record format on disk:
	// +--------+--------+--------+-----------+------+
	// | Length |  CRC   | Offset | Timestamp | Data |
	// | 4B     |  4B    |  8B    |    8B     | var  |
	// +--------+--------+--------+-----------+------+
	// Length = size of CRC + Offset + Timestamp + Data (excludes itself)
	// CRC = checksum over Offset + Timestamp + Data
	recordLengthSize    = 4
	recordCRCSize       = 4
	recordOffsetSize    = 8
	recordTimestampSize = 8

	// recordHeaderSize is the fixed overhead per record (Length + CRC + Offset + Timestamp).
	recordHeaderSize = recordLengthSize + recordCRCSize + recordOffsetSize + recordTimestampSize

	// Minimum capacity for the segments slice.
	minSegSliceCap = 16

	// Name of the clean shutdown marker file.
	cleanShutdownMarkerFile = ".clean_shutdown"
)

// recordDataSize returns the size of the data portion from a record's length field.
// The length field stores: CRC + Offset + Timestamp + Data, so we subtract the fixed parts.
func recordDataSize(recordLength int64) int64 {
	return recordLength - recordCRCSize - recordOffsetSize - recordTimestampSize
}

// shutdownMarker is written to the log directory on graceful close.
// Its presence indicates the log was closed cleanly and no recovery is needed.
type shutdownMarker struct {
	Timestamp    time.Time `json:"timestamp"`
	LastOffset   int64     `json:"last_offset"`
	SegmentCount int       `json:"segment_count"`
}

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

	// RetentionDuration is the maximum age of segments before they are eligible for cleanup.
	// Set to 0 to disable time-based retention.
	RetentionDuration time.Duration
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

// New takes the log directory and attempts to create a new log.
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

	// Check for clean shutdown marker.
	markerPath := filepath.Join(dir, cleanShutdownMarkerFile)
	markerExists := fileExists(markerPath)

	// If segments exist but no marker, we need to recover the last segment.
	if len(logFilePaths) > 0 && !markerExists {
		// Recover the last segment (it's the only one that could be corrupted).
		lastBaseOffset, err := parseBaseOffsetFromPath(logFilePaths[len(logFilePaths)-1])
		if err != nil {
			return nil, err
		}

		// Perform recovery on the last segment.
		err = recoverLastSegment(dir, lastBaseOffset, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to recover last segment: %w", err)
		}

		// Re-read the log file paths after recovery (the last segment might have been deleted if empty).
		logFilePaths, err = filepath.Glob(filepath.Join(dir, "*.log"))
		if err != nil {
			return nil, fmt.Errorf("failed to get log file paths after recovery: %w", err)
		}
	}

	// If marker exists, delete it (we'll write a new one on close).
	if markerExists {
		_ = os.Remove(markerPath)
	}

	// Initialize the segments slice.
	segmentsCap := max(minSegSliceCap, len(logFilePaths)*2)
	segments := make([]readSegment, 0, segmentsCap)

	// Each segment file corresponds to a sealed segment.
	for _, logFilePath := range logFilePaths {
		baseOffset, err := parseBaseOffsetFromPath(logFilePath)
		if err != nil {
			return nil, err
		}

		// Create the sealed segment.
		sealedSegment, err := newSealedSegment(dir, baseOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to create sealed segment %d: %w", baseOffset, err)
		}

		// Append the sealed segment to the segments slice.
		segments = append(segments, sealedSegment)
	}

	// We need to determine the base offset for the new active segment based on the last sealed segment.
	// If there are no sealed segments, then we use 0 as the base offset.
	var activeSegmentBaseOffset Offset
	if len(segments) > 0 {
		lastSealedSegment := segments[len(segments)-1]
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
	recordSize := recordHeaderSize + len(data)

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
// On graceful close, it writes a shutdown marker file to indicate clean shutdown.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var errs []error

	// Check if active segment is empty and capture segment info BEFORE closing anything.
	activeSegmentEmpty := l.activeSegment.Length() == 0
	activeSegmentBaseOffset := l.activeSegment.BaseOffset()

	// Calculate the last offset for the shutdown marker BEFORE closing segments.
	// We need this info while segments are still open.
	var lastOffset int64
	var segmentCount int
	if activeSegmentEmpty {
		// Active segment is empty and will be deleted.
		// The last offset comes from the previous sealed segment (if any).
		if len(l.segments) > 1 {
			lastSealedSeg := l.segments[len(l.segments)-2]
			lastOffset = lastSealedSeg.BaseOffset() + int64(lastSealedSeg.Length()) - 1
			segmentCount = len(l.segments) - 1
		} else {
			lastOffset = -1 // No records in log.
			segmentCount = 0
		}
	} else {
		// Active segment has records.
		lastOffset = l.activeSegment.BaseOffset() + int64(l.activeSegment.Length()) - 1
		segmentCount = len(l.segments)
	}

	// Close all sealed segments first.
	for i := 0; i < len(l.segments)-1; i++ {
		if err := l.segments[i].Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Handle the active segment.
	if activeSegmentEmpty {
		// Close and delete the empty segment files.
		if err := l.activeSegment.Close(); err != nil {
			errs = append(errs, err)
		}

		// Delete the empty segment files.
		logPath := logFilePath(l.dir, activeSegmentBaseOffset)
		indexPath := indexFilePath(l.dir, activeSegmentBaseOffset)
		_ = os.Remove(logPath)
		_ = os.Remove(indexPath)

		// Remove from segments slice.
		l.segments = l.segments[:len(l.segments)-1]
	} else {
		// Close normally (flush and sync).
		if err := l.activeSegment.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Write the clean shutdown marker.
	marker := shutdownMarker{
		Timestamp:    time.Now(),
		LastOffset:   lastOffset,
		SegmentCount: segmentCount,
	}

	if err := l.writeShutdownMarker(marker); err != nil {
		errs = append(errs, fmt.Errorf("failed to write shutdown marker: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// writeShutdownMarker writes the shutdown marker to the log directory.
func (l *Log) writeShutdownMarker(marker shutdownMarker) error {
	markerPath := filepath.Join(l.dir, cleanShutdownMarkerFile)

	// Create the marker file.
	file, err := os.OpenFile(markerPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePermissions)
	if err != nil {
		return fmt.Errorf("failed to create shutdown marker file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Write the marker as JSON.
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(marker); err != nil {
		return fmt.Errorf("failed to encode shutdown marker: %w", err)
	}

	// Sync the marker file to disk.
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync shutdown marker file: %w", err)
	}

	// Sync the directory to ensure the marker is visible.
	if err := syncDir(l.dir); err != nil {
		return fmt.Errorf("failed to sync directory: %w", err)
	}

	return nil
}

// RunRetentionCleanup deletes sealed segments older than RetentionDuration.
// It is the caller's responsibility to invoke this periodically if desired.
// Returns the number of segments deleted and any error encountered.
// If RetentionDuration is 0, this method returns immediately (retention is disabled).
func (l *Log) RunRetentionCleanup() (int, error) {
	// If retention is disabled, return early.
	if l.cfg.RetentionDuration == 0 {
		return 0, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	cutoff := time.Now().Add(-l.cfg.RetentionDuration)
	deleted := 0
	var errs []error

	// We only process sealed segments (everything except the last segment which is the active one).
	// We iterate from the beginning since older segments are at the front.
	// We need to be careful about modifying the slice while iterating.
	i := 0
	for i < len(l.segments)-1 {
		seg := l.segments[i]

		// Get the log file path to check its modification time.
		logPath := logFilePath(l.dir, seg.BaseOffset())

		info, err := os.Stat(logPath)
		if err != nil {
			// If we can't stat the file, skip this segment.
			errs = append(errs, fmt.Errorf("failed to stat segment %d: %w", seg.BaseOffset(), err))
			i++
			continue
		}

		// Check if the segment is older than the retention duration.
		if info.ModTime().Before(cutoff) {
			// Close the segment.
			if err := seg.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close segment %d: %w", seg.BaseOffset(), err))
				i++
				continue
			}

			// Delete the log and index files.
			indexPath := indexFilePath(l.dir, seg.BaseOffset())
			if err := os.Remove(logPath); err != nil {
				errs = append(errs, fmt.Errorf("failed to delete log file %s: %w", logPath, err))
			}
			if err := os.Remove(indexPath); err != nil {
				errs = append(errs, fmt.Errorf("failed to delete index file %s: %w", indexPath, err))
			}

			// Remove from segments slice.
			l.segments = append(l.segments[:i], l.segments[i+1:]...)
			deleted++
			// Don't increment i since we removed an element.
		} else {
			// Once we find a segment that's not expired, we can stop.
			// Segments are ordered by time (older segments have smaller base offsets).
			break
		}
	}

	if len(errs) > 0 {
		return deleted, errors.Join(errs...)
	}

	return deleted, nil
}

// syncDir syncs a directory to ensure all directory entries are persisted.
func syncDir(dirPath string) error {
	dir, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()

	return dir.Sync()
}

// fileExists checks if a file exists at the given path.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// parseBaseOffsetFromPath extracts the base offset from a log file path.
// The filename is expected to be in the format "00000000000000000000.log".
func parseBaseOffsetFromPath(logFilePath string) (Offset, error) {
	filename := filepath.Base(logFilePath)
	baseOffsetStr := strings.TrimSuffix(filename, ".log")
	baseOffset, err := strconv.ParseInt(baseOffsetStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid base offset in filename %s: %w", filename, err)
	}
	return baseOffset, nil
}

// rollActiveSegment rolls the active segment by closing it and creating a new one.
func (l *Log) rollActiveSegment() error {
	// First we close the current active segment so it becomes sealed
	// i.e. turned into it's stable, read-only version.
	// This also flushes and buffers and syncs files to disk.
	err := l.activeSegment.Close()
	if err != nil {
		return fmt.Errorf("failed to close active segment: %w", err)
	}

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
