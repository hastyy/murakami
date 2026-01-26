package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// recoverLastSegment performs crash recovery on the last segment of the log.
// It validates records by checking their CRC, truncates at the first corruption,
// and rebuilds the index file. If the segment is empty after recovery, it deletes both files.
func recoverLastSegment(dir string, baseOffset Offset, cfg Config) error {
	logPath := logFilePath(dir, baseOffset)
	indexPath := indexFilePath(dir, baseOffset)

	// Scan the log file to find the last valid record boundary.
	validBoundary, recordCount, err := scanLogFile(logPath, cfg)
	if err != nil {
		return fmt.Errorf("failed to scan log file: %w", err)
	}

	// If no valid records, delete both files.
	if recordCount == 0 {
		_ = os.Remove(logPath)
		_ = os.Remove(indexPath)
		return nil
	}

	// Truncate the log file to the valid boundary.
	if err := os.Truncate(logPath, validBoundary); err != nil {
		return fmt.Errorf("failed to truncate log file: %w", err)
	}

	// Delete the existing index file entirely.
	_ = os.Remove(indexPath)

	// Rebuild the index by scanning the (now clean) log file.
	if err := rebuildIndex(logPath, indexPath, cfg); err != nil {
		return fmt.Errorf("failed to rebuild index: %w", err)
	}

	return nil
}

// scanLogFile opens a log file, scans it for valid records, and returns the boundary and count.
func scanLogFile(logPath string, cfg Config) (int64, int, error) {
	logFile, err := os.OpenFile(logPath, os.O_RDONLY, filePermissions)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open log file for recovery: %w", err)
	}
	defer func() { _ = logFile.Close() }()

	return scanForValidBoundary(logFile, cfg)
}

// scanForValidBoundary scans the log file and returns the byte position of the last valid record boundary
// and the count of valid records found. It stops at the first corruption (bad CRC, incomplete record, or unexpected EOF).
func scanForValidBoundary(logFile *os.File, cfg Config) (int64, int, error) {
	reader := bufio.NewReaderSize(logFile, cfg.LogReaderBufferSize)

	var position int64
	var recordCount int

	// Maximum record size for buffer allocation.
	maxRecordSize := recordHeaderSize + cfg.MaxRecordDataSize
	recordBuffer := make([]byte, maxRecordSize)

	for {
		// Remember the position at the start of this record.
		recordStart := position

		// Read the record length.
		var recordLength uint32
		err := binary.Read(reader, binary.BigEndian, &recordLength)
		if err != nil {
			if err == io.EOF {
				// Clean EOF at record boundary - this is fine.
				break
			}
			// Incomplete length read - truncate here.
			break
		}
		position += recordLengthSize

		// Reading length 0 means we've reached the end of valid records
		// (pre-allocated zero'd bytes or truncated file).
		if recordLength == 0 {
			position = recordStart // Reset to start of this "record".
			break
		}

		// Sanity check on record length.
		if int(recordLength) > maxRecordSize-recordLengthSize {
			// Record length is too large, likely corruption.
			position = recordStart
			break
		}

		// Read the record CRC.
		var recordCRC uint32
		err = binary.Read(reader, binary.BigEndian, &recordCRC)
		if err != nil {
			// Incomplete CRC read - truncate at record start.
			position = recordStart
			break
		}
		position += recordCRCSize

		// Read the remaining record bytes (offset + timestamp + data).
		remainingSize := int64(recordLength) - recordCRCSize
		if remainingSize <= 0 {
			// Invalid remaining size.
			position = recordStart
			break
		}

		_, err = io.ReadFull(reader, recordBuffer[:int(remainingSize)])
		if err != nil {
			// Incomplete record read - truncate at record start.
			position = recordStart
			break
		}

		// Verify CRC over the record content (offset + timestamp + data).
		crc := crc32.ChecksumIEEE(recordBuffer[:int(remainingSize)])
		if crc != recordCRC {
			// CRC mismatch - truncate at record start.
			position = recordStart
			break
		}

		// Record is valid.
		position += int64(remainingSize)
		recordCount++
	}

	return position, recordCount, nil
}

// rebuildIndex rebuilds the index file by scanning the log file.
// It uses the same indexing interval logic as activeSegment.shouldIndex().
func rebuildIndex(logPath, indexPath string, cfg Config) error {
	// Open the log file for reading.
	logFile, err := os.OpenFile(logPath, os.O_RDONLY, filePermissions)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer func() { _ = logFile.Close() }()

	// Create the index file.
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePermissions)
	if err != nil {
		return fmt.Errorf("failed to create index file: %w", err)
	}
	defer func() { _ = indexFile.Close() }()

	reader := bufio.NewReaderSize(logFile, cfg.LogReaderBufferSize)
	writer := bufio.NewWriterSize(indexFile, cfg.IndexWriterBufferSize)

	var position int64
	lastIndexedPosition := -int64(cfg.IndexIntervalBytes) // Force first record to be indexed.
	indexBuffer := make([]byte, indexEntrySize)

	for {
		// Remember the position at the start of this record.
		recordStart := position

		// Read the record length.
		var recordLength uint32
		err := binary.Read(reader, binary.BigEndian, &recordLength)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read record length: %w", err)
		}
		position += recordLengthSize

		// End of valid records.
		if recordLength == 0 {
			break
		}

		// Skip the CRC.
		position += recordCRCSize
		_, err = reader.Discard(recordCRCSize)
		if err != nil {
			return fmt.Errorf("failed to skip CRC: %w", err)
		}

		// Read the offset.
		var recordOffset uint64
		err = binary.Read(reader, binary.BigEndian, &recordOffset)
		if err != nil {
			return fmt.Errorf("failed to read record offset: %w", err)
		}
		position += recordOffsetSize

		// Check if we should index this record.
		if recordStart == 0 || (recordStart-lastIndexedPosition >= int64(cfg.IndexIntervalBytes)) {
			entry := newIndexEntry(int64(recordOffset), recordStart)
			_, err := encodeIndexEntry(entry, indexBuffer)
			if err != nil {
				return fmt.Errorf("failed to encode index entry: %w", err)
			}

			_, err = writer.Write(indexBuffer)
			if err != nil {
				return fmt.Errorf("failed to write index entry: %w", err)
			}

			lastIndexedPosition = recordStart
		}

		// Skip the remaining record bytes (timestamp + data).
		// recordDataSize returns just the data portion, so we add timestamp size.
		remainingSize := recordTimestampSize + recordDataSize(int64(recordLength))
		_, err = reader.Discard(int(remainingSize))
		if err != nil {
			return fmt.Errorf("failed to skip remaining record bytes: %w", err)
		}
		position += int64(remainingSize)
	}

	// Flush and sync.
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush index writer: %w", err)
	}

	if err := indexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file: %w", err)
	}

	return nil
}
