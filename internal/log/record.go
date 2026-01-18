package log

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"slices"
	"sync"
)

// Record represents a record in the log.
type Record struct {
	Offset    Offset
	Timestamp int64
	Data      []byte
}

// newRecord creates a new record.
func newRecord(offset Offset, timestamp int64, data []byte) Record {
	return Record{
		Offset:    offset,
		Timestamp: timestamp,
		Data:      data,
	}
}

// encodeRecord encodes a record to its binary format and stores it in the provided buffer.
func encodeRecord(r Record, buf []byte) (int, error) {
	recordBinarySize := recordCRCSize + recordOffsetSize + recordTimestampSize + len(r.Data)
	totalBinarySize := recordLengthSize + recordBinarySize

	if len(buf) < totalBinarySize {
		return 0, io.ErrShortBuffer
	}

	// Write the record size first (length prefix pattern).
	binary.BigEndian.PutUint32(buf[0:recordLengthSize], uint32(recordBinarySize))

	// Now write the actual record into the buffer, skipping the CRC position for now.
	pos := recordLengthSize + recordCRCSize                                           // initial position after the length prefix and CRC
	binary.BigEndian.PutUint64(buf[pos:pos+recordOffsetSize], uint64(r.Offset))       // write offset
	pos += recordOffsetSize                                                           // advance position
	binary.BigEndian.PutUint64(buf[pos:pos+recordTimestampSize], uint64(r.Timestamp)) // write timestamp
	pos += recordTimestampSize                                                        // advance position
	copy(buf[pos:], r.Data)                                                           // write data

	// Now compute the CRC over the binary fields it covers (offset [bytes 8 - 15], timestamp [bytes 16 - 23], data [bytes 24 - totalBinarySize]).
	crc := crc32.ChecksumIEEE(buf[recordLengthSize+recordCRCSize : totalBinarySize]) // need to specify :totalBinarySize to cap the []byte slice we pass to CRC otherwise we might give it the remainder of the buffer we're not interested in.
	binary.BigEndian.PutUint32(buf[recordLengthSize:recordLengthSize+recordCRCSize], crc)

	return totalBinarySize, nil
}

// recordCacheEntry is a record and its position in the cache.
type recordCacheEntry struct {
	record   Record
	position int64
}

// recordCache is a cache of records.
// It should hold the latest appended records of the active segment within the current flushing window.
type recordCache struct {
	records []recordCacheEntry
	mu      sync.RWMutex
}

// newRecordCache creates a new record cache.
func newRecordCache(size int) *recordCache {
	return &recordCache{
		records: make([]recordCacheEntry, 0, size),
	}
}

// Add adds a record to the cache.
func (c *recordCache) Add(record Record, position int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = append(c.records, recordCacheEntry{record: record, position: position})
}

// Clear clears the cache.
// Keeps the allocated capacity.
func (c *recordCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = c.records[:0]
}

// Get returns a record from the cache by offset.
// If the record is not found, it returns false.
func (c *recordCache) Get(offset Offset) (recordCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	idx, found := slices.BinarySearchFunc(c.records, offset, compareRecordCacheEntryByOffset)
	if !found {
		return recordCacheEntry{}, false
	}
	return c.records[idx], true
}

// compareRecordCacheEntryByOffset compares a recordCacheEntry with a target offset for binary search.
func compareRecordCacheEntryByOffset(curr recordCacheEntry, target Offset) int {
	return int(curr.record.Offset - target)
}
