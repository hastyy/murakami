package log_old2

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"slices"
	"sync"
)

// record is a record in the log.
// This is the disk representation of a record.
type record struct {
	Offset    Offset
	Timestamp int64
	Data      []byte
}

// newRecord creates a new record.
func newRecord(offset Offset, data []byte) record {
	return record{
		Offset:    offset,
		Timestamp: 0, // TODO: set timestamp
		Data:      data,
	}
}

// encodeRecord encodes a record into a buffer.
func encodeRecord(r record, buf []byte) (int, error) {
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

// recordCache is a cache of records.
// It should hold the latest appended records of the active segment
// within the current flushing window.
type recordCache struct {
	records   []record
	positions map[Offset]int64
	mu        sync.RWMutex
}

// newRecordCache creates a new record cache.
// It takes in a size which should be large enough to hold all records within the flushing window.
// This is to prevent reallocations and keep the performance and memory footprints predictable.
func newRecordCache(size int) *recordCache {
	return &recordCache{
		records:   make([]record, 0, size),
		positions: make(map[Offset]int64, size),
	}
}

// Add adds a record to the cache.
func (c *recordCache) Add(record record, position int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = append(c.records, record)
	c.positions[record.Offset] = position
}

// Clear clears the cache.
// Keeps the allocated capacity.
func (c *recordCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = c.records[:0]
	clear(c.positions)
}

// Get returns a record from the cache by offset.
// If the record is not found, it returns false.
func (c *recordCache) Get(offset Offset) (record, int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Since the cache is not sparse (i.e. all record within the flushing window are in the cache),
	// found = false just means the offset is not in range.
	// In other words, the offset is either smaller than the first record or greater than the last record.
	// We can guarantee the latter is not true if we validate this on the caller side before calling this method.
	idx, found := slices.BinarySearchFunc(c.records, offset, compareRecordByOffset)
	if !found {
		return record{}, 0, false
	}

	return c.records[idx], c.positions[offset], true
}

// compareRecordByOffset compares a Record with a target offset for binary search.
func compareRecordByOffset(curr record, target Offset) int {
	return int(curr.Offset - target)
}
