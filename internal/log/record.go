package log

import (
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
