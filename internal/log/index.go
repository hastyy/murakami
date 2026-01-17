package log

import "slices"

const (
	// Size of an index entry in bytes: [4 bytes: relative offset] [4 bytes: position].
	indexEntrySize = 8
)

// indexEntry represents an entry in the index.
// It contains the relative offset of the record in the log file
// and the byte position of the record in the log file.
// Entries are sorted in ascending order by relative offset and the index can be sparse.
// We use the offset to find the closest byte position to the target record in the log file (using binary search).
type indexEntry struct {
	RelativeOffset Offset
	Position       int64
}

// newIndexEntry creates a new index entry.
func newIndexEntry(relativeOffset Offset, position int64) indexEntry {
	return indexEntry{
		RelativeOffset: relativeOffset,
		Position:       position,
	}
}

// indexCache is an in-memory cache of index entries.
// It should hold all the index entries for the active segment.
type indexCache []indexEntry

// newIndexCache creates a new index cache.
func newIndexCache(size int) indexCache {
	return make(indexCache, 0, size)
}

// Find finds the index entry for the given offset or the closest smaller one.
// If the exact index entry is found, it returns true. Otherwise, it returns false.
func (c indexCache) Find(offset Offset) (indexEntry, bool) {
	if len(c) == 0 {
		return indexEntry{}, false
	}

	idx, found := slices.BinarySearchFunc(c, offset, compareIndexEntryByOffset)
	if found {
		return c[idx], true
	}

	return c[idx-1], false
}

// compareIndexEntryByOffset compares an IndexEntry with a target offset for binary search.
func compareIndexEntryByOffset(curr indexEntry, target Offset) int {
	return int(curr.RelativeOffset - target)
}
