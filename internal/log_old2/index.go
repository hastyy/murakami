package log_old2

import (
	"encoding/binary"
	"io"
	"slices"
)

type indexEntry struct {
	RelativeOffset Offset
	Position       int64
}

func newIndexEntry(relativeOffset Offset, position int64) indexEntry {
	return indexEntry{
		RelativeOffset: relativeOffset,
		Position:       position,
	}
}

func encodeIndexEntry(entry indexEntry, buf []byte) (int, error) {
	if len(buf) < indexEntrySize {
		return 0, io.ErrShortBuffer
	}

	binary.BigEndian.PutUint32(buf[0:4], uint32(entry.RelativeOffset))
	binary.BigEndian.PutUint32(buf[4:8], uint32(entry.Position))

	return indexEntrySize, nil
}

func decodeIndexEntry(bytes []byte) (indexEntry, error) {
	if len(bytes) < indexEntrySize {
		return indexEntry{}, io.ErrShortBuffer
	}

	relativeOffset := int64(binary.BigEndian.Uint32(bytes[0:4]))
	position := int64(binary.BigEndian.Uint32(bytes[4:8]))

	return indexEntry{
		RelativeOffset: relativeOffset,
		Position:       position,
	}, nil
}

type indexCache []indexEntry

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
