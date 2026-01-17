package log

import "errors"

// readSegment is a read-only representation of a segment of the log.
type readSegment interface {
	// Reads a record from the segment at the given offset.
	Read(Offset) (Record, error)

	// Returns the base offset of the segment.
	BaseOffset() Offset

	// Returns the number of records in the segment.
	Length() int

	// Closes the segment.
	Close() error
}

// activeSegment is a read/write representation of the latest segment of the log.
// A log will only contain one active segment at a time, which is always the last one in the segments slice.
// Each time we (re)start the log, a new active segment is created and all other segments that might already exist
// are considered sealed even if they haven't been explicitly closed (e.g. after a crash).
type activeSegment struct{}

func newActiveSegment(logDir string, baseOffset Offset, cfg Config) (*activeSegment, error) {
	return nil, errors.New("not implemented")
}

func (s *activeSegment) Append(data []byte) (Offset, error) {
	return 0, errors.New("not implemented")
}

func (s *activeSegment) Read(offset Offset) (Record, error) {
	return Record{}, errors.New("not implemented")
}

func (s *activeSegment) BaseOffset() Offset {
	panic("sealedSegment.BaseOffset() not implemented")
}

func (s *activeSegment) Length() int {
	panic("sealedSegment.Length() not implemented")
}

// Size returns the size of the segment in bytes.
func (s *activeSegment) Size() int {
	panic("sealedSegment.Size() not implemented")
}

func (s *activeSegment) Sync() error {
	return errors.New("not implemented")
}

func (s *activeSegment) Close() error {
	return errors.New("not implemented")
}

// sealedSegment is a read-only representation of a sealed segment of the log.
// Sealed segments are immutable and can be shared between multiple readers.
// A segment becomes sealed after explicitly closing an active segment (be it because of a roll or shutdown),
// or when we restart the log after a crash.
type sealedSegment struct{}

func newSealedSegment(logDir string, baseOffset Offset) (*sealedSegment, error) {
	return nil, errors.New("not implemented")
}

func (s *sealedSegment) Read(offset Offset) (Record, error) {
	return Record{}, errors.New("not implemented")
}

func (s *sealedSegment) BaseOffset() Offset {
	panic("sealedSegment.BaseOffset() not implemented")
}

func (s *sealedSegment) Length() int {
	panic("sealedSegment.Length() not implemented")
}

func (s *sealedSegment) Close() error {
	return errors.New("not implemented")
}
