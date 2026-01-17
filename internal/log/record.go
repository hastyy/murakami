package log

// Record represents a record in the log.
type Record struct {
	Offset    Offset
	Timestamp int64
	Data      []byte
}
