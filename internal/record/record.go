package record

import "fmt"

type Record struct {
	Timestamp Timestamp
	Value     []byte
}

type Timestamp struct {
	Millis int64
	SeqNum int
}

func (t Timestamp) String() string {
	return fmt.Sprintf("%d-%d", t.Millis, t.SeqNum)
}
