package store

import (
	"strconv"
	"time"

	"github.com/hastyy/murakami/internal/store/logtree"
)

type TimestampKeyGenerator func() logtree.Key

func (gen TimestampKeyGenerator) GetNext() logtree.Key {
	return gen()
}

func NewTimestampKeyGenerator() TimestampKeyGenerator {
	return func() logtree.Key {
		ts := time.Now().UnixMilli()
		return logtree.Key(strconv.FormatInt(ts, 10))
	}
}
