package protocol

import (
	"time"

	"github.com/hastyy/murakami/internal/util"
)

type Config struct {
	// Dependencies
	// The available pool of []byte to use for reading command values.
	BufPool BufferPool

	// Config values
	// The maximum length of a stream name. It defaults to 256 bytes.
	MaxStreamNameLength int
	// The maximum number of records per APPEND request.
	MaxRecordsPerAppend int
	// The maximum size of a record in bytes. It is advised to allign this with server.cfg.ConnectionReadBufferSize.
	// TODO: Remove for now? Is redundant with server.cfg.ConnectionReadBufferSize as that is what's used to init the buffers in the BufferPool. OR maybe remove the server config and use this instead?
	MaxRecordSize int
	// The maximum aggregate payload size for the entire APPEND request.
	MaxAppendPayloadSize int
	// The default number of records to read in a READ request.
	MaxReadCount int
	// The maximum block duration for a READ request.
	MaxReadBlock time.Duration
}

var DefaultConfig = Config{
	MaxStreamNameLength:  256 * util.Byte,
	MaxRecordsPerAppend:  1_000,
	MaxRecordSize:        1 * util.KiB,
	MaxAppendPayloadSize: 1 * util.MiB,
	MaxReadCount:         1_000,
	MaxReadBlock:         10 * time.Second,
}

// CombineWith takes the values from the other config and combines them with the values from the current config.
// Specifically, it fills the gaps on the current config (unset values) with the corresponding values from the other config (if it has them).
func (cfg Config) CombineWith(other Config) Config {
	if cfg.BufPool == nil {
		cfg.BufPool = other.BufPool
	}
	if cfg.MaxStreamNameLength == 0 {
		cfg.MaxStreamNameLength = other.MaxStreamNameLength
	}
	if cfg.MaxRecordsPerAppend == 0 {
		cfg.MaxRecordsPerAppend = other.MaxRecordsPerAppend
	}
	if cfg.MaxRecordSize == 0 {
		cfg.MaxRecordSize = other.MaxRecordSize
	}
	if cfg.MaxAppendPayloadSize == 0 {
		cfg.MaxAppendPayloadSize = other.MaxAppendPayloadSize
	}
	if cfg.MaxReadCount == 0 {
		cfg.MaxReadCount = other.MaxReadCount
	}
	if cfg.MaxReadBlock == 0 {
		cfg.MaxReadBlock = other.MaxReadBlock
	}
	return cfg
}
