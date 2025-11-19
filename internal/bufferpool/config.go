package bufferpool

import "github.com/hastyy/murakami/internal/util"

type Config struct {
	// Config values
	// The size of the buffer pool (number of buffers).
	PoolSize int
	// The byte size of each buffer in the pool.
	// Preferably this should align with protocol.Config.MaxRecordSize, but if not, it should be higher.
	BufferSize int
}

// DefaultConfig specifies the default config values for a BufferPool.
var DefaultConfig = Config{
	PoolSize:   1_000_000,
	BufferSize: 1 * util.KiB,
}

// CombineWith takes the values from the other config and combines them with the values from the current config.
// Specifically, it fills the gaps on the current config (unset values) with the corresponding values from the other config (if it has them).
func (cfg Config) CombineWith(other Config) Config {
	if cfg.PoolSize == 0 {
		cfg.PoolSize = other.PoolSize
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = other.BufferSize
	}
	return cfg
}
