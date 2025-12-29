package protocol

import "github.com/hastyy/murakami/internal/unit"

const (
	// TODO: Make all these configurable
	// DEFAULT_POOL_SIZE is the maximum number of buffers that can be pooled.
	DEFAULT_POOL_SIZE = 1024 * 1024
	// DEFAULT_BUFFER_SIZE is the size of each buffer in the pool (4 KiB).
	DEFAULT_BUFFER_SIZE = 4 * unit.KiB
)

// BufferPool is a channel-based buffer pool for efficient reuse of byte slices.
// It pre-allocates a fixed number of buffers at initialization and uses a buffered channel
// for lock-free buffer management. Get() blocks when empty (back-pressure), and Put() discards
// buffers when full to prevent blocking. This implementation prioritizes memory safety
// over service availability.
type BufferPool struct {
	pool chan []byte
}

// NewBufferPool creates a new BufferPool pre-populated with DEFAULT_POOL_SIZE buffers,
// each of size DEFAULT_BUFFER_SIZE.
func NewBufferPool() *BufferPool {
	pool := make(chan []byte, DEFAULT_POOL_SIZE)
	for range DEFAULT_POOL_SIZE {
		pool <- make([]byte, DEFAULT_BUFFER_SIZE)
	}

	return &BufferPool{
		pool: pool,
	}
}

// Get retrieves a buffer from the pool. Blocks if the pool is empty, providing back-pressure
// to callers. This blocking behavior prevents unbounded memory growth by limiting the number
// of concurrent buffer allocations.
func (p *BufferPool) Get() []byte {
	// Could add a select/default to allocate a new buffer if the pool is empty
	// But won't do it for now since the preference for now is memory safety
	// over service availability
	return <-p.pool
}

// Put returns a buffer to the pool for reuse. If the pool is full, the buffer is discarded
// (non-blocking). This prevents callers from blocking on Put() operations, ensuring Put() never
// stalls request processing.
func (p *BufferPool) Put(b []byte) {
	// Could add some sort of pointer set to detect if we're trying to put the same buffer twice
	// But won't do it for now to avoid complexity
	select {
	case p.pool <- b:
	default:
		// If the pool is full, discard the buffer
	}
}
