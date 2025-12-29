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
//
// Note: The current implementation uses a single buffer size class (DEFAULT_BUFFER_SIZE).
// The bufferSize parameter in Get() is accepted for interface compatibility but the pool
// always returns buffers of DEFAULT_BUFFER_SIZE. Future implementations may support
// multiple size classes for more efficient memory usage.
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

// Get retrieves a buffer from the pool with at least the requested size.
// Blocks if the pool is empty, providing back-pressure to callers.
// This blocking behavior prevents unbounded memory growth by limiting the number
// of concurrent buffer allocations.
//
// Note: The current implementation ignores bufferSize and always returns buffers
// of DEFAULT_BUFFER_SIZE. Callers should ensure their requested size does not
// exceed DEFAULT_BUFFER_SIZE, or handle the case where the returned buffer
// may be smaller than requested.
func (p *BufferPool) Get(bufferSize int) []byte {
	// Could add a select/default to allocate a new buffer if the pool is empty
	// But won't do it for now since the preference for now is memory safety
	// over service availability
	//
	// TODO: Consider implementing size classes or dynamic allocation when
	// bufferSize > DEFAULT_BUFFER_SIZE
	_ = bufferSize // Currently unused; pool returns fixed-size buffers
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
