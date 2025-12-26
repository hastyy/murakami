package tcp

import "github.com/hastyy/murakami/internal/unit"

const (
	// TODO: Make all these configurable
	// DEFAULT_POOL_SIZE is the maximum number of connections that can be pooled.
	DEFAULT_POOL_SIZE = 1024
	// DEFAULT_READ_BUFFER_SIZE is the size of the read buffer for each pooled connection (4 KiB).
	DEFAULT_READ_BUFFER_SIZE = 4 * unit.KiB
	// DEFAULT_WRITE_BUFFER_SIZE is the size of the write buffer for each pooled connection (4 KiB).
	DEFAULT_WRITE_BUFFER_SIZE = 4 * unit.KiB
)

// ConnectionPool is a channel-based connection pool for efficient reuse of Connection objects.
// It pre-allocates a fixed number of connections at initialization and uses a buffered channel
// for lock-free connection management. Get() blocks when empty (back-pressure), and Put() discards
// connections when full to prevent blocking. This implementation prioritizes memory safety
// over service availability.
type ConnectionPool struct {
	pool chan *Connection
}

// NewConnectionPool creates a new ConnectionPool pre-populated with DEFAULT_POOL_SIZE connections,
// each configured with DEFAULT_READ_BUFFER_SIZE and DEFAULT_WRITE_BUFFER_SIZE buffers.
func NewConnectionPool() *ConnectionPool {
	pool := make(chan *Connection, DEFAULT_POOL_SIZE)
	for range DEFAULT_POOL_SIZE {
		pool <- NewConnection(DEFAULT_READ_BUFFER_SIZE, DEFAULT_WRITE_BUFFER_SIZE)
	}

	return &ConnectionPool{
		pool: pool,
	}
}

// Get retrieves a Connection from the pool.
// This method blocks until a connection becomes available, providing natural back-pressure
// when all connections are in use. If the pool is empty, the caller waits until another
// goroutine returns a connection via Put().
func (p *ConnectionPool) Get() *Connection {
	// Could add a select/default to allocate a new Connection if the pool is empty
	// But won't do it for now since the preference for now is memory safety
	// over service availability
	return <-p.pool
}

// Put returns a Connection to the pool for reuse.
// If the pool is full, the connection is silently discarded to avoid blocking the caller.
// The caller should ensure the connection is properly reset before returning it to the pool.
// Putting the same connection into the pool multiple times can lead to undefined behavior
// and is not detected by the current implementation.
func (p *ConnectionPool) Put(c *Connection) {
	// Could add some sort of pointer set to detect if we're trying to put the same Connection twice
	// But won't do it for now to avoid complexity
	select {
	case p.pool <- c:
	default:
		// If the pool is full, discard the Connection
	}
}
