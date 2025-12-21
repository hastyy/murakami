package tcp

import "github.com/hastyy/murakami/internal/unit"

const (
	// TODO: Make all these configurable
	DEFAULT_POOL_SIZE         = 1024
	DEFAULT_READ_BUFFER_SIZE  = 4 * unit.KiB
	DEFAULT_WRITE_BUFFER_SIZE = 4 * unit.KiB
)

type ConnectionPool struct {
	pool chan *Connection
}

func NewConnectionPool() *ConnectionPool {
	pool := make(chan *Connection, DEFAULT_POOL_SIZE)
	for range DEFAULT_POOL_SIZE {
		pool <- NewConnection(DEFAULT_READ_BUFFER_SIZE, DEFAULT_WRITE_BUFFER_SIZE)
	}

	return &ConnectionPool{
		pool: pool,
	}
}

func (p *ConnectionPool) Get() *Connection {
	return <-p.pool
}

func (p *ConnectionPool) Put(c *Connection) {
	p.pool <- c
}
