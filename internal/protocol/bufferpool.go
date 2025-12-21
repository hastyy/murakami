package protocol

import "github.com/hastyy/murakami/internal/unit"

const (
	// TODO: Make all these configurable
	DEFAULT_POOL_SIZE   = 1024
	DEFAULT_BUFFER_SIZE = 4 * unit.KiB
)

type BufferPool struct {
	pool chan []byte
}

func NewBufferPool() *BufferPool {
	pool := make(chan []byte, DEFAULT_POOL_SIZE)
	for range DEFAULT_POOL_SIZE {
		pool <- make([]byte, DEFAULT_BUFFER_SIZE)
	}

	return &BufferPool{
		pool: pool,
	}
}

func (p *BufferPool) Get() []byte {
	return <-p.pool
}

func (p *BufferPool) Put(b []byte) {
	p.pool <- b
}
