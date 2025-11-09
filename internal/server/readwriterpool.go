package server

import "github.com/hastyy/murakami/internal/assert"

type ConcurrentConnectionReadWriterPool struct {
	pool chan *ConnectionReadWriter
}

func NewConcurrentConnectionReadWriterPool(poolSize, bufferSize int) *ConcurrentConnectionReadWriterPool {
	assert.OK(poolSize > 0, "ConnectionReadWriterPool size must be > 0, got %d", poolSize)
	assert.OK(bufferSize > 0, "ConnectionReadWriterPool buffer size must be > 0, got %d", bufferSize)

	pool := make(chan *ConnectionReadWriter, poolSize)
	for range poolSize {
		pool <- NewConnectionReadWriter(bufferSize)
	}

	return &ConcurrentConnectionReadWriterPool{
		pool: pool,
	}
}

func (p *ConcurrentConnectionReadWriterPool) Get() *ConnectionReadWriter {
	return <-p.pool
}

func (p *ConcurrentConnectionReadWriterPool) Put(rw *ConnectionReadWriter) {
	p.pool <- rw
}
