package bufferpool

import "github.com/hastyy/murakami/internal/assert"

type BufferPool struct {
	pool chan []byte
}

func New(cfg Config) *BufferPool {
	cfg = cfg.CombineWith(DefaultConfig)

	assert.OK(cfg.PoolSize > 0, "PoolSize must be > 0, got %d", cfg.PoolSize)
	assert.OK(cfg.BufferSize > 0, "BufferSize must be > 0, got %d", cfg.BufferSize)

	pool := make(chan []byte, cfg.PoolSize)
	for range cfg.PoolSize {
		pool <- make([]byte, cfg.BufferSize)
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
