package server

import (
	"net"
	"net/netip"
	"time"

	"github.com/hastyy/murakami/internal/util"
)

type Config struct {
	// Server params
	// The address to listen on.
	Address netip.AddrPort

	// Dependencies
	// The pool of ConnectionReadWriters to use for new server connections.
	RWPool ConnectionReadWriterPool

	// Config values
	// The function to create a new listener for the server. Takes in the Config.Address. It defaults to net.Listen.
	StartListener func(addr netip.AddrPort) (net.Listener, error)
	// The function to back off the server after an error. Takes in the delay duration. It defaults to time.Sleep.
	BackoffFunc func(delay time.Duration)
	// The maximum delay to wait before accepting a new connection.
	MaxAcceptDelay time.Duration
	// The maximum number of concurrent connections to accept.
	MaxConcurrentConnections int
	// The size of the buffer to use for reading from the connection.
	ConnectionReadBufferSize int
}

// DefaultConfig specifies the default config values for a Server.
var DefaultConfig = Config{
	StartListener: func(addr netip.AddrPort) (net.Listener, error) {
		return net.Listen("tcp", addr.String())
	},
	BackoffFunc:              time.Sleep,
	MaxAcceptDelay:           1 * time.Second,
	MaxConcurrentConnections: 10_000,
	ConnectionReadBufferSize: 1 * util.KiB,
}

// CombineWith takes the values from the other config and combines them with the values from the current config.
// Specifically, it fills the gaps on the current config (unset values) with the corresponding values from the other config (if it has them).
func (cfg Config) CombineWith(other Config) Config {
	var zero netip.AddrPort
	if cfg.Address == zero {
		cfg.Address = other.Address
	}
	if cfg.RWPool == nil {
		cfg.RWPool = other.RWPool
	}
	if cfg.StartListener == nil {
		cfg.StartListener = other.StartListener
	}
	if cfg.BackoffFunc == nil {
		cfg.BackoffFunc = other.BackoffFunc
	}
	if cfg.MaxAcceptDelay == 0 {
		cfg.MaxAcceptDelay = other.MaxAcceptDelay
	}
	if cfg.MaxConcurrentConnections == 0 {
		cfg.MaxConcurrentConnections = other.MaxConcurrentConnections
	}
	if cfg.ConnectionReadBufferSize == 0 {
		cfg.ConnectionReadBufferSize = other.ConnectionReadBufferSize
	}
	return cfg
}
