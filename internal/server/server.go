package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hastyy/murakami/internal/assert"
)

var (
	ErrAlreadyStarted = errors.New("server already started")
	ErrAlreadyStopped = errors.New("server already stopped")
)

// serverState is a type that represents the state of the server.
// A server can be in one of the following states:
// - idle: the server hasn't been started or stopped yet
// - running: the server is started and accepting connections
// - stopped: the server has been stopped and is not accepting connections
// A server will always be created in the idle state. The possible state transitions are:
// - idle -> running: the server is running after Start() is successfully called
// - running -> stopped: the server is stopped after Stop() is successfully called
// - idle -> stopped: the server is stopped after Stop() is called without being started first
// A server that is stopped cannot be started (again) and should be garbage collected.
type serverState int

const (
	idle serverState = iota
	running
	stopped
)

// Handler should specify the application layer logic to run in each iteration of
// the read/write loop of a connection.
type Handler interface {
	// Handle is called for each iteration of the read/write loop of a connection.
	// The context is passed to the handler to allow for cancellation of the connection handling.
	// The ConnectionReadWriter is passed to the handler to allow for reading and writing to the connection.
	// It returns a boolean value indicating whether the connection should be closed.
	Handle(ctx context.Context, rw *ConnectionReadWriter) (close bool)
}

type ConnectionReadWriterPool interface {
	Get() *ConnectionReadWriter
	Put(*ConnectionReadWriter)
}

// Server represents a TCP server.
type Server struct {
	// Config uses in initialization.
	cfg Config

	// Server state.
	state    serverState
	listener net.Listener

	// Server dependencies.
	rwPool      ConnectionReadWriterPool
	backoffFunc func(delay time.Duration)

	// Server synchronization.
	mu     sync.Mutex     // Mutex to synchronize access to the server state.
	stopCh chan struct{}  // Channel to signal the server to stop accepting new connections.
	connWg sync.WaitGroup // WaitGroup to wait for all connection handlers to finish.
}

func New(cfg Config) *Server {
	// Guarantee params and dependencies are passed through correctly
	assert.OK(cfg.Address.IsValid(), "invalid address: %s", cfg.Address)
	assert.NonNil(cfg.RWPool, "ConnectionReadWriterPool is required")

	// Set defaults
	cfg = cfg.CombineWith(DefaultConfig)

	// Create and return the server.
	return &Server{
		cfg:         cfg.CombineWith(DefaultConfig),
		state:       idle,
		rwPool:      cfg.RWPool,
		backoffFunc: cfg.BackoffFunc,
		stopCh:      make(chan struct{}),
	}
}

// Start starts the server and starts accepting connections.
// If it fails before listening for connections, it will return an error and never transitions from idle to running.
// Otherwise, it will transition from idle to running and start accepting connections, and Start() cannot be called again.
// If multiple Start() calls are made, even if concurrently, only one will succeed.
// If we try to start and already stoped server, it will also return an error.
func (s *Server) Start(h Handler) error {
	// Synchronize with other Start() and Stop() calls
	s.mu.Lock()

	// Check if the server is already started or stopped
	// since it can only be started and stopped once
	switch s.state {
	case stopped:
		s.mu.Unlock()
		return ErrAlreadyStopped
	case running:
		s.mu.Unlock()
		return ErrAlreadyStarted
	}

	// Start listening for TCP connections
	listener, err := s.cfg.StartListener(s.cfg.Address)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("unable to listen on %s: %w", s.cfg.Address, err)
	}

	// Set state and release the lock
	s.listener = listener
	s.state = running
	s.mu.Unlock()

	// Start accepting connections
	return s.acceptLoop(h)
}

// Stop stops the server and stops accepting new connections.
// After a server is stopped it cannot be started again and should be garbage collected.
// If the server is not running, it will just set the state to stopped.
// If the server is already stopped, it will return an error.
// If the server is running, it will stop accepting new connections and wait for all connection handlers to finish.
// If the context is done, it will return the context error.
func (s *Server) Stop(ctx context.Context) error {
	// Synchronize with other Start() and Stop() calls
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the server is already stopped
	if s.state == stopped {
		return ErrAlreadyStopped
	}

	isRunning := s.state == running

	// Set stopped state
	close(s.stopCh)
	s.state = stopped

	// If the server is not running, just return nil
	if !isRunning {
		return nil
	}

	// Close the listener
	if err := s.listener.Close(); err != nil {
		return err
	}

	// Wait for all connection handlers to finish
	done := make(chan struct{})
	go func() {
		s.connWg.Wait()
		close(done)
	}()

	// Wait for the context to be done or for all connection handlers to finish.
	// If the context is cancelled first, return the context error.
	// If all connection handlers finished, return nil.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (s *Server) acceptLoop(h Handler) error {
	// Context to control connection handlers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Acts as a backoff mechanism to avoid busy-waiting
	var acceptDelay time.Duration

	for {
		select {
		case <-s.stopCh:
			// Graceful shutdown
			return nil
		default:
			// Accept a new connection
			conn, err := s.listener.Accept()
			if err != nil {
				// If the error is a timeout, back off
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					if acceptDelay == 0 {
						acceptDelay = 5 * time.Millisecond
					} else {
						acceptDelay *= 2
					}
					if acceptDelay > s.cfg.MaxAcceptDelay {
						acceptDelay = s.cfg.MaxAcceptDelay
					}

					// Back off for the duration of the accept delay
					s.backoffFunc(acceptDelay)
					continue
				}
				// If the error is not a timeout, return it
				return err
			}

			// Reset the backoff delay
			acceptDelay = 0

			// Handle the connection in a new goroutine to continue accepting new connections
			go s.handle(ctx, conn, h)
		}
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn, h Handler) {
	// Close the connection when the handler returns
	defer conn.Close()

	// Add to the wait group to wait for the connection handler to finish
	s.connWg.Add(1)
	// Remove from the wait group when the connection handler returns
	defer s.connWg.Done()

	// Get a ConnectionReadWriter from the pool
	rw := s.cfg.RWPool.Get()
	// Return the ConnectionReadWriter to the pool when the handler returns
	defer s.cfg.RWPool.Put(rw)

	// Attach the connection to the ConnectionReadWriter
	rw.Attach(conn)
	// Detach the connection from the ConnectionReadWriter when the handler returns
	defer rw.Detach()

	// Handle the connection in a loop
	for {
		select {
		case <-ctx.Done():
			// If the context is done, return
			return
		default:
			// Reset the limits of the ConnectionReadWriter for each iteration
			rw.ResetLimits()
			if close := h.Handle(ctx, rw); close {
				// If the handler returns close=true, return so we close the connection and
				// cleanup any state associated with it.
				return
			}
		}
	}
}
