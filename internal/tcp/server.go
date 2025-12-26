package tcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hastyy/murakami/internal/assert"
)

const (
	// TODO: Make all these configurable
	DEFAULT_MAX_ACCEPT_DELAY = 1 * time.Second
)

var (
	errAlreadyStarted = errors.New("TCP server already started")
	errAlreadyStopped = errors.New("TCP server already stopped")
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
	// The Connection is passed to the handler to allow for reading and writing to the connection.
	// It returns a boolean value indicating whether the connection should be closed.
	Handle(ctx context.Context, conn *Connection) (close bool)
}

// HandlerFunc can wrap a function with the Handle format and turn it into a Handler.
type HandlerFunc func(ctx context.Context, conn *Connection) (close bool)

// Handle implements the Handler interface.
func (h HandlerFunc) Handle(ctx context.Context, c *Connection) (close bool) {
	return h(ctx, c)
}

// ConnectionProvider represents a connection pool that manages pre-allocated *Connection objects.
// It enables efficient connection reuse and reduces allocation pressure during server operation.
type ConnectionProvider interface {
	// Get returns a Connection from the internal pool.
	// If the pool is empty, the underlying implementation may choose to block waiting
	// for a Connection to become available, or allocate new Connections on demand.
	Get() *Connection

	// Put returns a Connection to the pool for reuse.
	// If the pool is full, the underlying implementation may discard the Connection.
	// This call never blocks.
	// Putting the same Connection into the pool more than once concurrently is not expected
	// and can cause problems; the underlying implementation may or may not detect this.
	Put(*Connection)
}

// ListenFunc is a function that creates and returns a net.Listener bound to the specified address.
// It abstracts the network listening logic, allowing for custom listener implementations or testing.
type ListenFunc func(address string) (net.Listener, error)

// BackoffFunc is a function that implements a backoff delay strategy.
// It's called with the desired delay duration when the server needs to back off
// (e.g., after temporary connection acceptance errors) before retrying.
type BackoffFunc func(delay time.Duration)

// Server is a TCP server that manages connection lifecycle and delegates application logic to a Handler.
// It uses a ConnectionProvider to efficiently pool and reuse Connection objects across multiple sessions.
// The server manages its own lifecycle state (idle -> running -> stopped) and provides graceful shutdown
// with synchronization primitives to ensure all active connections are properly cleaned up.
type Server struct {
	// Server state
	state    serverState
	listener net.Listener

	// Server dependencies
	connProvider ConnectionProvider
	listen       ListenFunc
	backoff      BackoffFunc

	// Server config
	cfg ServerConfig

	// Server synchronization
	mu     sync.Mutex     // Mutex to synchronize access to the server state
	stopCh chan struct{}  // Channel to signal the server to stop accepting new connections
	connWg sync.WaitGroup // WaitGroup to wait for all connection handlers to finish
}

// ServerConfig contains configuration parameters for the TCP server.
type ServerConfig struct {
	// Address is the network address to listen on (e.g., ":8080" or "localhost:8080").
	Address string
}

// NewServer creates a new TCP Server with the specified dependencies and configuration.
// The Server is created in an idle state and must be started via Start() before accepting connections.
func NewServer(connProvider ConnectionProvider, listen ListenFunc, backoff BackoffFunc, cfg ServerConfig) *Server {
	assert.NonNil(connProvider, "ConnectionProvider is required")
	assert.NonNil(listen, "ListenFunc is required")
	assert.NonNil(backoff, "BackoffFunc is required")

	return &Server{
		state:        idle,
		connProvider: connProvider,
		listen:       listen,
		backoff:      backoff,
		cfg:          cfg,
		stopCh:       make(chan struct{}),
	}
}

// Start starts the server and starts accepting connections.
// If it fails before listening for connections, it will return an error and never transitions from idle to running.
// Otherwise, it will transition from idle to running and start accepting connections, and Start() cannot be called again.
// If multiple Start() calls are made, even if concurrently, only one will succeed.
// If we try to start an already stopped server, it will also return an error.
func (s *Server) Start(h Handler) error {
	// Validate handler is not nil
	assert.NonNil(h, "Handler is required")

	// Synchronize with other Start() and Stop() calls
	s.mu.Lock()

	// Check if the server is already started or stopped
	// since it can only be started and stopped once
	switch s.state {
	case stopped:
		s.mu.Unlock()
		return errAlreadyStopped
	case running:
		s.mu.Unlock()
		return errAlreadyStarted
	}

	// Start listening for TCP connections
	listener, err := s.listen(s.cfg.Address)
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
		return errAlreadyStopped
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
					if acceptDelay > DEFAULT_MAX_ACCEPT_DELAY {
						acceptDelay = DEFAULT_MAX_ACCEPT_DELAY
					}

					// Back off for the duration of the accept delay
					s.backoff(acceptDelay)
					continue
				}
				// If the error is not a timeout, return it
				return err
			}

			// Reset the backoff delay
			acceptDelay = 0

			// TODO: Fix race condition here.
			// I need to hold the lock here and confirm the server state is still running.
			// if it's not, we should close the connection and either return or loop again
			// to find s.stopCh has been closed.
			//
			// We should also move the wg.Add(1) here before spawning the new goroutine.
			// And let the first line in handle() be the defer wg.Done().
			//
			// wg.Add(1) should be done while still holding the lock.
			// Spawning the goroutine can be done after we release the lock.
			//
			// EXAMPLE:
			// // After successful Accept()
			// s.mu.Lock()
			// if s.state == stopped {
			//     s.mu.Unlock()
			//     conn.Close()
			//     return nil
			// }
			// s.connWg.Add(1)
			// s.mu.Unlock()
			// go s.handle(...)

			// Handle the connection in a new goroutine to continue accepting new connections
			//
			// NOTE: Should we introduce a Scheduler abstraction here to control goroutine scheduling
			// for deterministic simulation testing? Would we also need to replace stdlib non-blocking
			// calls (I/O, networking) with alternatives that yield control back to the scheduler?
			go s.handle(ctx, conn, h)
		}
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn, h Handler) {
	defer conn.Close()

	s.connWg.Add(1)
	defer s.connWg.Done()

	c := s.connProvider.Get()
	defer s.connProvider.Put(c)

	c.Attach(conn)
	defer c.Detach()

	c.ResetLimits()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Reset the limits of the ConnectionReadWriter for each iteration
			c.ResetLimits()

			close := h.Handle(ctx, c)

			// Flush the buffered writer to send the response to the client
			c.BufferedWriter().Flush()

			if close {
				// If the handler returns close=true, return so we close the connection and
				// cleanup any state associated with it.
				return
			}
		}
	}
}
