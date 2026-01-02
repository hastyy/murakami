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

// Listener abstracts the network listening logic, allowing for custom listener implementations or testing.
type Listener interface {
	// Listen creates and returns a net.Listener bound to the specified address.
	Listen(address string) (net.Listener, error)
}

// AcceptDelayer implements a backoff delay strategy for handling temporary accept errors.
// It encapsulates the delay state and provides methods to apply and reset the backoff.
type AcceptDelayer interface {
	// Backoff increments the internal delay (with exponential backoff) and sleeps for that duration.
	// The delay starts at an initial value and doubles on each call, up to a maximum.
	Backoff()

	// Reset resets the internal delay to zero, typically called after a successful accept.
	Reset()
}

// ExponentialAcceptDelayer is an AcceptDelayer that uses exponential backoff.
// It starts with an initial delay and doubles the delay on each call to Backoff(),
// up to a maximum delay. The delay is reset to zero when Reset() is called.
type ExponentialAcceptDelayer struct {
	delay   time.Duration
	initial time.Duration
	max     time.Duration
	sleepFn func(time.Duration)
}

// NewExponentialAcceptDelayer creates a new ExponentialAcceptDelayer with the specified
// initial and maximum delay durations, and a sleep function to apply the delay.
func NewExponentialAcceptDelayer(initial, max time.Duration, sleepFn func(time.Duration)) *ExponentialAcceptDelayer {
	return &ExponentialAcceptDelayer{
		delay:   0,
		initial: initial,
		max:     max,
		sleepFn: sleepFn,
	}
}

// Backoff increments the delay and sleeps for that duration.
func (d *ExponentialAcceptDelayer) Backoff() {
	if d.delay == 0 {
		d.delay = d.initial
	} else {
		d.delay *= 2
	}
	if d.delay > d.max {
		d.delay = d.max
	}
	d.sleepFn(d.delay)
}

// Reset resets the delay to zero.
func (d *ExponentialAcceptDelayer) Reset() {
	d.delay = 0
}

// Server is a TCP server that manages connection lifecycle and delegates application logic to a Handler.
// It uses a ConnectionProvider to efficiently pool and reuse Connection objects across multiple sessions.
// The server manages its own lifecycle state (idle -> running -> stopped) and provides graceful shutdown
// with synchronization primitives to ensure all active connections are properly cleaned up.
type Server struct {
	// Server state
	state       serverState
	netListener net.Listener

	// Server dependencies
	connProvider  ConnectionProvider
	listener      Listener
	acceptDelayer AcceptDelayer

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
func NewServer(connProvider ConnectionProvider, listener Listener, acceptDelayer AcceptDelayer, cfg ServerConfig) *Server {
	assert.NonNil(connProvider, "ConnectionProvider is required")
	assert.NonNil(listener, "Listener is required")
	assert.NonNil(acceptDelayer, "AcceptDelayer is required")

	return &Server{
		state:         idle,
		connProvider:  connProvider,
		listener:      listener,
		acceptDelayer: acceptDelayer,
		cfg:           cfg,
		stopCh:        make(chan struct{}),
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
	netListener, err := s.listener.Listen(s.cfg.Address)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("unable to listen on %s: %w", s.cfg.Address, err)
	}

	// Set state and release the lock
	s.netListener = netListener
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
	if err := s.netListener.Close(); err != nil {
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

	for {
		select {
		case <-s.stopCh:
			// Graceful shutdown
			return nil
		default:
			// Accept a new connection
			conn, err := s.netListener.Accept()
			if err != nil {
				// If the error is a timeout, back off
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					s.acceptDelayer.Backoff()
					continue
				}
				// If the error is not a timeout, return it
				return err
			}

			// Reset the backoff delay
			s.acceptDelayer.Reset()

			// Check if server is stopped before handling the connection
			// and increment WaitGroup while holding the lock to avoid races with Stop()
			// which might be already in s.connWg.Wait()
			s.mu.Lock()
			if s.state == stopped {
				s.mu.Unlock()
				// Close error is ignored because we're in shutdown path and there's no handler to report to.
				// The connection was just accepted, so close failures are rare (typically only syscall.EBADF).
				// TODO: Log this error when we add a logger to the Server.
				_ = conn.Close()
				return nil
			}
			s.connWg.Add(1)
			s.mu.Unlock()

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
	defer s.connWg.Done()
	// Close error is ignored in this cleanup defer because we're done with the connection regardless
	// of whether Close succeeds. Common errors (syscall.EBADF for already-closed connections) aren't actionable.
	// TODO: Log this error when we add a logger to the Server.
	defer func() { _ = conn.Close() }()

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
			err := c.BufferedWriter().Flush()
			if err != nil {
				// If we can't flush the response, close the connection
				return
			}

			if close {
				// If the handler returns close=true, return so we close the connection and
				// cleanup any state associated with it.
				return
			}
		}
	}
}
