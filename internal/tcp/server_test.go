package tcp

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockConnectionProvider struct{}

func (m *mockConnectionProvider) Get() *Connection {
	return NewConnection(1024, 1024)
}

func (m *mockConnectionProvider) Put(c *Connection) {
	// no-op
}

type mockListener struct {
	acceptFunc func() (net.Conn, error)
	closeFunc  func() error
	addrFunc   func() net.Addr
}

func (m *mockListener) Accept() (net.Conn, error) {
	if m.acceptFunc != nil {
		return m.acceptFunc()
	}
	return nil, errors.New("not implemented")
}

func (m *mockListener) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *mockListener) Addr() net.Addr {
	if m.addrFunc != nil {
		return m.addrFunc()
	}
	return nil
}

type mockTimeoutError struct{}

func (e *mockTimeoutError) Error() string   { return "mock timeout" }
func (e *mockTimeoutError) Timeout() bool   { return true }
func (e *mockTimeoutError) Temporary() bool { return true }

type mockConn struct {
	closed atomic.Bool
}

func newMockConn() *mockConn {
	return &mockConn{}
}

func (c *mockConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (c *mockConn) Write(b []byte) (n int, err error) {
	return 0, nil
}

func (c *mockConn) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *mockConn) LocalAddr() net.Addr {
	return nil
}

func (c *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (c *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestNew_CreatesProperServer(t *testing.T) {
	require := require.New(t)

	connProvider := &mockConnectionProvider{}
	listen := func(address string) (net.Listener, error) {
		// Can return nil, nil here since listen is never called in the test
		return nil, nil
	}
	backoff := func(delay time.Duration) {
		time.Sleep(delay)
	}
	cfg := ServerConfig{
		Address: ":8080",
	}

	s := NewServer(connProvider, listen, backoff, cfg)

	// Verify server is not nil
	require.NotNil(s)

	// Verify state is idle
	require.Equal(idle, s.state)

	// Verify dependencies are set correctly
	require.NotNil(s.connProvider)
	require.NotNil(s.listen)
	require.NotNil(s.backoff)
	require.Equal(":8080", s.cfg.Address)
	require.NotNil(s.stopCh)
}

func TestStart_ReturnsErrorOnAlreadyStopped(t *testing.T) {
	require := require.New(t)

	connProvider := &mockConnectionProvider{}
	listen := func(address string) (net.Listener, error) {
		// Can return nil, nil here since listen is never called in the test
		return nil, nil
	}
	backoff := func(delay time.Duration) {}
	cfg := ServerConfig{
		Address: ":8080",
	}

	s := NewServer(connProvider, listen, backoff, cfg)

	// Stop the server first
	ctx := context.Background()
	err := s.Stop(ctx)
	require.NoError(err)
	require.Equal(stopped, s.state)

	// Attempt to start after stopping should return error
	mockHandler := HandlerFunc(func(ctx context.Context, conn *Connection) bool {
		return true
	})

	err = s.Start(mockHandler)
	require.Error(err)
	require.Equal(errAlreadyStopped, err)
}

func TestStart_ReturnsErrorOnAlreadyStarted(t *testing.T) {
	require := require.New(t)

	signalEnteredAccept := make(chan struct{})
	signalCloseListener := make(chan struct{})
	listenerClosedErr := errors.New("listener closed")

	connProvider := &mockConnectionProvider{}
	listen := func(address string) (net.Listener, error) {
		return &mockListener{
			acceptFunc: func() (net.Conn, error) {
				close(signalEnteredAccept)
				<-signalCloseListener
				return nil, listenerClosedErr
			},
		}, nil
	}
	backoff := func(delay time.Duration) {}
	cfg := ServerConfig{
		Address: ":8080",
	}

	s := NewServer(connProvider, listen, backoff, cfg)

	mockHandler := HandlerFunc(func(ctx context.Context, conn *Connection) bool {
		return true
	})

	// Start server in a goroutine and capture the error
	startErrCh := make(chan error, 1)
	go func() {
		err := s.Start(mockHandler)
		startErrCh <- err
	}()
	defer s.Stop(t.Context())

	// Wait for server to enter accept loop
	<-signalEnteredAccept

	// Try to start again - should return error
	err := s.Start(mockHandler)
	require.Error(err)
	require.Equal(errAlreadyStarted, err)

	// Signal to close the listener so the first Start() can complete
	close(signalCloseListener)

	// Verify the first Start() returned the expected error
	startErr := <-startErrCh
	require.ErrorIs(startErr, listenerClosedErr)
}

func TestStart_ReturnsErrorOnListenerError(t *testing.T) {
	require := require.New(t)

	listenerErr := errors.New("listener error")

	connProvider := &mockConnectionProvider{}
	listen := func(address string) (net.Listener, error) {
		return nil, listenerErr
	}
	backoff := func(delay time.Duration) {}
	cfg := ServerConfig{
		Address: ":8080",
	}

	s := NewServer(connProvider, listen, backoff, cfg)

	mockHandler := HandlerFunc(func(ctx context.Context, conn *Connection) bool {
		return true
	})

	// Attempt to start should return the listener error
	err := s.Start(mockHandler)
	require.Error(err)
	require.ErrorIs(err, listenerErr)

	// Verify server never started running
	require.Equal(idle, s.state)
}

func TestStart_ReturnsErrorOnListenerAcceptError(t *testing.T) {
	require := require.New(t)

	listenerErr := errors.New("listener accept error")

	connProvider := &mockConnectionProvider{}
	listen := func(address string) (net.Listener, error) {
		return &mockListener{
			acceptFunc: func() (net.Conn, error) {
				return nil, listenerErr
			},
		}, nil
	}
	backoff := func(delay time.Duration) {}
	cfg := ServerConfig{
		Address: ":8080",
	}

	s := NewServer(connProvider, listen, backoff, cfg)

	mockHandler := HandlerFunc(func(ctx context.Context, conn *Connection) bool {
		return true
	})

	// Attempt to start should return the accept error
	err := s.Start(mockHandler)
	require.Error(err)
	require.ErrorIs(err, listenerErr)

	// Verify server transitioned to running (since listener started successfully)
	require.Equal(running, s.state)
}

func TestStart_Backoff(t *testing.T) {
	require := require.New(t)

	iterations := 9
	i := 0
	delays := make([]time.Duration, 0, iterations)

	connProvider := &mockConnectionProvider{}
	listen := func(address string) (net.Listener, error) {
		return &mockListener{
			acceptFunc: func() (net.Conn, error) {
				if i < iterations {
					i++
					return nil, &mockTimeoutError{}
				}
				return nil, errors.New("listener error")
			},
		}, nil
	}
	backoff := func(delay time.Duration) {
		delays = append(delays, delay)
	}
	cfg := ServerConfig{
		Address: ":8080",
	}

	s := NewServer(connProvider, listen, backoff, cfg)

	mockHandler := HandlerFunc(func(ctx context.Context, conn *Connection) bool {
		return true
	})

	// Start in a goroutine since it will block until the error
	startErrCh := make(chan error, 1)
	go func() {
		err := s.Start(mockHandler)
		startErrCh <- err
	}()

	// Wait for Start to complete
	err := <-startErrCh
	require.Error(err)

	// Verify we got the expected number of backoff calls
	require.Equal(iterations, len(delays))

	// Verify exponential backoff pattern
	require.Equal(5*time.Millisecond, delays[0])
	require.Equal(10*time.Millisecond, delays[1])
	require.Equal(20*time.Millisecond, delays[2])
	require.Equal(40*time.Millisecond, delays[3])
	require.Equal(80*time.Millisecond, delays[4])
	require.Equal(160*time.Millisecond, delays[5])
	require.Equal(320*time.Millisecond, delays[6])
	require.Equal(640*time.Millisecond, delays[7])

	// Next step would be 1280ms, but capped at DEFAULT_MAX_ACCEPT_DELAY (1 second)
	require.Equal(DEFAULT_MAX_ACCEPT_DELAY, delays[8])
}

func TestStop_StopsServerThatHasNeverBeenStarted(t *testing.T) {
	require := require.New(t)

	connProvider := &mockConnectionProvider{}
	listen := func(address string) (net.Listener, error) {
		// Can return nil, nil here since listen is never called in the test
		return nil, nil
	}
	backoff := func(delay time.Duration) {}
	cfg := ServerConfig{
		Address: ":8080",
	}

	s := NewServer(connProvider, listen, backoff, cfg)

	// Verify server is idle
	require.Equal(idle, s.state)

	// Stop should succeed without error
	err := s.Stop(t.Context())
	require.NoError(err)

	// Verify server is stopped
	require.Equal(stopped, s.state)
}

func TestStop_ReturnsErrorOnAlreadyStopped(t *testing.T) {
	require := require.New(t)

	connProvider := &mockConnectionProvider{}
	listen := func(address string) (net.Listener, error) {
		// Can return nil, nil here since listen is never called in the test
		return nil, nil
	}
	backoff := func(delay time.Duration) {}
	cfg := ServerConfig{
		Address: ":8080",
	}

	s := NewServer(connProvider, listen, backoff, cfg)

	// Stop the server once
	err := s.Stop(t.Context())
	require.NoError(err)
	require.Equal(stopped, s.state)

	// Attempt to stop again should return error
	err = s.Stop(t.Context())
	require.Error(err)
	require.Equal(errAlreadyStopped, err)
}

func TestStop_ClosesListenerAndWaitsForConnections(t *testing.T) {
	require := require.New(t)

	// Setup: Create mock connections that will be "accepted" by the server
	numConns := 5
	conns := make([]*mockConn, 0, numConns)
	connsCh := make(chan *mockConn, numConns)

	for range numConns {
		conn := newMockConn()
		conns = append(conns, conn)
		connsCh <- conn
	}

	// Synchronization: Signal when all connections have been accepted
	// This lets the test know when the server has accepted all the connections
	allConnsAccepted := make(chan struct{})
	var closeAllConnsAcceptedOnce sync.Once

	connProvider := &mockConnectionProvider{}
	listen := func(address string) (net.Listener, error) {
		return &mockListener{
			acceptFunc: func() (net.Conn, error) {
				select {
				case conn := <-connsCh:
					// Return one of the pre-created connections
					return conn, nil
				default:
					// Once all connections are accepted, signal the test and return timeout errors
					// The timeout errors keep the accept loop running without accepting new connections
					closeAllConnsAcceptedOnce.Do(func() {
						close(allConnsAccepted)
					})
					return nil, &mockTimeoutError{}
				}
			},
		}, nil
	}
	backoff := func(delay time.Duration) {}
	cfg := ServerConfig{
		Address: ":8080",
	}

	s := NewServer(connProvider, listen, backoff, cfg)

	// Handler that sleeps briefly then closes the connection (returns true)
	// The sleep simulates work being done on each connection
	mockHandler := HandlerFunc(func(ctx context.Context, conn *Connection) bool {
		time.Sleep(1 * time.Millisecond)
		return true
	})

	// Start server in a goroutine and capture the error
	startErrCh := make(chan error, 1)
	go func() {
		err := s.Start(mockHandler)
		startErrCh <- err
	}()

	// Wait for all connections to be accepted
	<-allConnsAccepted

	// Call Stop() which should:
	// 1. Close the stopCh channel (signaling handlers to stop)
	// 2. Wait for all connection handlers to finish
	// 3. Return without error once all handlers complete
	err := s.Stop(t.Context())
	require.NoError(err)

	// Verify that Start() returned nil (graceful shutdown via stopCh)
	startErr := <-startErrCh
	require.NoError(startErr)

	// Verify the server transitioned to stopped state
	require.Equal(stopped, s.state)

	// Verify all connections were properly closed
	for _, conn := range conns {
		require.True(conn.closed.Load())
	}
}

func TestStop_ReturnsContextCanceledError(t *testing.T) {
	require := require.New(t)

	// Setup: Create mock connections that will be "accepted" by the server
	numConns := 5
	connsCh := make(chan *mockConn, numConns)

	for range numConns {
		conn := newMockConn()
		connsCh <- conn
	}

	// Synchronization: Signal when all connections have been accepted
	allConnsAccepted := make(chan struct{})
	var closeAllConnsAcceptedOnce sync.Once

	connProvider := &mockConnectionProvider{}
	listen := func(address string) (net.Listener, error) {
		return &mockListener{
			acceptFunc: func() (net.Conn, error) {
				select {
				case conn := <-connsCh:
					// Return one of the pre-created connections
					return conn, nil
				default:
					// Once all connections are accepted, signal the test and return timeout errors
					closeAllConnsAcceptedOnce.Do(func() {
						close(allConnsAccepted)
					})
					return nil, &mockTimeoutError{}
				}
			},
		}, nil
	}
	backoff := func(delay time.Duration) {}
	cfg := ServerConfig{
		Address: ":8080",
	}

	s := NewServer(connProvider, listen, backoff, cfg)

	// Handler that sleeps for a long time (1 second) to simulate long-running connections
	// These handlers will still be running when we call Stop() with a canceled context
	mockHandler := HandlerFunc(func(ctx context.Context, conn *Connection) bool {
		time.Sleep(1 * time.Second)
		return false
	})

	// Start server in background
	go s.Start(mockHandler)

	// Wait for all connections to be accepted
	// At this point, all handlers are running and sleeping for 1 second
	<-allConnsAccepted

	// Create a context that's already canceled
	// This simulates a situation where the caller's context expires during shutdown
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// Call Stop() with the canceled context
	// Stop() should return immediately with context.Canceled error
	// instead of waiting for the handlers (which would take 1 second)
	err := s.Stop(ctx)
	require.ErrorIs(err, context.Canceled)
}
