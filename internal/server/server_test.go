package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/hastyy/murakami/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestNew_PanicsOnInvalidAddress(t *testing.T) {
	testutil.AssertPanics(t, func() {
		cfg := Config{
			Address: netip.AddrPort{}, // Invalid address (zero value)
		}
		New(cfg)
	})
}

func TestNew_PanicsOnNilRWPool(t *testing.T) {
	testutil.AssertPanics(t, func() {
		cfg := Config{
			Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
			RWPool:  nil,
		}
		New(cfg)
	})
}

func TestNew_CreatesProperServer(t *testing.T) {
	require := require.New(t)

	addr := netip.AddrPortFrom(netip.IPv4Unspecified(), 8080)
	rwPool := &mockConnectionReadWriterPool{}

	cfg := Config{
		Address: addr,
		RWPool:  rwPool,
	}

	var s *Server
	testutil.AssertNotPanics(t, func() {
		s = New(cfg)
	})

	// Verify server is not nil
	require.NotNil(s)

	// Verify state is idle
	require.Equal(idle, s.state)

	// Verify config values are set correctly
	require.Equal(addr, s.cfg.Address)
	require.NotNil(s.cfg.StartListener)
	require.Equal(rwPool, s.cfg.RWPool)
	require.NotNil(s.cfg.BackoffFunc)
	require.Equal(DefaultConfig.MaxAcceptDelay, s.cfg.MaxAcceptDelay)
	require.NotNil(s.cfg.StartListener)
}

func TestStart_PanicsOnNilHandler(t *testing.T) {
	testutil.AssertPanics(t, func() {
		New(Config{
			Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
			RWPool:  &mockConnectionReadWriterPool{},
		}).Start(nil)
	})
}

func TestStart_ReturnsErrorOnAlreadyStopped(t *testing.T) {
	require := require.New(t)

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
	})

	s.Stop(t.Context())

	err := s.Start(HandlerFunc(func(ctx context.Context, rw *ConnectionReadWriter) bool {
		return true
	}))
	require.Error(err)
	require.Equal(ErrAlreadyStopped, err)
}

func TestStart_ReturnsErrorOnAlreadyStarted(t *testing.T) {
	require := require.New(t)

	signalEnteredAccept := make(chan struct{})
	signalCloseListener := make(chan struct{})

	listenerClosedErr := errors.New("listener closed")

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
		StartListener: func(addr netip.AddrPort) (net.Listener, error) {
			return &mockListener{acceptLogic: func() (net.Conn, error) {
				close(signalEnteredAccept)
				<-signalCloseListener
				return nil, listenerClosedErr
			}}, nil
		},
	})

	mockHandler := HandlerFunc(func(ctx context.Context, rw *ConnectionReadWriter) bool {
		return true
	})

	go s.Start(mockHandler)
	defer s.Stop(t.Context())

	<-signalEnteredAccept

	err := s.Start(mockHandler)
	require.Error(err)
	require.Equal(ErrAlreadyStarted, err)

	close(signalCloseListener)
}

func TestStart_ReturnsErrorOnListenerError(t *testing.T) {
	require := require.New(t)

	listenerErr := errors.New("listener error")

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
		StartListener: func(addr netip.AddrPort) (net.Listener, error) {
			return nil, listenerErr
		},
	})

	mockHandler := HandlerFunc(func(ctx context.Context, rw *ConnectionReadWriter) bool {
		return true
	})

	// Confirm error is returned (wrapped in the error chain)
	err := s.Start(mockHandler)
	require.Error(err)
	require.ErrorIs(err, listenerErr)

	// Confirm server never started running
	require.Equal(s.state, idle)
}

func TestStart_ReturnsErrorOnListenerAcceptError(t *testing.T) {
	require := require.New(t)

	listenerErr := errors.New("listener error")

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
		StartListener: func(addr netip.AddrPort) (net.Listener, error) {
			return &mockListener{acceptLogic: func() (net.Conn, error) {
				return nil, listenerErr
			}}, nil
		},
	})

	mockHandler := HandlerFunc(func(ctx context.Context, rw *ConnectionReadWriter) bool {
		return true
	})

	// Confirm error is returned (wrapped in the error chain)
	err := s.Start(mockHandler)
	require.Error(err)
	require.ErrorIs(err, listenerErr)

	// Confirm server never started running
	require.Equal(s.state, running)
}

func TestStart_Backoff(t *testing.T) {
	require := require.New(t)

	iterations := 5
	i := 0
	delays := make([]time.Duration, 0, iterations)

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
		StartListener: func(addr netip.AddrPort) (net.Listener, error) {
			return &mockListener{acceptLogic: func() (net.Conn, error) {
				if i < iterations {
					i++
					return nil, &mockTimeoutError{}
				}
				return nil, errors.New("listener error")
			}}, nil
		},
		BackoffFunc: func(delay time.Duration) {
			delays = append(delays, delay)
		},
		MaxAcceptDelay: 50 * time.Millisecond,
	})

	mockHandler := HandlerFunc(func(ctx context.Context, rw *ConnectionReadWriter) bool {
		return true
	})

	signal := make(chan struct{})
	go func() {
		_ = s.Start(mockHandler)
		close(signal)
	}()
	defer s.Stop(t.Context())

	<-signal

	require.Equal(iterations, len(delays))
	require.Equal(5*time.Millisecond, delays[0])
	require.Equal(10*time.Millisecond, delays[1])
	require.Equal(20*time.Millisecond, delays[2])
	require.Equal(40*time.Millisecond, delays[3])

	// Next step would be 80*time.Millisecond, but since we set cfg.MaxAcceptDelay to 50*time.Millisecond, it will be capped at that value
	require.Equal(50*time.Millisecond, delays[4])
}

func TestStop_StopsServerThatHasNeverBeenStarted(t *testing.T) {
	require := require.New(t)

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
	})

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

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
	})

	// Stop the server once
	err := s.Stop(t.Context())
	require.NoError(err)
	require.Equal(stopped, s.state)

	// Attempt to stop again should return error
	err = s.Stop(t.Context())
	require.Error(err)
	require.Equal(ErrAlreadyStopped, err)
}

func TestStop_ClosesListenerAndWaitsForConnections(t *testing.T) {
	require := require.New(t)

	conns := make(chan *mockConn, 5)
	connRefs := make([]*mockConn, 0, 5)
	var connWg sync.WaitGroup
	connWg.Add(5)
	for range 5 {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		conn := &mockConn{Conn: clientConn, wg: &connWg}
		conns <- conn
		connRefs = append(connRefs, conn)
	}

	connsReady := make(chan struct{})

	listenerCloseCalled := make(chan struct{})
	listenerClosedErr := errors.New("listener closed")

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
		StartListener: func(addr netip.AddrPort) (net.Listener, error) {
			return &mockListener{
				acceptLogic: func() (net.Conn, error) {
					select {
					case conn := <-conns:
						return conn, nil
					case <-listenerCloseCalled:
						return nil, listenerClosedErr
					default:
						close(connsReady)
						return nil, &mockTimeoutError{}
					}
				},
				closeLogic: func() error {
					close(listenerCloseCalled)
					return nil
				},
			}, nil
		},
	})

	go func() {
		err := s.Start(HandlerFunc(func(ctx context.Context, rw *ConnectionReadWriter) bool {
			time.Sleep(1 * time.Millisecond)
			return false
		}))
		require.Nil(err)
	}()

	<-connsReady

	err := s.Stop(t.Context())
	require.NoError(err)

	require.Equal(stopped, s.state)

	connWg.Wait()
	for _, conn := range connRefs {
		require.True(conn.closed)
	}
}

func TestStop_ReturnsContextCanceledError(t *testing.T) {
	require := require.New(t)

	conns := make(chan *mockConn, 5)
	for range 5 {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()
		conn := &mockConn{Conn: clientConn}
		conns <- conn
	}
	connsReady := make(chan struct{})

	closeListenerSignal := make(chan struct{})
	defer close(closeListenerSignal)

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
		StartListener: func(addr netip.AddrPort) (net.Listener, error) {
			return &mockListener{
				acceptLogic: func() (net.Conn, error) {
					select {
					case conn := <-conns:
						return conn, nil
					default:
						close(connsReady)
						return nil, &mockTimeoutError{}
					}
				},
			}, nil
		},
	})

	go s.Start(HandlerFunc(func(ctx context.Context, rw *ConnectionReadWriter) bool {
		time.Sleep(1 * time.Second)
		return false
	}))

	<-connsReady

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := s.Stop(ctx)
	require.ErrorIs(err, context.Canceled)
}

type mockConnectionReadWriterPool struct{}

func (p *mockConnectionReadWriterPool) Get() *ConnectionReadWriter {
	return NewConnectionReadWriter(1024)
}

func (p *mockConnectionReadWriterPool) Put(rw *ConnectionReadWriter) {}

type mockListener struct {
	conn      net.Conn
	acceptErr error
	closeErr  error
	netAddr   net.Addr

	acceptLogic func() (net.Conn, error)
	closeLogic  func() error
	addrLogic   func() net.Addr
}

func (l *mockListener) Accept() (net.Conn, error) {
	if l.acceptLogic != nil {
		return l.acceptLogic()
	}
	return l.conn, l.acceptErr
}

func (l *mockListener) Close() error {
	if l.closeLogic != nil {
		return l.closeLogic()
	}
	return l.closeErr
}

func (l *mockListener) Addr() net.Addr {
	if l.addrLogic != nil {
		return l.addrLogic()
	}
	return l.netAddr
}

type mockTimeoutError struct{}

func (e *mockTimeoutError) Error() string   { return "mock timeout" }
func (e *mockTimeoutError) Timeout() bool   { return true }
func (e *mockTimeoutError) Temporary() bool { return true }

type mockConn struct {
	net.Conn
	closed bool
	wg     *sync.WaitGroup
}

func (c *mockConn) Close() error {
	defer func() {
		c.closed = true
		c.wg.Done()
	}()
	return c.Conn.Close()
}
