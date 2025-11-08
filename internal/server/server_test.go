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
	require.NotNil(t, s)

	// Verify state is idle
	require.Equal(t, idle, s.state)

	// Verify config values are set correctly
	require.Equal(t, addr, s.cfg.Address)
	require.NotNil(t, s.cfg.StartListener)
	require.Equal(t, rwPool, s.cfg.RWPool)
	require.NotNil(t, s.cfg.BackoffFunc)
	require.Equal(t, DefaultConfig.MaxAcceptDelay, s.cfg.MaxAcceptDelay)
	require.NotNil(t, s.cfg.StartListener)
}

func TestStart_ReturnsErrorOnAlreadyStopped(t *testing.T) {
	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
	})

	s.Stop(t.Context())

	err := s.Start(nil)
	require.Error(t, err)
	require.Equal(t, ErrAlreadyStopped, err)
}

func TestStart_ReturnsErrorOnAlreadyStarted(t *testing.T) {
	signalEnteredAccept := make(chan struct{})
	signalCloseListener := make(chan struct{})

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
		StartListener: func(addr netip.AddrPort) (net.Listener, error) {
			return &mockListener{acceptLogic: func() (net.Conn, error) {
				close(signalEnteredAccept)
				<-signalCloseListener
				return nil, nil
			}}, nil
		},
	})

	go s.Start(nil)
	defer s.Stop(t.Context())

	<-signalEnteredAccept

	err := s.Start(nil)
	require.Error(t, err)
	require.Equal(t, ErrAlreadyStarted, err)

	close(signalCloseListener)
}

func TestStart_ReturnsErrorOnListenerError(t *testing.T) {
	listenerErr := errors.New("listener error")

	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
		StartListener: func(addr netip.AddrPort) (net.Listener, error) {
			return nil, listenerErr
		},
	})

	// Confirm error is returned (wrapped in the error chain)
	err := s.Start(nil)
	require.Error(t, err)
	require.ErrorIs(t, err, listenerErr)

	// Confirm server never started running
	require.Equal(t, s.state, idle)
}

func TestStart_ReturnsErrorOnListenerAcceptError(t *testing.T) {
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

	// Confirm error is returned (wrapped in the error chain)
	err := s.Start(nil)
	require.Error(t, err)
	require.ErrorIs(t, err, listenerErr)

	// Confirm server never started running
	require.Equal(t, s.state, running)
}

func TestStart_Backoff(t *testing.T) {
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

	signal := make(chan struct{})
	go func() {
		_ = s.Start(nil)
		close(signal)
	}()
	defer s.Stop(t.Context())

	<-signal

	require.Equal(t, iterations, len(delays))
	require.Equal(t, 5*time.Millisecond, delays[0])
	require.Equal(t, 10*time.Millisecond, delays[1])
	require.Equal(t, 20*time.Millisecond, delays[2])
	require.Equal(t, 40*time.Millisecond, delays[3])

	// Next step would be 80*time.Millisecond, but since we set cfg.MaxAcceptDelay to 50*time.Millisecond, it will be capped at that value
	require.Equal(t, 50*time.Millisecond, delays[4])
}

func TestStop_StopsServerThatHasNeverBeenStarted(t *testing.T) {
	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
	})

	// Verify server is idle
	require.Equal(t, idle, s.state)

	// Stop should succeed without error
	err := s.Stop(t.Context())
	require.NoError(t, err)

	// Verify server is stopped
	require.Equal(t, stopped, s.state)
}

func TestStop_ReturnsErrorOnAlreadyStopped(t *testing.T) {
	s := New(Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
	})

	// Stop the server once
	err := s.Stop(t.Context())
	require.NoError(t, err)
	require.Equal(t, stopped, s.state)

	// Attempt to stop again should return error
	err = s.Stop(t.Context())
	require.Error(t, err)
	require.Equal(t, ErrAlreadyStopped, err)
}

func TestStop_ClosesListenerAndWaitsForConnections(t *testing.T) {
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
		require.Nil(t, err)
	}()

	<-connsReady

	err := s.Stop(t.Context())
	require.NoError(t, err)

	require.Equal(t, stopped, s.state)

	connWg.Wait()
	for _, conn := range connRefs {
		require.True(t, conn.closed)
	}
}

func TestStop_ReturnsContextCanceledError(t *testing.T) {
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
						//select {} // block foreveer
						//return nil, nil
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
	require.ErrorIs(t, err, context.Canceled)
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
