package tcp

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAttach(t *testing.T) {
	require := require.New(t)

	// Create a Connection
	c := NewConnection(1024, 1024)

	// Verify initial state is detached
	require.Nil(c.conn)
	require.Nil(c.lreader.R)

	// Create a mock connection using net.Pipe
	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	// Attach the connection
	c.Attach(serverConn)

	// Verify the connection is attached
	require.NotNil(c.conn)
	require.Equal(serverConn, c.conn)
	require.NotNil(c.lreader.R)

	// Verify the buffered reader can read from the connection
	// Write from client side
	go func() {
		_, _ = clientConn.Write([]byte("test"))
	}()

	buf := make([]byte, 4)
	n, err := c.breader.Read(buf)
	require.NoError(err)
	require.Equal(4, n)
	require.Equal([]byte("test"), buf)
}

func TestDetach(t *testing.T) {
	require := require.New(t)

	// Create and attach a Connection
	c := NewConnection(1024, 1024)
	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	c.Attach(serverConn)

	// Verify connection is attached
	require.NotNil(c.conn)
	require.NotNil(c.lreader.R)

	// Detach the connection
	c.Detach()

	// Verify the connection is detached
	require.Nil(c.conn)
	require.Nil(c.lreader.R)
}

func TestResetLimits(t *testing.T) {
	require := require.New(t)

	// Create and attach a Connection
	c := NewConnection(1024, 1024)
	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	c.Attach(serverConn)

	// Modify the limited reader's N value
	c.lreader.N = 100

	// Call ResetLimits
	c.ResetLimits()

	// Verify the limit was reset to buffer size
	require.Equal(int64(1024), c.lreader.N)
}

func TestBufferedReader(t *testing.T) {
	require := require.New(t)

	c := NewConnection(1024, 1024)

	// Verify BufferedReader returns the internal reader
	reader := c.BufferedReader()
	require.NotNil(reader)
	require.Equal(c.breader, reader)
}

func TestBufferedWriter(t *testing.T) {
	require := require.New(t)

	c := NewConnection(1024, 1024)

	// Verify BufferedWriter returns the internal writer
	writer := c.BufferedWriter()
	require.NotNil(writer)
	require.Equal(c.bwriter, writer)
}
