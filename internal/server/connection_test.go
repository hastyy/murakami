package server

import (
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionReadWriter_Read_WithConnection(t *testing.T) {
	// Create a pipe to simulate a network connection
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create ConnectionReadWriter and attach connection
	rw := NewConnectionReadWriter(1024)
	rw.Attach(serverConn)
	rw.ResetLimits()

	// Write data from client side
	testData := []byte("Hello, World!")
	go func() {
		_, err := clientConn.Write(testData)
		require.NoError(t, err)
	}()

	// Read data from server side using ConnectionReadWriter
	buf := make([]byte, len(testData))
	n, err := rw.Read(buf)

	require.NoError(t, err)
	require.Equal(t, len(testData), n)
	require.Equal(t, testData, buf)
}

func TestConnectionReadWriter_Read_WithoutConnection(t *testing.T) {
	// Create ConnectionReadWriter without attaching a connection
	rw := NewConnectionReadWriter(1024)

	// Attempt to read
	buf := make([]byte, 10)
	n, err := rw.Read(buf)

	require.Error(t, err)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)
}

func TestConnectionReadWriter_Read_AfterConnectionClosed(t *testing.T) {
	// Create a pipe to simulate a network connection
	serverConn, clientConn := net.Pipe()

	// Create ConnectionReadWriter and attach connection
	rw := NewConnectionReadWriter(1024)
	rw.Attach(serverConn)
	rw.ResetLimits()

	// Close both ends of the connection
	clientConn.Close()
	serverConn.Close()

	// Attempt to read from closed connection
	buf := make([]byte, 10)
	n, err := rw.Read(buf)

	require.Error(t, err)
	require.Equal(t, 0, n)
}

func TestConnectionReadWriter_Write_WithConnection(t *testing.T) {
	// Create a pipe to simulate a network connection
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create ConnectionReadWriter and attach connection
	rw := NewConnectionReadWriter(1024)
	rw.Attach(serverConn)

	// Write data from server side using ConnectionReadWriter
	testData := []byte("Hello, World!")

	// Read from client side in goroutine
	done := make(chan struct{})
	var readData []byte
	var readErr error
	go func() {
		buf := make([]byte, len(testData))
		n, err := clientConn.Read(buf)
		readData = buf[:n]
		readErr = err
		close(done)
	}()

	// Write using ConnectionReadWriter
	n, err := rw.Write(testData)

	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	// Wait for read to complete
	<-done
	require.NoError(t, readErr)
	require.Equal(t, testData, readData)
}

func TestConnectionReadWriter_Write_WithoutConnection(t *testing.T) {
	// Create ConnectionReadWriter without attaching a connection
	rw := NewConnectionReadWriter(1024)

	// Attempt to write
	testData := []byte("Hello, World!")
	n, err := rw.Write(testData)

	require.Error(t, err)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)
}

func TestConnectionReadWriter_Write_AfterConnectionClosed(t *testing.T) {
	// Create a pipe to simulate a network connection
	serverConn, clientConn := net.Pipe()

	// Create ConnectionReadWriter and attach connection
	rw := NewConnectionReadWriter(1024)
	rw.Attach(serverConn)

	// Close both ends of the connection
	clientConn.Close()
	serverConn.Close()

	// Attempt to write to closed connection
	testData := []byte("Hello, World!")
	n, err := rw.Write(testData)

	require.Error(t, err)
	require.Equal(t, 0, n)
}

func TestConnectionReadWriter_Read_HitsLimit(t *testing.T) {
	// Create a pipe to simulate a network connection
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create ConnectionReadWriter with buffer size > 16 (bufio.Reader min size)
	bufferSize := 20
	rw := NewConnectionReadWriter(bufferSize)
	rw.Attach(serverConn)
	rw.ResetLimits()

	// Write more data than the limit from client side
	testData := []byte("Hello, World! This is more than the buffer limit!")
	require.Greater(t, len(testData), bufferSize)

	go clientConn.Write(testData)

	// Read all available data until we hit the limit
	buf := make([]byte, len(testData))
	totalRead := 0

	for {
		n, err := rw.Read(buf[totalRead:])
		totalRead += n

		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// Should have read exactly up to the limit (bufferSize)
	require.Equal(t, bufferSize, totalRead, "should read exactly buffer size bytes before hitting limit")

	// Verify we can't read more after hitting the limit
	n, err := rw.Read(buf[totalRead:])
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)

	// Verify we read the correct data (first bufferSize bytes)
	require.Equal(t, testData[:bufferSize], buf[:totalRead])
}

func TestConnectionReadWriter_Read_ResetLimitsAllowsMoreReads(t *testing.T) {
	// Create a pipe to simulate a network connection
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create ConnectionReadWriter with buffer size > 16 (bufio.Reader min size)
	bufferSize := 25
	rw := NewConnectionReadWriter(bufferSize)
	rw.Attach(serverConn)
	rw.ResetLimits()

	// Write more data than the limit from client side
	testData := []byte("Hello, World! This is more than the buffer limit!")
	require.Greater(t, len(testData), bufferSize)

	go func() {
		_, err := clientConn.Write(testData)
		require.NoError(t, err)
	}()

	// Read all available data until we hit the limit
	buf := make([]byte, len(testData))
	totalRead := 0

	for {
		n, err := rw.Read(buf[totalRead:])
		totalRead += n

		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// Should have read exactly up to the limit (bufferSize)
	require.Equal(t, bufferSize, totalRead, "should read exactly buffer size bytes before hitting limit")

	// Reset limits
	rw.ResetLimits()

	// Verify we can read more after resetting limits
	n, err := rw.Read(buf[totalRead:])
	require.NoError(t, err)
	require.Equal(t, len(testData[bufferSize:]), n)

	// Verify we read the correct data (first bufferSize bytes)
	require.Equal(t, testData[:bufferSize], buf[:totalRead])
}
