package server

import (
	"bufio"
	"io"
	"net"
)

// ConnectionReadWriter is a wrapper around a net.Conn that implements the io.ReadWriter interface.
// It implements the io.Reader interface through *bufio.Reader.
// It implements the io.Writer interface through direct implementation of the Write method.
// It is meant to be a pooled resource within a statically allocated pool to avoid allocations during runtime.
// The underlying *bufio.Reader buffer is never resized.
// It also uses an *io.LimitedReader to limit the number of bytes that can be read from the connection.
// Since this is a pooled resource, it offers a way to reset its internal state, namely to detach it from the underlying connection.
// It expects a connection to be attached to it when it's taken out of the pool.
type ConnectionReadWriter struct {
	// Created at initialization.
	bufReader *bufio.Reader
	limReader *io.LimitedReader

	// Attached and detached per session.
	conn net.Conn
}

// NewConnectionReadWriter creates a new ConnectionReadWriter with the given buffer size for the underlying *bufio.Reader.
func NewConnectionReadWriter(bufferSize int) *ConnectionReadWriter {
	limReader := &io.LimitedReader{R: nil, N: int64(bufferSize)}
	return &ConnectionReadWriter{
		bufReader: bufio.NewReaderSize(limReader, bufferSize),
		limReader: limReader,
	}
}

// Read reads data from the underlying connection into the given buffer.
// If a connection is not attached, it returns an io.EOF error.
func (rw *ConnectionReadWriter) Read(p []byte) (n int, err error) {
	if rw.conn == nil {
		return 0, io.EOF
	}
	return rw.bufReader.Read(p)
}

// Write writes data to the underlying connection.
// If a connection is not attached, it returns an io.EOF error.
func (rw *ConnectionReadWriter) Write(p []byte) (n int, err error) {
	if rw.conn == nil {
		return 0, io.EOF
	}
	return rw.conn.Write(p)
}

// Reader returns the underlying *bufio.Reader.
func (rw *ConnectionReadWriter) Reader() *bufio.Reader {
	return rw.bufReader
}

// Attach attaches a connection to the ConnectionReadWriter.
func (rw *ConnectionReadWriter) Attach(conn net.Conn) {
	rw.conn = conn
	rw.limReader.R = conn
}

// Detach detaches the connection from the ConnectionReadWriter.
func (rw *ConnectionReadWriter) Detach() {
	rw.conn = nil
	rw.limReader.R = nil
}

// ResetLimits resets the limits of the underlying *io.LimitedReader.
func (rw *ConnectionReadWriter) ResetLimits() {
	rw.limReader.N = int64(rw.bufReader.Size())
}
