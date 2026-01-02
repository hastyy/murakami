package tcp

import (
	"bufio"
	"io"
	"net"
	"time"
)

const (
	// TODO: Make all these configurable
	DEFAULT_CONNECTION_READ_DEADLINE  = 10 * time.Second
	DEFAULT_CONNECTION_WRITE_DEADLINE = 15 * time.Second
)

// Connection is a wrapper around net.Conn for TCP connections that implements io.Reader and io.Writer.
// It provides buffered I/O through an underlying *bufio.Reader and *bufio.Writer, each created with
// a configured buffer size. A Connection can support one TCP session at a time but multiple sessions
// throughout its lifetime via Attach() and Detach(). These objects are created ahead of time (allocating
// buffers internally) and are meant to be pooled and reused by TCP servers and clients for efficient
// resource management. The Connection also contains an *io.LimitedReader to enforce configurable read
// limits and deadlines that should be applied on each exchange and reset before the next one.
type Connection struct {
	// The underlying TCP connection. Attached at the start of a session, detached at the end.
	conn net.Conn

	// Enforces a read limit per exchange to prevent unbounded reads.
	// Reset before each exchange via ResetLimits().
	lreader *io.LimitedReader

	// The internal buffers in bufio.Reader and bufio.Writer never grow beyond their initial size.
	// This guarantees predictable memory usage and makes these objects safe to pool and reuse.
	breader *bufio.Reader
	bwriter *bufio.Writer
}

// NewConnection creates a new Connection with the specified read and write buffer sizes.
// The Connection is created in a detached state (no underlying net.Conn attached).
// Buffers are pre-allocated to avoid allocations during runtime.
func NewConnection(readBufferSize, writeBufferSize int) *Connection {
	lreader := &io.LimitedReader{R: nil, N: int64(readBufferSize * 2)}
	breader := bufio.NewReaderSize(lreader, readBufferSize)
	bwriter := bufio.NewWriterSize(nil, writeBufferSize)
	return &Connection{
		conn:    nil,
		lreader: lreader,
		breader: breader,
		bwriter: bwriter,
	}
}

// Attach attaches a net.Conn to this Connection, making it ready to handle a new TCP session.
// This should be called when taking a Connection from the pool to begin a new session.
func (c *Connection) Attach(conn net.Conn) {
	c.conn = conn
	c.lreader.R = conn
	c.breader.Reset(c.lreader)
	c.bwriter.Reset(conn)
}

// Detach detaches the underlying net.Conn from this Connection, resetting it to a detached state.
// This should be called when returning a Connection to the pool after a session ends.
func (c *Connection) Detach() {
	c.conn = nil
	c.lreader.R = nil
}

// ResetLimits resets the read limit and read/write deadlines for the connection.
// This should be called before each request/response exchange to enforce fresh limits and timeouts.
func (c *Connection) ResetLimits() {
	c.lreader.N = int64(c.breader.Size() * 2)
	// Deadline errors are ignored because they rarely fail in practice and typically only occur
	// when the connection is already closed or invalid (syscall.EINVAL, io.ErrClosedPipe).
	// If the connection is dead, subsequent I/O operations will fail with appropriate errors anyway.
	_ = c.conn.SetReadDeadline(time.Now().Add(DEFAULT_CONNECTION_READ_DEADLINE))
	_ = c.conn.SetWriteDeadline(time.Now().Add(DEFAULT_CONNECTION_WRITE_DEADLINE))
}

// Read reads data from the underlying connection into the given buffer through the buffered reader.
// If no connection is attached, it returns io.EOF.
func (c *Connection) Read(p []byte) (int, error) {
	if c.conn == nil {
		return 0, io.EOF
	}
	return c.breader.Read(p)
}

// Write writes data to the underlying connection through the buffered writer.
// If no connection is attached, it returns io.EOF.
func (c *Connection) Write(p []byte) (int, error) {
	if c.conn == nil {
		return 0, io.EOF
	}
	return c.bwriter.Write(p)
}

// BufferedReader returns the underlying *bufio.Reader for direct access to buffered reading operations.
func (c *Connection) BufferedReader() *bufio.Reader {
	return c.breader
}

// BufferedWriter returns the underlying *bufio.Writer for direct access to buffered writing operations.
func (c *Connection) BufferedWriter() *bufio.Writer {
	return c.bwriter
}
