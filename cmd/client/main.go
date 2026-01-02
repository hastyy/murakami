package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hastyy/murakami/internal/protocol"
	"github.com/hastyy/murakami/internal/tcp"
	"github.com/hastyy/murakami/internal/unit"
)

func main() {
	// Define command-line flags
	concurrency := flag.Int("concurrency", 4, "number of concurrent clients to spawn (must be > 0)")
	requestCount := flag.Int("requests", 10_000, "number of requests each client should send (must be > 0)")
	requestSize := flag.Int("size", 4*unit.KiB, "size of the payload to send in bytes (must be > 0)")

	// Parse flags
	flag.Parse()

	// Validate flags
	if *concurrency <= 0 {
		fmt.Fprintf(os.Stderr, "Error: concurrency must be greater than 0, got %d\n", *concurrency)
		os.Exit(1)
	}

	if *requestCount <= 0 {
		fmt.Fprintf(os.Stderr, "Error: requests must be greater than 0, got %d\n", *requestCount)
		os.Exit(1)
	}

	if *requestSize <= 0 {
		fmt.Fprintf(os.Stderr, "Error: size must be greater than 0, got %d\n", *requestSize)
		os.Exit(1)
	}

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Concurrency: %d clients\n", *concurrency)
	fmt.Printf("  Requests per client: %d\n", *requestCount)
	fmt.Printf("  Payload size: %d bytes\n", *requestSize)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	encoder := protocol.NewCommandEncoder()
	decoder := protocol.NewReplyDecoder()

	payload := randomBytes(*requestSize)

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			client := NewSimpleS3PClient(encoder, decoder)

			err := client.Connect(ctx, "localhost:7500")
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Client %d] Failed to connect: %v\n", i, err)
				// TODO: Handle error
				return
			}

			defer func() { _ = client.Close() }()

			streamName := fmt.Sprintf("test-%d", i)

			// Create stream
			reply, err := client.CreateStream(ctx, protocol.CreateCommand{
				StreamName: streamName,
			})
			if err != nil {
				if perr, ok := protocol.IsProtocolError(err); ok {
					if perr.Code != protocol.ErrCodeStreamExists {
						fmt.Fprintf(os.Stderr, "[Client %d] Failed to create stream %s: %v\n", i, streamName, err)
						// TODO: Handle error
						return
					}
				} else {
					fmt.Fprintf(os.Stderr, "[Client %d] Failed to create stream %s: %v\n", i, streamName, err)
					// TODO: Handle error
					return
				}
			}
			if !reply.Ok && reply.Err.Code != protocol.ErrCodeStreamExists {
				fmt.Fprintf(os.Stderr, "[Client %d] Create stream %s returned error: %v\n", i, streamName, reply.Err)
				// TODO: Handle error
				return
			}
			fmt.Printf("Created stream %s\n", streamName)

			// Append records
			for j := 0; j < *requestCount; j++ {
				reply, err := client.AppendRecords(ctx, protocol.AppendCommand{
					StreamName: streamName,
					Records:    [][]byte{payload},
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "[Client %d] Failed to append record %d to stream %s: %v\n", i, j, streamName, err)
					// TODO: Handle error
					return
				}
				if reply.ID == "" {
					fmt.Fprintf(os.Stderr, "[Client %d] Append record %d to stream %s returned error: %v\n", i, j, streamName, reply.Err)
					// TODO: Handle error
					return
				}
				fmt.Printf("Appended record %d to stream %s\n", j, streamName)
			}
		}(i)
	}

	wg.Wait()

	fmt.Printf("All clients completed\n")
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}

type CommandEncoder interface {
	EncodeCreateCommand(w *bufio.Writer, cmd protocol.CreateCommand) error
	EncodeAppendCommand(w *bufio.Writer, cmd protocol.AppendCommand) error
	EncodeReadCommand(w *bufio.Writer, cmd protocol.ReadCommand) error
	EncodeTrimCommand(w *bufio.Writer, cmd protocol.TrimCommand) error
	EncodeDeleteCommand(w *bufio.Writer, cmd protocol.DeleteCommand) error
}

type ReplyDecoder interface {
	DecodeCreateReply(r *bufio.Reader) (protocol.CreateReply, error)
	DecodeAppendReply(r *bufio.Reader) (protocol.AppendReply, error)
	DecodeReadReply(r *bufio.Reader) (protocol.ReadReply, error)
	DecodeTrimReply(r *bufio.Reader) (protocol.TrimReply, error)
	DecodeDeleteReply(r *bufio.Reader) (protocol.DeleteReply, error)
}

// SimpleS3PClient is a naive S3P client implementation that performs one command/reply exchange at a time.
// Each operation (CreateStream, AppendRecords, etc.) acquires a mutex, sends a command, waits for the reply,
// and then releases the mutex. This serializes all operations on the connection.
//
// A better implementation would leverage the FIFO ordering guarantee between client and server:
// commands are processed in the order they are sent, and replies arrive in the same order.
// This allows pipelining: a client could have separate writer and reader goroutines, where the writer
// continuously sends commands without waiting for replies, and the reader continuously reads replies
// and matches them to pending commands (tracked via a channel or queue). This would significantly
// improve throughput by eliminating round-trip latency between operations.
type SimpleS3PClient struct {
	mu      sync.Mutex
	encoder CommandEncoder
	decoder ReplyDecoder
	conn    *tcp.Connection
	netConn net.Conn
}

func NewSimpleS3PClient(encoder CommandEncoder, decoder ReplyDecoder) *SimpleS3PClient {
	return &SimpleS3PClient{
		encoder: encoder,
		decoder: decoder,
		conn:    tcp.NewConnection(1024, 1024),
	}
}

func (c *SimpleS3PClient) Connect(ctx context.Context, address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already connected
	if c.netConn != nil {
		return errors.New("already connected")
	}

	// Dial with context support
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	// Attach the connection
	c.netConn = conn
	c.conn.Attach(conn)

	return nil
}

func (c *SimpleS3PClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if not connected
	if c.netConn == nil {
		return nil // Already closed, no error
	}

	// Close the underlying connection
	err := c.netConn.Close()

	// Detach regardless of close error
	c.conn.Detach()
	c.netConn = nil

	if err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	return nil
}

func (c *SimpleS3PClient) CreateStream(ctx context.Context, cmd protocol.CreateCommand) (protocol.CreateReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn.ResetLimits()

	err := c.encoder.EncodeCreateCommand(c.conn.BufferedWriter(), cmd)
	if err != nil {
		return protocol.CreateReply{}, fmt.Errorf("failed to encode create command: %w", err)
	}
	err = c.conn.BufferedWriter().Flush()
	if err != nil {
		return protocol.CreateReply{}, fmt.Errorf("failed to flush create command: %w", err)
	}

	reply, err := c.decoder.DecodeCreateReply(c.conn.BufferedReader())
	if err != nil {
		return protocol.CreateReply{}, fmt.Errorf("failed to decode create reply: %w", err)
	}

	return reply, nil
}

func (c *SimpleS3PClient) AppendRecords(ctx context.Context, cmd protocol.AppendCommand) (protocol.AppendReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn.ResetLimits()

	err := c.encoder.EncodeAppendCommand(c.conn.BufferedWriter(), cmd)
	if err != nil {
		return protocol.AppendReply{}, fmt.Errorf("failed to encode append command: %w", err)
	}
	err = c.conn.BufferedWriter().Flush()
	if err != nil {
		return protocol.AppendReply{}, fmt.Errorf("failed to flush append command: %w", err)
	}

	reply, err := c.decoder.DecodeAppendReply(c.conn.BufferedReader())
	if err != nil {
		return protocol.AppendReply{}, fmt.Errorf("failed to decode append reply: %w", err)
	}

	return reply, nil
}

func (c *SimpleS3PClient) ReadRecords(ctx context.Context, cmd protocol.ReadCommand) (protocol.ReadReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn.ResetLimits()

	err := c.encoder.EncodeReadCommand(c.conn.BufferedWriter(), cmd)
	if err != nil {
		return protocol.ReadReply{}, fmt.Errorf("failed to encode read command: %w", err)
	}
	err = c.conn.BufferedWriter().Flush()
	if err != nil {
		return protocol.ReadReply{}, fmt.Errorf("failed to flush read command: %w", err)
	}

	reply, err := c.decoder.DecodeReadReply(c.conn.BufferedReader())
	if err != nil {
		return protocol.ReadReply{}, fmt.Errorf("failed to decode read reply: %w", err)
	}

	return reply, nil
}

func (c *SimpleS3PClient) TrimStream(ctx context.Context, cmd protocol.TrimCommand) (protocol.TrimReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn.ResetLimits()

	err := c.encoder.EncodeTrimCommand(c.conn.BufferedWriter(), cmd)
	if err != nil {
		return protocol.TrimReply{}, fmt.Errorf("failed to encode trim command: %w", err)
	}
	err = c.conn.BufferedWriter().Flush()
	if err != nil {
		return protocol.TrimReply{}, fmt.Errorf("failed to flush trim command: %w", err)
	}

	reply, err := c.decoder.DecodeTrimReply(c.conn.BufferedReader())
	if err != nil {
		return protocol.TrimReply{}, fmt.Errorf("failed to decode trim reply: %w", err)
	}

	return reply, nil
}

func (c *SimpleS3PClient) DeleteStream(ctx context.Context, cmd protocol.DeleteCommand) (protocol.DeleteReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn.ResetLimits()

	err := c.encoder.EncodeDeleteCommand(c.conn.BufferedWriter(), cmd)
	if err != nil {
		return protocol.DeleteReply{}, fmt.Errorf("failed to encode delete command: %w", err)
	}
	err = c.conn.BufferedWriter().Flush()
	if err != nil {
		return protocol.DeleteReply{}, fmt.Errorf("failed to flush delete command: %w", err)
	}

	reply, err := c.decoder.DecodeDeleteReply(c.conn.BufferedReader())
	if err != nil {
		return protocol.DeleteReply{}, fmt.Errorf("failed to decode delete reply: %w", err)
	}

	return reply, nil
}
