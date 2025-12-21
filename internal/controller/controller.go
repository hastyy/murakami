package controller

import (
	"bufio"
	"context"
	"log/slog"
	"net"

	"github.com/hastyy/murakami/internal/protocol"
	"github.com/hastyy/murakami/internal/tcp"
)

type StreamStore interface {
	CreateStream(ctx context.Context, cmd protocol.CreateCommand) error
	AppendRecords(ctx context.Context, cmd protocol.AppendCommand) (id string, err error)
	ReadRecords(ctx context.Context, cmd protocol.ReadCommand) ([]protocol.Record, error)
	TrimStream(ctx context.Context, cmd protocol.TrimCommand) error
	DeleteStream(ctx context.Context, cmd protocol.DeleteCommand) error
}

type CommandDecoder interface {
	DecodeNextCommand(r *bufio.Reader) (protocol.CommandSpec, error)
	DecodeCreateCommand(r *bufio.Reader) (protocol.CreateCommand, error)
	DecodeAppendCommand(r *bufio.Reader) (protocol.AppendCommand, error)
	DecodeReadCommand(r *bufio.Reader) (protocol.ReadCommand, error)
	DecodeTrimCommand(r *bufio.Reader) (protocol.TrimCommand, error)
	DecodeDeleteCommand(r *bufio.Reader) (protocol.DeleteCommand, error)
}

type ReplyEncoder interface {
	EncodeOK(w *bufio.Writer) error
	EncodeError(w *bufio.Writer, error protocol.Error) error
	EncodeBulkString(w *bufio.Writer, value string) error
	EncodeRecords(w *bufio.Writer, records []protocol.Record) error
}

type Controller struct {
	store   StreamStore
	decoder CommandDecoder
	encoder ReplyEncoder
	slog    *slog.Logger
}

func New(store StreamStore, decoder CommandDecoder, encoder ReplyEncoder, slog *slog.Logger) *Controller {
	return &Controller{
		store:   store,
		decoder: decoder,
		encoder: encoder,
		slog:    slog,
	}
}

func (c *Controller) Handle(ctx context.Context, conn *tcp.Connection) (close bool) {
	cmdSpec, err := c.decoder.DecodeNextCommand(conn.BufferedReader())
	if err != nil {
		// Check if it's a timeout error - if so, don't close the connection, just retry
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			return false
		}
		// For any other error (including EOF), handle it normally
		return c.handleError(err, conn)
	}

	switch cmdSpec.Name {
	case protocol.CommandCreate:
		if cmdSpec.ArgsLength != 2 {
			return c.handleError(protocol.BadFormatErrorf("CREATE command must have 2 arguments"), conn)
		}
		return c.handleCreateCommand(ctx, conn)
	case protocol.CommandAppend:
		if cmdSpec.ArgsLength != 3 {
			return c.handleError(protocol.BadFormatErrorf("APPEND command must have 3 arguments"), conn)
		}
		return c.handleAppendCommand(ctx, conn)
	case protocol.CommandRead:
		if cmdSpec.ArgsLength != 2 {
			return c.handleError(protocol.BadFormatErrorf("READ command must have 2 arguments"), conn)
		}
		return c.handleReadCommand(ctx, conn)
	case protocol.CommandTrim:
		if cmdSpec.ArgsLength != 2 {
			return c.handleError(protocol.BadFormatErrorf("TRIM command must have 2 arguments"), conn)
		}
		return c.handleTrimCommand(ctx, conn)
	case protocol.CommandDelete:
		if cmdSpec.ArgsLength != 2 {
			return c.handleError(protocol.BadFormatErrorf("DELETE command must have 2 arguments"), conn)
		}
		return c.handleDeleteCommand(ctx, conn)
	default:
		c.slog.Error("unknown command", "command", cmdSpec.Name)
		return c.handleError(protocol.BadFormatErrorf("unknown command: %s", cmdSpec.Name), conn)
	}
}

func (c *Controller) handleCreateCommand(ctx context.Context, conn *tcp.Connection) (close bool) {
	cmd, err := c.decoder.DecodeCreateCommand(conn.BufferedReader())
	if err != nil {
		return c.handleError(err, conn)
	}

	err = c.store.CreateStream(ctx, cmd)
	if err != nil {
		return c.handleError(err, conn)
	}

	err = c.encoder.EncodeOK(conn.BufferedWriter())
	if err != nil {
		return c.handleError(err, conn)
	}

	return false
}

func (c *Controller) handleAppendCommand(ctx context.Context, conn *tcp.Connection) (close bool) {
	cmd, err := c.decoder.DecodeAppendCommand(conn.BufferedReader())
	if err != nil {
		return c.handleError(err, conn)
	}

	id, err := c.store.AppendRecords(ctx, cmd)
	if err != nil {
		return c.handleError(err, conn)
	}

	err = c.encoder.EncodeBulkString(conn.BufferedWriter(), id)
	if err != nil {
		return c.handleError(err, conn)
	}

	return false
}

func (c *Controller) handleReadCommand(ctx context.Context, conn *tcp.Connection) (close bool) {
	cmd, err := c.decoder.DecodeReadCommand(conn.BufferedReader())
	if err != nil {
		return c.handleError(err, conn)
	}

	records, err := c.store.ReadRecords(ctx, cmd)
	if err != nil {
		return c.handleError(err, conn)
	}

	err = c.encoder.EncodeRecords(conn.BufferedWriter(), records)
	if err != nil {
		return c.handleError(err, conn)
	}

	return false
}

func (c *Controller) handleTrimCommand(ctx context.Context, conn *tcp.Connection) (close bool) {
	cmd, err := c.decoder.DecodeTrimCommand(conn.BufferedReader())
	if err != nil {
		return c.handleError(err, conn)
	}

	err = c.store.TrimStream(ctx, cmd)
	if err != nil {
		return c.handleError(err, conn)
	}

	err = c.encoder.EncodeOK(conn.BufferedWriter())
	if err != nil {
		return c.handleError(err, conn)
	}

	return false
}

func (c *Controller) handleDeleteCommand(ctx context.Context, conn *tcp.Connection) (close bool) {
	cmd, err := c.decoder.DecodeDeleteCommand(conn.BufferedReader())
	if err != nil {
		return c.handleError(err, conn)
	}

	err = c.store.DeleteStream(ctx, cmd)
	if err != nil {
		return c.handleError(err, conn)
	}

	err = c.encoder.EncodeOK(conn.BufferedWriter())
	if err != nil {
		return c.handleError(err, conn)
	}

	return false
}

func (c *Controller) handleError(err error, conn *tcp.Connection) (close bool) {
	if perr, ok := protocol.IsProtocolError(err); ok {
		c.slog.Debug("protocol error", "error", perr)
		c.encoder.EncodeError(conn.BufferedWriter(), perr)
		return !perr.IsRecoverable() // If the error is not recoverable, close the connection
	}
	c.slog.Error("error decoding command", "error", err)
	return true
}
