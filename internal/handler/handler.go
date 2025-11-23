package handler

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"

	"github.com/hastyy/murakami/internal/assert"
	"github.com/hastyy/murakami/internal/protocol"
	"github.com/hastyy/murakami/internal/record"
	"github.com/hastyy/murakami/internal/server"
	"github.com/hastyy/murakami/internal/service"
)

type StreamService interface {
	CreateStream(ctx context.Context, req service.CreateRequest) error
	AppendRecords(ctx context.Context, req service.AppendRequest) (ts record.Timestamp, err error)
	ReadRecords(ctx context.Context, req service.ReadRequest) (records []record.Record, err error)
	TrimStream(ctx context.Context, req service.TrimRequest) error
	DeleteStream(ctx context.Context, req service.DeleteRequest) error
}

type CommandDecoder interface {
	DecodeNextCommand(reader *bufio.Reader) (protocol.CommandSpec, error)
	DecodeCreateCommand(reader *bufio.Reader) (protocol.CreateCommand, error)
	DecodeAppendCommand(reader *bufio.Reader) (protocol.AppendCommand, error)
	DecodeReadCommand(reader *bufio.Reader) (protocol.ReadCommand, error)
	DecodeTrimCommand(reader *bufio.Reader) (protocol.TrimCommand, error)
	DecodeDeleteCommand(reader *bufio.Reader) (protocol.DeleteCommand, error)
}

type ReplyEncoder interface {
	EncodeOK(writer io.Writer) error
	EncodeError(writer io.Writer, error protocol.Error) error
	EncodeBulkString(writer io.Writer, value string) error
	EncodeBulkBytesArray(writer io.Writer, values [][]byte) error
}

type Handler struct {
	streamService  StreamService
	commandDecoder CommandDecoder
	replyEncoder   ReplyEncoder
	logger         *slog.Logger
}

func NewHandler(streamService StreamService, commandDecoder CommandDecoder, replyEncoder ReplyEncoder, logger *slog.Logger) *Handler {
	return &Handler{
		streamService:  streamService,
		commandDecoder: commandDecoder,
		replyEncoder:   replyEncoder,
		logger:         logger,
	}
}

func (h *Handler) Handle(ctx context.Context, rw *server.ConnectionReadWriter) (close bool) {
	cmdSpec, err := h.commandDecoder.DecodeNextCommand(rw.Reader())
	if err != nil {
		// Check if it's a timeout error - if so, don't close the connection, just retry
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			return false
		}
		// For any other error (including EOF), handle it normally
		return h.handleError(err, rw)
	}

	switch cmdSpec.Name {
	case protocol.CommandCreate:
		return h.handleCreateCommand(ctx, rw)
	case protocol.CommandAppend:
		return h.handleAppendCommand(ctx, rw)
	case protocol.CommandRead:
		return h.handleReadCommand(ctx, rw)
	case protocol.CommandTrim:
		return h.handleTrimCommand(ctx, rw)
	case protocol.CommandDelete:
		return h.handleDeleteCommand(ctx, rw)
	default:
		h.logger.Error("unknown command", "command", cmdSpec.Name)
		h.replyEncoder.EncodeError(rw, protocol.Error{Code: protocol.ErrCodeBadFormat, Message: "unknown command: " + cmdSpec.Name})
		return true
	}
}

func (h *Handler) handleCreateCommand(ctx context.Context, rw *server.ConnectionReadWriter) (close bool) {
	cmd, err := h.commandDecoder.DecodeCreateCommand(rw.Reader())
	if err != nil {
		return h.handleError(err, rw)
	}

	if err = h.streamService.CreateStream(ctx, service.CreateRequest{
		StreamName: cmd.StreamName,
		Strategy:   service.TimestampStrategy(cmd.Options.TimestampStrategy),
	}); err != nil {
		if errors.Is(err, service.ErrStreamExists) {
			err = protocol.Error{Code: protocol.ErrCodeStreamExists, Message: fmt.Sprintf("stream %s already exists", cmd.StreamName)}
		}
		return h.handleError(err, rw)
	}

	err = h.replyEncoder.EncodeOK(rw)
	if err != nil {
		h.logger.Error("error encoding reply", "error", err)
		return true
	}

	return false
}

func (h *Handler) handleAppendCommand(ctx context.Context, rw *server.ConnectionReadWriter) (close bool) {
	cmd, err := h.commandDecoder.DecodeAppendCommand(rw.Reader())
	if err != nil {
		return h.handleError(err, rw)
	}

	var millis int64
	if cmd.Options.Timestamp != "" {
		tsParts := strings.Split(cmd.Options.Timestamp, "-")
		assert.OK(len(tsParts) == 2, "invalid timestamp reached handler: %s ; error: %v", cmd.Options.Timestamp, err)
		millis, err = strconv.ParseInt(tsParts[0], 10, 64)
		assert.OK(err == nil, "invalid timestamp reached handler: %s ; error: %v", cmd.Options.Timestamp, err)
	}

	var ts record.Timestamp
	if ts, err = h.streamService.AppendRecords(ctx, service.AppendRequest{
		StreamName: cmd.StreamName,
		Timestamp:  millis,
		Records:    cmd.Records,
	}); err != nil {
		if errors.Is(err, service.ErrUnknownStream) {
			err = protocol.Error{Code: protocol.ErrCodeUnknownStream, Message: fmt.Sprintf("stream %s not found", cmd.StreamName)}
		}
		if errors.Is(err, service.ErrBadFormatConflictingTimestampPolicy) {
			var errMsg string
			if millis != 0 {
				errMsg = "conflicting timestamp policy: client provided timestamp but stream is configured to use server timestamps"
			} else {
				errMsg = "conflicting timestamp policy: client did not provide timestamp but stream is configured to use client timestamps"
			}
			err = protocol.Error{Code: protocol.ErrCodeBadFormat, Message: errMsg}
		}
		if errors.Is(err, service.ErrBadFormatNonMonotonicTimestamp) {
			err = protocol.Error{Code: protocol.ErrCodeBadFormat, Message: fmt.Sprintf("non-monotonic timestamp: %s", cmd.Options.Timestamp)}
		}
		return h.handleError(err, rw)
	}

	err = h.replyEncoder.EncodeBulkString(rw, ts.String())
	if err != nil {
		h.logger.Error("error encoding reply", "error", err)
		return true
	}

	return false
}

func (h *Handler) handleReadCommand(ctx context.Context, rw *server.ConnectionReadWriter) (close bool) {
	cmd, err := h.commandDecoder.DecodeReadCommand(rw.Reader())
	if err != nil {
		return h.handleError(err, rw)
	}

	tsParts := strings.Split(cmd.Options.MinTimestamp, "-")
	assert.OK(len(tsParts) == 2, "invalid timestamp reached handler: %s ; error: %v", cmd.Options.MinTimestamp, err)
	millis, err := strconv.ParseInt(tsParts[0], 10, 64)
	assert.OK(err == nil, "invalid timestamp reached handler: %s ; error: %v", cmd.Options.MinTimestamp, err)
	seqNum, err := strconv.Atoi(tsParts[1])
	assert.OK(err == nil, "invalid timestamp reached handler: %s ; error: %v", cmd.Options.MinTimestamp, err)

	var records []record.Record
	if records, err = h.streamService.ReadRecords(ctx, service.ReadRequest{
		StreamName: cmd.StreamName,
		Count:      cmd.Options.Count,
		Block:      cmd.Options.Block,
		MinTimestamp: record.Timestamp{
			Millis: millis,
			SeqNum: seqNum,
		},
	}); err != nil {
		if errors.Is(err, service.ErrUnknownStream) {
			err = protocol.Error{Code: protocol.ErrCodeUnknownStream, Message: fmt.Sprintf("stream %s not found", cmd.StreamName)}
		}
		return h.handleError(err, rw)
	}

	bulkBytes := make([][]byte, 0, 2*len(records))
	for _, record := range records {
		bulkBytes = append(bulkBytes, []byte(record.Timestamp.String()))
		bulkBytes = append(bulkBytes, record.Value)
	}

	err = h.replyEncoder.EncodeBulkBytesArray(rw, bulkBytes)
	if err != nil {
		h.logger.Error("error encoding reply", "error", err)
		return true
	}

	return false
}

func (h *Handler) handleTrimCommand(ctx context.Context, rw *server.ConnectionReadWriter) (close bool) {
	cmd, err := h.commandDecoder.DecodeTrimCommand(rw.Reader())
	if err != nil {
		return h.handleError(err, rw)
	}

	tsParts := strings.Split(cmd.Options.Until, "-")
	assert.OK(len(tsParts) == 2, "invalid timestamp reached handler: %s ; error: %v", cmd.Options.Until, err)
	millis, err := strconv.ParseInt(tsParts[0], 10, 64)
	assert.OK(err == nil, "invalid timestamp reached handler: %s ; error: %v", cmd.Options.Until, err)
	seqNum, err := strconv.Atoi(tsParts[1])
	assert.OK(err == nil, "invalid timestamp reached handler: %s ; error: %v", cmd.Options.Until, err)

	if err = h.streamService.TrimStream(ctx, service.TrimRequest{
		StreamName: cmd.StreamName,
		Until: record.Timestamp{
			Millis: millis,
			SeqNum: seqNum,
		},
	}); err != nil {
		if errors.Is(err, service.ErrUnknownStream) {
			err = protocol.Error{Code: protocol.ErrCodeUnknownStream, Message: fmt.Sprintf("stream %s not found", cmd.StreamName)}
		}
		return h.handleError(err, rw)
	}

	err = h.replyEncoder.EncodeOK(rw)
	if err != nil {
		h.logger.Error("error encoding reply", "error", err)
		return true
	}

	return false
}

func (h *Handler) handleDeleteCommand(ctx context.Context, rw *server.ConnectionReadWriter) (close bool) {
	cmd, err := h.commandDecoder.DecodeDeleteCommand(rw.Reader())
	if err != nil {
		return h.handleError(err, rw)
	}

	if err = h.streamService.DeleteStream(ctx, service.DeleteRequest{
		StreamName: cmd.StreamName,
	}); err != nil {
		if errors.Is(err, service.ErrUnknownStream) {
			err = protocol.Error{Code: protocol.ErrCodeUnknownStream, Message: fmt.Sprintf("stream %s not found", cmd.StreamName)}
		}
		return h.handleError(err, rw)
	}

	err = h.replyEncoder.EncodeOK(rw)
	if err != nil {
		h.logger.Error("error encoding reply", "error", err)
		return true
	}

	return false
}

func (h *Handler) handleError(err error, rw *server.ConnectionReadWriter) (close bool) {
	if perr, ok := protocol.IsProtocolError(err); ok {
		h.logger.Debug("protocol error", "error", perr)
		h.replyEncoder.EncodeError(rw, perr)
		// For now all protocol errors should close the connection.
		return true
	}
	h.logger.Error("error decoding command", "error", err)
	return true
}
