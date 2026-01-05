package controller

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/hastyy/murakami/internal/assert"
	"github.com/hastyy/murakami/internal/logging"
	"github.com/hastyy/murakami/internal/protocol"
	"github.com/hastyy/murakami/internal/tcp"
)

// StreamStore is the storage layer interface for managing streams and their records.
// It provides operations for stream lifecycle management (create, delete) and record operations
// (append, read, trim). Implementations must be safe for concurrent access and handle context
// cancellation appropriately. All operations return protocol.Error for expected error conditions
// (stream exists, stream not found, non-monotonic ID, etc.) which the controller can encode
// and send back to clients.
type StreamStore interface {
	// CreateStream creates a new stream with the given name.
	// Returns protocol.Error with ErrCodeStreamExists if the stream already exists.
	// Returns protocol.Error with ErrCodeLimits if stream name validation fails.
	CreateStream(ctx context.Context, cmd protocol.CreateCommand) error

	// AppendRecords appends one or more records to an existing stream.
	// If cmd.Options.MillisID is provided, it is used as the timestamp component of the generated ID.
	// Otherwise, the current system time is used. The sequence number is auto-incremented.
	// Returns the ID of the last appended record (format: "milliseconds-sequence").
	// Returns protocol.Error with ErrCodeUnknownStream if the stream does not exist.
	// Returns protocol.Error with ErrCodeNonMonotonicID if the provided ID is not greater than the last ID.
	// Returns protocol.Error with ErrCodeLimits if record validation fails.
	AppendRecords(ctx context.Context, cmd protocol.AppendCommand) (id string, err error)

	// ReadRecords reads records from a stream starting from cmd.Options.MinID.
	// Returns up to cmd.Options.Count records. If cmd.Options.Block is non-zero and no records
	// are immediately available, blocks for up to that duration waiting for new records.
	// Returns an empty slice if no records match the criteria (not an error).
	// Returns protocol.Error with ErrCodeUnknownStream if the stream does not exist.
	ReadRecords(ctx context.Context, cmd protocol.ReadCommand) ([]protocol.Record, error)

	// TrimStream removes all records with IDs less than cmd.Options.MinID from the stream.
	// This is a permanent deletion operation used to reclaim storage space.
	// Returns protocol.Error with ErrCodeUnknownStream if the stream does not exist.
	TrimStream(ctx context.Context, cmd protocol.TrimCommand) error

	// DeleteStream permanently deletes a stream and all its records.
	// Returns protocol.Error with ErrCodeUnknownStream if the stream does not exist.
	DeleteStream(ctx context.Context, cmd protocol.DeleteCommand) error
}

// CommandDecoder is the protocol decoder interface for parsing S3P commands from client byte streams.
// It validates command structure, enforces protocol rules (zero-length bulk strings not allowed,
// even-length options arrays, case-insensitive command/option matching), and applies configurable
// limits (stream name length, record count/size, payload size).
type CommandDecoder interface {
	// DecodeNextCommand decodes the next command header from the reader and returns a CommandSpec.
	// The CommandSpec contains the command name and the number of remaining arguments, allowing the
	// caller to route to the appropriate command-specific decoder method.
	// Returns protocol.Error with ErrCodeBadFormat for malformed protocol structures.
	// Returns protocol.Error with ErrCodeLimits if command name exceeds maximum length.
	DecodeNextCommand(r *bufio.Reader) (protocol.CommandSpec, error)

	// DecodeCreateCommand decodes a CREATE command from the reader.
	// It reads the stream name and options array, validates the stream name length,
	// and rejects any unrecognized options.
	// Pre-condition: DecodeNextCommand() returned a CREATE spec with ArgsLength == 2.
	// Returns protocol.Error with ErrCodeLimits for invalid stream name.
	// Returns protocol.Error with ErrCodeBadFormat for invalid options.
	DecodeCreateCommand(r *bufio.Reader) (protocol.CreateCommand, error)

	// DecodeAppendCommand decodes an APPEND command from the reader.
	// It reads the stream name, options array, and records array.
	// Validates stream name length, ID format (if provided), and enforces limits on record count
	// and total payload size.
	// Pre-condition: DecodeNextCommand() returned an APPEND spec with ArgsLength == 3.
	// Returns protocol.Error with ErrCodeLimits for validation failures.
	// Returns protocol.Error with ErrCodeBadFormat for invalid protocol structure or ID format.
	DecodeAppendCommand(r *bufio.Reader) (protocol.AppendCommand, error)

	// DecodeReadCommand decodes a READ command from the reader.
	// It reads the stream name and options array. Supports three optional parameters:
	// COUNT (number of records to read), BLOCK (blocking timeout in milliseconds),
	// and MIN_ID (minimum ID to read from, must be a valid ID format).
	// Pre-condition: DecodeNextCommand() returned a READ spec with ArgsLength == 2.
	// Returns protocol.Error with ErrCodeLimits for out-of-range option values.
	// Returns protocol.Error with ErrCodeBadFormat for invalid option format or unknown options.
	DecodeReadCommand(r *bufio.Reader) (protocol.ReadCommand, error)

	// DecodeTrimCommand decodes a TRIM command from the reader.
	// It reads the stream name and options array. Requires one mandatory parameter:
	// MIN_ID (minimum ID to trim from, must be a valid ID format).
	// Pre-condition: DecodeNextCommand() returned a TRIM spec with ArgsLength == 2.
	// Returns protocol.Error with ErrCodeLimits for invalid stream name.
	// Returns protocol.Error with ErrCodeBadFormat if MIN_ID is missing, invalid, or unknown options present.
	DecodeTrimCommand(r *bufio.Reader) (protocol.TrimCommand, error)

	// DecodeDeleteCommand decodes a DELETE command from the reader.
	// It reads the stream name and an options array. Validates the stream name length.
	// Pre-condition: DecodeNextCommand() returned a DELETE spec with ArgsLength == 2.
	// Returns protocol.Error with ErrCodeLimits for invalid stream name.
	// Returns protocol.Error with ErrCodeBadFormat for non-empty options.
	DecodeDeleteCommand(r *bufio.Reader) (protocol.DeleteCommand, error)
}

// ReplyEncoder is the protocol encoder interface for serializing S3P responses to client byte streams.
// It provides methods to encode success responses (OK, bulk strings, records) and error responses
// according to the S3P protocol specification. All methods write to a buffered writer and return
// any I/O errors encountered. The caller is responsible for flushing the writer after encoding.
type ReplyEncoder interface {
	// EncodeOK encodes a simple OK response ("+OK\r\n").
	// Used for successful CREATE, TRIM, and DELETE operations that don't return data.
	// Returns any I/O error encountered during writing.
	EncodeOK(w *bufio.Writer) error

	// EncodeError encodes a protocol error response in the format "-ERROR_CODE message\r\n".
	// The error code and message are extracted from the protocol.Error.
	// Used when operations fail with expected error conditions (stream exists, limits exceeded, etc.).
	// Returns any I/O error encountered during writing.
	EncodeError(w *bufio.Writer, error protocol.Error) error

	// EncodeBulkString encodes a bulk string response in the format "$length\r\nvalue\r\n".
	// Used for successful APPEND operations to return the generated record ID.
	// Returns any I/O error encountered during writing.
	EncodeBulkString(w *bufio.Writer, value string) error

	// EncodeRecords encodes an array of records in the format "*count*2\r\n$id_len\r\nid\r\n$val_len\r\nvalue\r\n...".
	// Each record is encoded as two consecutive bulk strings: the ID followed by the value.
	// Used for successful READ operations to return matching records.
	// Returns any I/O error encountered during writing.
	EncodeRecords(w *bufio.Writer, records []protocol.Record) error
}

// Controller is the main request handler that orchestrates command processing.
// It decodes client commands using the CommandDecoder, dispatches them to the StreamStore,
// and encodes responses using the ReplyEncoder. The controller handles protocol-level
// error conditions (converting protocol.Error to wire format) and determines whether
// connections should be closed based on error recoverability. All command processing
// is routed through the Handle method which performs command-specific validation and delegation.
type Controller struct {
	store   StreamStore
	decoder CommandDecoder
	encoder ReplyEncoder
	slog    *slog.Logger
}

// New creates a new Controller with the given dependencies.
func New(store StreamStore, decoder CommandDecoder, encoder ReplyEncoder, slog *slog.Logger) *Controller {
	assert.NonNil(store, "StreamStore is required")
	assert.NonNil(decoder, "CommandDecoder is required")
	assert.NonNil(encoder, "ReplyEncoder is required")
	assert.NonNil(slog, "Logger is required")

	return &Controller{
		store:   store,
		decoder: decoder,
		encoder: encoder,
		slog:    slog,
	}
}

// Handle processes a single client command from the connection.
// It decodes the command header, validates the argument count, and routes to the appropriate
// command handler (CREATE, APPEND, READ, TRIM, DELETE). Returns an error if the connection should
// be closed (due to non-recoverable errors or non-protocol errors), nil otherwise. Timeout errors
// are handled specially and do not close the connection, allowing the client to retry.
func (c *Controller) Handle(ctx context.Context, conn *tcp.Connection) error {
	cmdSpec, err := c.decoder.DecodeNextCommand(conn.BufferedReader())
	if err != nil {
		// Check if it's a timeout error - if so, don't close the connection, just retry
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			logging.Record(ctx, slog.String("recoverable_error", "timeout"))
			return nil
		}
		// For any other error (including EOF), handle it normally
		return c.handleError(ctx, err, conn)
	}

	logging.Record(ctx, slog.Group("command_spec",
		slog.String("name", cmdSpec.Name),
		slog.Int("args_length", cmdSpec.ArgsLength)))

	switch cmdSpec.Name {
	case protocol.CommandCreate:
		if cmdSpec.ArgsLength != 2 {
			return c.handleError(ctx, protocol.BadFormatErrorf("CREATE command must have 2 arguments"), conn)
		}
		return c.handleCreateCommand(ctx, conn)
	case protocol.CommandAppend:
		if cmdSpec.ArgsLength != 3 {
			return c.handleError(ctx, protocol.BadFormatErrorf("APPEND command must have 3 arguments"), conn)
		}
		return c.handleAppendCommand(ctx, conn)
	case protocol.CommandRead:
		if cmdSpec.ArgsLength != 2 {
			return c.handleError(ctx, protocol.BadFormatErrorf("READ command must have 2 arguments"), conn)
		}
		return c.handleReadCommand(ctx, conn)
	case protocol.CommandTrim:
		if cmdSpec.ArgsLength != 2 {
			return c.handleError(ctx, protocol.BadFormatErrorf("TRIM command must have 2 arguments"), conn)
		}
		return c.handleTrimCommand(ctx, conn)
	case protocol.CommandDelete:
		if cmdSpec.ArgsLength != 2 {
			return c.handleError(ctx, protocol.BadFormatErrorf("DELETE command must have 2 arguments"), conn)
		}
		return c.handleDeleteCommand(ctx, conn)
	default:
		c.slog.Error("unknown command", "command", cmdSpec.Name)
		return c.handleError(ctx, protocol.BadFormatErrorf("unknown command: %s", cmdSpec.Name), conn)
	}
}

func (c *Controller) handleCreateCommand(ctx context.Context, conn *tcp.Connection) error {
	cmd, err := c.decoder.DecodeCreateCommand(conn.BufferedReader())
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	logging.Record(ctx, slog.Group("command",
		slog.String("name", protocol.CommandCreate),
		slog.Group("args",
			slog.String("stream_name", cmd.StreamName))))

	err = c.store.CreateStream(ctx, cmd)
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	err = c.encoder.EncodeOK(conn.BufferedWriter())
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	return nil
}

func (c *Controller) handleAppendCommand(ctx context.Context, conn *tcp.Connection) error {
	cmd, err := c.decoder.DecodeAppendCommand(conn.BufferedReader())
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	logging.Record(ctx, slog.Group("command",
		"name", protocol.CommandAppend,
		slog.Group("args",
			slog.String("stream_name", cmd.StreamName),
			slog.Int("records_count", len(cmd.Records)),
			slog.Group("options", slog.String("millis_id", cmd.Options.MillisID)))))

	id, err := c.store.AppendRecords(ctx, cmd)
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	logging.Record(ctx, slog.String("last_appended_id", id))

	err = c.encoder.EncodeBulkString(conn.BufferedWriter(), id)
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	return nil
}

func (c *Controller) handleReadCommand(ctx context.Context, conn *tcp.Connection) error {
	cmd, err := c.decoder.DecodeReadCommand(conn.BufferedReader())
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	logging.Record(ctx, slog.Group("command",
		slog.String("name", protocol.CommandRead),
		slog.Group("args",
			slog.String("stream_name", cmd.StreamName),
			slog.Group("options",
				slog.Int("count", cmd.Options.Count),
				slog.Int("block_ms", int(cmd.Options.Block.Milliseconds())),
				slog.String("min_id", cmd.Options.MinID)))))

	records, err := c.store.ReadRecords(ctx, cmd)
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	logging.Record(ctx, slog.Int("records_count", len(records)))

	err = c.encoder.EncodeRecords(conn.BufferedWriter(), records)
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	return nil
}

func (c *Controller) handleTrimCommand(ctx context.Context, conn *tcp.Connection) error {
	cmd, err := c.decoder.DecodeTrimCommand(conn.BufferedReader())
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	logging.Record(ctx, slog.Group("command",
		slog.String("name", protocol.CommandTrim),
		slog.Group("args",
			slog.String("stream_name", cmd.StreamName),
			slog.Group("options",
				slog.String("min_id", cmd.Options.MinID)))))

	err = c.store.TrimStream(ctx, cmd)
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	err = c.encoder.EncodeOK(conn.BufferedWriter())
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	return nil
}

func (c *Controller) handleDeleteCommand(ctx context.Context, conn *tcp.Connection) error {
	cmd, err := c.decoder.DecodeDeleteCommand(conn.BufferedReader())
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	logging.Record(ctx, slog.Group("command",
		slog.String("name", protocol.CommandDelete),
		slog.Group("args",
			slog.String("stream_name", cmd.StreamName))))

	err = c.store.DeleteStream(ctx, cmd)
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	err = c.encoder.EncodeOK(conn.BufferedWriter())
	if err != nil {
		return c.handleError(ctx, err, conn)
	}

	return nil
}

func (c *Controller) handleError(ctx context.Context, err error, conn *tcp.Connection) error {
	if perr, ok := protocol.IsProtocolError(err); ok {
		logging.Record(ctx, slog.Group("protocol_error",
			slog.String("code", string(perr.Code)),
			slog.String("message", perr.Message),
			slog.Bool("recoverable", perr.IsRecoverable())))
		encodeErr := c.encoder.EncodeError(conn.BufferedWriter(), perr)
		if encodeErr != nil {
			logging.Record(ctx, slog.String("encoding_error", encodeErr.Error()))
			// Return the encoding error to close the connection if we can't send error response
			return fmt.Errorf("failed to encode error response: %w", encodeErr)
		}
		// If the error is non-recoverable, wrap it in a normal error and return it
		if !perr.IsRecoverable() {
			return fmt.Errorf("non-recoverable protocol error: %w", perr)
		}
		// If the error is recoverable, return nil (don't close the connection)
		return nil
	}
	// For non-protocol errors, return them as-is
	return err
}
