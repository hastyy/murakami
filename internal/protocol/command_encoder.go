package protocol

import (
	"bufio"
	"strconv"
)

// CommandEncoder is the client-side protocol encoder that serializes S3P commands to server byte streams.
// It encodes commands according to the S3P protocol specification (arrays of bulk strings).
// All methods write to a buffered writer and return any I/O errors encountered.
// The caller is responsible for flushing the writer after encoding.
type CommandEncoder struct{}

// NewCommandEncoder creates a new CommandEncoder.
func NewCommandEncoder() *CommandEncoder {
	return &CommandEncoder{}
}

// EncodeCreateCommand encodes a CREATE command to the provided writer.
// The command is encoded as an array with 3 elements: command name, stream name, and options.
// The format is: *3\r\n$6\r\nCREATE\r\n$<stream_len>\r\n<stream_name>\r\n*0\r\n
// The options array is currently empty as no options are defined for CREATE in the current protocol version.
// Returns any I/O error encountered during writing.
func (e *CommandEncoder) EncodeCreateCommand(w *bufio.Writer, cmd CreateCommand) error {
	// Write array header: *3\r\n
	err := writeArrayHeader(w, 3)
	if err != nil {
		return err
	}

	// Write command name as bulk string: $6\r\nCREATE\r\n
	err = writeBulkString(w, CommandCreate)
	if err != nil {
		return err
	}

	// Write stream name as bulk string: $<len>\r\n<name>\r\n
	err = writeBulkString(w, cmd.StreamName)
	if err != nil {
		return err
	}

	// Write empty options array: *0\r\n
	err = writeArrayHeader(w, 0)
	if err != nil {
		return err
	}

	return nil
}

// EncodeAppendCommand encodes an APPEND command to the provided writer.
// The command is encoded as an array with 4 elements: command name, stream name, options, and records.
// The format is: *4\r\n$6\r\nAPPEND\r\n$<stream_len>\r\n<stream_name>\r\n*<opts>\r\n...\r\n*<recs>\r\n...\r\n
// Options may include the ID option (timestamp part only).
// Records array must contain at least one element.
// Returns any I/O error encountered during writing.
func (e *CommandEncoder) EncodeAppendCommand(w *bufio.Writer, cmd AppendCommand) error {
	// Write array header: *4\r\n
	err := writeArrayHeader(w, 4)
	if err != nil {
		return err
	}

	// Write command name as bulk string
	err = writeBulkString(w, CommandAppend)
	if err != nil {
		return err
	}

	// Write stream name as bulk string
	err = writeBulkString(w, cmd.StreamName)
	if err != nil {
		return err
	}

	// Write options array
	err = e.encodeAppendOptions(w, cmd.Options)
	if err != nil {
		return err
	}

	// Write records array
	err = e.encodeRecordsArray(w, cmd.Records)
	if err != nil {
		return err
	}

	return nil
}

// EncodeReadCommand encodes a READ command to the provided writer.
// The command is encoded as an array with 3 elements: command name, stream name, and options.
// The format is: *3\r\n$4\r\nREAD\r\n$<stream_len>\r\n<stream_name>\r\n*<opts>\r\n...\r\n
// Options may include COUNT, BLOCK, and MIN_ID.
// Returns any I/O error encountered during writing.
func (e *CommandEncoder) EncodeReadCommand(w *bufio.Writer, cmd ReadCommand) error {
	// Write array header: *3\r\n
	err := writeArrayHeader(w, 3)
	if err != nil {
		return err
	}

	// Write command name as bulk string
	err = writeBulkString(w, CommandRead)
	if err != nil {
		return err
	}

	// Write stream name as bulk string
	err = writeBulkString(w, cmd.StreamName)
	if err != nil {
		return err
	}

	// Write options array
	err = e.encodeReadOptions(w, cmd.Options)
	if err != nil {
		return err
	}

	return nil
}

// TODO: add check to error if cmd doesn't contain MIN_ID
// EncodeTrimCommand encodes a TRIM command to the provided writer.
// The command is encoded as an array with 3 elements: command name, stream name, and options.
// The format is: *3\r\n$4\r\nTRIM\r\n$<stream_len>\r\n<stream_name>\r\n*<opts>\r\n...\r\n
// Options must include the MIN_ID option (mandatory for TRIM).
// Returns any I/O error encountered during writing.
func (e *CommandEncoder) EncodeTrimCommand(w *bufio.Writer, cmd TrimCommand) error {
	// Write array header: *3\r\n
	err := writeArrayHeader(w, 3)
	if err != nil {
		return err
	}

	// Write command name as bulk string
	err = writeBulkString(w, CommandTrim)
	if err != nil {
		return err
	}

	// Write stream name as bulk string
	err = writeBulkString(w, cmd.StreamName)
	if err != nil {
		return err
	}

	// Write options array (MIN_ID is mandatory)
	err = e.encodeTrimOptions(w, cmd.Options)
	if err != nil {
		return err
	}

	return nil
}

// EncodeDeleteCommand encodes a DELETE command to the provided writer.
// The command is encoded as an array with 3 elements: command name, stream name, and options.
// The format is: *3\r\n$6\r\nDELETE\r\n$<stream_len>\r\n<stream_name>\r\n*0\r\n
// The options array is currently empty as no options are defined for DELETE in the current protocol version.
// Returns any I/O error encountered during writing.
func (e *CommandEncoder) EncodeDeleteCommand(w *bufio.Writer, cmd DeleteCommand) error {
	// Write array header: *3\r\n
	err := writeArrayHeader(w, 3)
	if err != nil {
		return err
	}

	// Write command name as bulk string
	err = writeBulkString(w, CommandDelete)
	if err != nil {
		return err
	}

	// Write stream name as bulk string
	err = writeBulkString(w, cmd.StreamName)
	if err != nil {
		return err
	}

	// Write empty options array: *0\r\n
	err = writeArrayHeader(w, 0)
	if err != nil {
		return err
	}

	return nil
}

// encodeAppendOptions encodes the options for an APPEND command
func (e *CommandEncoder) encodeAppendOptions(w *bufio.Writer, opts AppendCommandOptions) error {
	if opts.MillisID == "" {
		// Empty options array
		return writeArrayHeader(w, 0)
	}

	// Options array with 2 elements (key-value pair)
	err := writeArrayHeader(w, 2)
	if err != nil {
		return err
	}

	err = writeBulkString(w, optionID)
	if err != nil {
		return err
	}

	err = writeBulkString(w, opts.MillisID)
	if err != nil {
		return err
	}

	return nil
}

// encodeReadOptions encodes the options for a READ command
func (e *CommandEncoder) encodeReadOptions(w *bufio.Writer, opts ReadCommandOptions) error {
	// Count non-default options
	optCount := 0
	if opts.Count != 0 {
		optCount += 2
	}
	if opts.Block != 0 {
		optCount += 2
	}
	if opts.MinID != "" {
		optCount += 2
	}

	err := writeArrayHeader(w, optCount)
	if err != nil {
		return err
	}

	if opts.Count != 0 {
		err = writeBulkString(w, optionCount)
		if err != nil {
			return err
		}
		err = writeBulkString(w, strconv.Itoa(opts.Count))
		if err != nil {
			return err
		}
	}

	if opts.Block != 0 {
		err = writeBulkString(w, optionBlock)
		if err != nil {
			return err
		}
		// Convert duration to milliseconds
		err = writeBulkString(w, strconv.FormatInt(opts.Block.Milliseconds(), 10))
		if err != nil {
			return err
		}
	}

	if opts.MinID != "" {
		err = writeBulkString(w, optionMinID)
		if err != nil {
			return err
		}
		err = writeBulkString(w, opts.MinID)
		if err != nil {
			return err
		}
	}

	return nil
}

// encodeTrimOptions encodes the options for a TRIM command
func (e *CommandEncoder) encodeTrimOptions(w *bufio.Writer, opts TrimCommandOptions) error {
	// MIN_ID is mandatory for TRIM
	err := writeArrayHeader(w, 2)
	if err != nil {
		return err
	}

	err = writeBulkString(w, optionMinID)
	if err != nil {
		return err
	}

	err = writeBulkString(w, opts.MinID)
	if err != nil {
		return err
	}

	return nil
}

// encodeRecordsArray encodes the records array for an APPEND command
func (e *CommandEncoder) encodeRecordsArray(w *bufio.Writer, records [][]byte) error {
	err := writeArrayHeader(w, len(records))
	if err != nil {
		return err
	}

	for _, record := range records {
		err = writeBulkBytes(w, record)
		if err != nil {
			return err
		}
	}

	return nil
}
