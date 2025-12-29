package protocol

import (
	"bufio"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/hastyy/murakami/internal/unit"
)

const (
	// TODO: Make all these configurable
	DEFAULT_MAX_STREAM_NAME_LENGTH  = 256
	DEFAULT_MAX_BULK_STRING_LENGTH  = 256
	DEFAULT_MAX_RECORDS_PER_APPEND  = 1_000
	DEFAULT_MAX_APPEND_PAYLOAD_SIZE = 1 * unit.MiB
	DEFAULT_MAX_READ_COUNT          = 1_000
	DEFAULT_MAX_READ_BLOCK          = 10 * time.Second
)

// BufferProvider represents a buffer pool that manages pre-allocated []byte buffers.
// It enables efficient buffer reuse and reduces allocation pressure during command decoding.
type BufferProvider interface {
	// Get returns a buffer from the internal pool with at least the requested size.
	// The implementation may return a buffer larger than requested (e.g., from size classes).
	// If the pool is empty, the underlying implementation may choose to block waiting
	// for a buffer to become available, or allocate new buffers on demand.
	Get(bufferSize int) (buf []byte)

	// Put returns a buffer to the pool for reuse.
	// If the pool is full, the underlying implementation may discard the buffer.
	// This call never blocks.
	// Putting the same buffer into the pool more than once concurrently is not expected
	// and can cause problems; the underlying implementation may or may not detect this.
	Put(buf []byte)
}

// CommandSpec represents a parsed command header from the protocol stream.
// It contains the command name and the number of remaining arguments (excluding the command name itself).
// This allows the caller to route to the appropriate command-specific decoder.
type CommandSpec struct {
	Name       string
	ArgsLength int
}

// Option represents a key-value pair from the options array.
type Option struct {
	Key   string
	Value string
}

// CommandDecoder is the server-side protocol decoder that parses S3P commands from client byte streams.
// It validates command structure, enforces protocol rules (zero-length bulk strings not allowed,
// even-length options arrays, case-insensitive command/option matching), and applies configurable
// limits (stream name length, record count/size, payload size). The decoder uses a BufferProvider
// for efficient buffer management when reading record payloads.
type CommandDecoder struct {
	bufProvider BufferProvider
}

// NewCommandDecoder creates a new CommandDecoder.
func NewCommandDecoder(bufProvider BufferProvider) *CommandDecoder {
	return &CommandDecoder{
		bufProvider: bufProvider,
	}
}

// DecodeNextCommand decodes the next command from the reader and returns a CommandSpec.
// It validates that the command array has at least 3 elements (command name, stream name, and options).
// It returns a CommandSpec containing the command name and the number of remaining arguments.
func (d *CommandDecoder) DecodeNextCommand(r *bufio.Reader) (CommandSpec, error) {
	arrLength, err := readArrayLength(r)
	if err != nil {
		return CommandSpec{}, err
	}

	// Minimum number of arguments for a command is 3.
	// E.g. Command name + stream name + options.
	if arrLength < 3 {
		return CommandSpec{}, BadFormatErrorf("top-level command array must have at least 3 elements")
	}

	cmd, err := readBulkString(r, DEFAULT_MAX_BULK_STRING_LENGTH)
	if err != nil {
		return CommandSpec{}, err
	}

	return CommandSpec{
		Name:       cmd,
		ArgsLength: arrLength - 1,
	}, nil
}

// DecodeCreateCommand decodes a CREATE command from the reader.
// It reads the stream name and options array, validates the stream name length,
// and rejects any unrecognized options.
// Pre-condition: DecodeNextCommand() returned a CREATE spec with argsLength == 2.
func (d *CommandDecoder) DecodeCreateCommand(r *bufio.Reader) (CreateCommand, error) {
	streamName, err := readBulkString(r, DEFAULT_MAX_STREAM_NAME_LENGTH)
	if err != nil {
		return CreateCommand{}, err
	}

	optionPairs, err := optionsDecoder(r)
	if err != nil {
		return CreateCommand{}, err
	}

	for option, err := range optionPairs {
		if err != nil {
			return CreateCommand{}, err
		}

		switch option.Key {
		default:
			// No known options exist for CREATE command yet
			return CreateCommand{}, BadFormatErrorf("unknown option: %s for CREATE command", option.Key)
		}
	}

	return CreateCommand{
		StreamName: streamName,
	}, nil
}

// DecodeAppendCommand decodes an APPEND command from the reader.
// It reads the stream name, options array, and records array. It validates the stream name length,
// the ID option (if present, must be a valid millisecond timestamp), and enforces limits on
// record count and payload size. Buffers for records are obtained from the BufferProvider.
// Pre-condition: DecodeNextCommand() returned an APPEND spec with argsLength == 3.
func (d *CommandDecoder) DecodeAppendCommand(r *bufio.Reader) (AppendCommand, error) {
	streamName, err := readBulkString(r, DEFAULT_MAX_STREAM_NAME_LENGTH)
	if err != nil {
		return AppendCommand{}, err
	}

	cmd := AppendCommand{
		StreamName: streamName,
	}

	optionPairs, err := optionsDecoder(r)
	if err != nil {
		return AppendCommand{}, err
	}

	for option, err := range optionPairs {
		if err != nil {
			return AppendCommand{}, err
		}

		switch option.Key {
		case optionID:
			if !IsValidIDMillis(option.Value) {
				return AppendCommand{}, BadFormatErrorf("invalid value for option %s: %s", option.Key, option.Value)
			}
			cmd.Options.MillisID = option.Value
		default:
			return AppendCommand{}, BadFormatErrorf("unknown option: %s for APPEND command", option.Key)
		}
	}

	records, err := d.readRecords(r)
	if err != nil {
		return AppendCommand{}, err
	}

	cmd.Records = records

	return cmd, nil
}

// DecodeReadCommand decodes a READ command from the reader.
// It reads the stream name and options array. It validates the stream name length and supports
// three optional parameters: COUNT (number of records to read, 1-1000), BLOCK (blocking timeout
// in milliseconds, 0-10000), and MIN_ID (minimum ID to read from, must be a valid ID).
// Pre-condition: DecodeNextCommand() returned a READ spec with argsLength == 2.
func (d *CommandDecoder) DecodeReadCommand(r *bufio.Reader) (ReadCommand, error) {
	streamName, err := readBulkString(r, DEFAULT_MAX_STREAM_NAME_LENGTH)
	if err != nil {
		return ReadCommand{}, err
	}

	cmd := ReadCommand{
		StreamName: streamName,
		Options: ReadCommandOptions{
			Count: DEFAULT_MAX_READ_COUNT,
			Block: 0,
			MinID: "0-0",
		},
	}

	optionPairs, err := optionsDecoder(r)
	if err != nil {
		return ReadCommand{}, err
	}

	for option, err := range optionPairs {
		if err != nil {
			return ReadCommand{}, err
		}

		switch option.Key {
		case optionCount:
			count, err := strconv.Atoi(option.Value)
			if err != nil {
				return ReadCommand{}, BadFormatErrorf("invalid value for option %s: %s", option.Key, option.Value)
			}
			if count < 1 || count > DEFAULT_MAX_READ_COUNT {
				return ReadCommand{}, LimitsErrorf("value for option %s must be between 1 and %d, got %d", option.Key, DEFAULT_MAX_READ_COUNT, count)
			}
			cmd.Options.Count = count
		case optionBlock:
			nblock, err := strconv.Atoi(option.Value)
			if err != nil {
				return ReadCommand{}, BadFormatErrorf("invalid value for option %s: %s", option.Key, option.Value)
			}
			block := time.Duration(nblock) * time.Millisecond
			if block < 0 || block > DEFAULT_MAX_READ_BLOCK {
				return ReadCommand{}, LimitsErrorf("value for option %s must be between 0 and %d, got %d", option.Key, DEFAULT_MAX_READ_BLOCK, block)
			}
			cmd.Options.Block = block
		case optionMinID:
			if !IsValidID(option.Value) {
				return ReadCommand{}, BadFormatErrorf("invalid value for option %s: %s", option.Key, option.Value)
			}
			cmd.Options.MinID = option.Value
		default:
			return ReadCommand{}, BadFormatErrorf("unknown option: %s for READ command", option.Key)
		}
	}

	return cmd, nil
}

// DecodeTrimCommand decodes a TRIM command from the reader.
// It reads the stream name and options array. It validates the stream name length and requires
// the MIN_ID option to be present with a valid ID format (milliseconds-sequence).
// Pre-condition: DecodeNextCommand() returned a TRIM spec with argsLength == 2.
func (d *CommandDecoder) DecodeTrimCommand(r *bufio.Reader) (TrimCommand, error) {
	streamName, err := readBulkString(r, DEFAULT_MAX_STREAM_NAME_LENGTH)
	if err != nil {
		return TrimCommand{}, err
	}

	cmd := TrimCommand{
		StreamName: streamName,
	}

	optionPairs, err := optionsDecoder(r)
	if err != nil {
		return TrimCommand{}, err
	}

	var hasMinID bool
	for option, err := range optionPairs {
		if err != nil {
			return TrimCommand{}, err
		}

		switch option.Key {
		case optionMinID:
			hasMinID = true
			if !IsValidID(option.Value) {
				return TrimCommand{}, BadFormatErrorf("invalid value for option %s: %s", option.Key, option.Value)
			}
			cmd.Options.MinID = option.Value
		default:
			return TrimCommand{}, BadFormatErrorf("unknown option: %s for TRIM command", option.Key)
		}
	}

	if !hasMinID {
		return TrimCommand{}, BadFormatErrorf("option %s is required for TRIM command", optionMinID)
	}

	return cmd, nil
}

// DecodeDeleteCommand decodes a DELETE command from the reader.
// It reads the stream name and options array. It validates the stream name length and currently
// accepts no options (options array must be empty for now, for forward compatibility).
// Pre-condition: DecodeNextCommand() returned a DELETE spec with argsLength == 2.
func (d *CommandDecoder) DecodeDeleteCommand(r *bufio.Reader) (DeleteCommand, error) {
	streamName, err := readBulkString(r, DEFAULT_MAX_STREAM_NAME_LENGTH)
	if err != nil {
		return DeleteCommand{}, err
	}

	optionPairs, err := optionsDecoder(r)
	if err != nil {
		return DeleteCommand{}, err
	}

	for option, err := range optionPairs {
		if err != nil {
			return DeleteCommand{}, err
		}

		switch option.Key {
		default:
			// No known options exist for DELETE command yet
			return DeleteCommand{}, BadFormatErrorf("unknown option: %s for DELETE command", option.Key)
		}
	}

	return DeleteCommand{
		StreamName: streamName,
	}, nil
}

// optionsDecoder reads an options array from the protocol stream and returns an iterator.
// It validates that the array has an even number of elements (key/value pairs).
// Keys are automatically transformed to uppercase for case-insensitive matching.
func optionsDecoder(r *bufio.Reader) (iter.Seq2[Option, error], error) {
	optionsLength, err := readArrayLength(r)
	if err != nil {
		return nil, err
	}

	// All entries must come in key/value pairs
	if !isPair(optionsLength) {
		return nil, BadFormatErrorf("options array must have an even number of elements")
	}

	return func(yield func(Option, error) bool) {
		for range numberOfPairs(optionsLength) {
			key, err := readBulkString(r, DEFAULT_MAX_BULK_STRING_LENGTH)
			if err != nil {
				if !yield(Option{}, err) {
					return
				}
			}

			value, err := readBulkString(r, DEFAULT_MAX_BULK_STRING_LENGTH)
			if err != nil {
				if !yield(Option{}, err) {
					return
				}
			}

			// Transform key to uppercase for case-insensitive matching
			key = strings.ToUpper(key)

			if !yield(Option{Key: key, Value: value}, nil) {
				return
			}
		}
	}, nil
}

func (d *CommandDecoder) readRecords(r *bufio.Reader) ([][]byte, error) {
	recordCount, err := readArrayLength(r)
	if err != nil {
		return nil, err
	}

	if recordCount < 1 {
		return nil, LimitsErrorf("records array must have at least 1 element, got %d", recordCount)
	}

	if recordCount > DEFAULT_MAX_RECORDS_PER_APPEND {
		return nil, LimitsErrorf("records array must have at most %d elements, got %d", DEFAULT_MAX_RECORDS_PER_APPEND, recordCount)
	}

	limit := DEFAULT_MAX_APPEND_PAYLOAD_SIZE
	records := make([][]byte, 0, recordCount)

	// Register a defer to put the buffer back in the pool if an error is found.
	var errFound bool
	defer func() {
		if errFound {
			for _, buf := range records {
				d.bufProvider.Put(buf)
			}
		}
	}()

	for range recordCount {
		length, err := readBulkBytesLengthWithLimit(r, limit)
		if err != nil {
			errFound = true
			return nil, err
		}

		buf := d.bufProvider.Get(length)

		n, err := readNBulkBytes(r, buf, length)
		if err != nil {
			errFound = true
			// Returns the buffer used in processing the current failed record, which never got added to the records slice
			// and therefore wouldn't get returned by the defer.
			d.bufProvider.Put(buf)
			return nil, err
		}

		err = consumeCRLF(r)
		if err != nil {
			errFound = true
			// Returns the buffer used in processing the current failed record, which never got added to the records slice
			// and therefore wouldn't get returned by the defer.
			d.bufProvider.Put(buf)
			return nil, err
		}

		// Update the limit
		limit -= n

		// Add the record to the records slice
		records = append(records, buf[:n])
	}

	return records, nil
}
