package protocol

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hastyy/murakami/internal/assert"
)

const (
	// Command names
	CommandCreate = "CREATE"
	CommandAppend = "APPEND"
	CommandRead   = "READ"
	CommandTrim   = "TRIM"
	CommandDelete = "DELETE"

	// CREATE Options
	optionTimestampStrategy       = "TIMESTAMP_STRATEGY"
	optionTimestampStrategyServer = "server"
	optionTimestampStrategyClient = "client"

	// APPEND Options
	optionTimestamp = "TIMESTAMP"

	// READ Options
	optionCount        = "COUNT"
	optionBlock        = "BLOCK"
	optionMinTimestamp = "MIN_TIMESTAMP"

	// TRIM Options
	optionUntil = "UNTIL"

	// DELETE Options - None yet
)

type CommandSpec struct {
	Name       string
	ArgsLength int
}

type CreateCommand struct {
	StreamName string
	Options    CreateCommandOptions
}

type CreateCommandOptions struct {
	TimestampStrategy string
}

type AppendCommand struct {
	StreamName string
	Options    AppendCommandOptions
	Records    [][]byte
}

type AppendCommandOptions struct {
	Timestamp string
}

type ReadCommand struct {
	StreamName string
	Options    ReadCommandOptions
}

type ReadCommandOptions struct {
	Count        int
	Block        time.Duration
	MinTimestamp string
}

type TrimCommand struct {
	StreamName string
	Options    TrimCommandOptions
}

type TrimCommandOptions struct {
	Until string
}

type DeleteCommand struct {
	StreamName string
	Options    DeleteCommandOptions
}

type DeleteCommandOptions struct{}

type BufferPool interface {
	Get() []byte
	Put([]byte)
}

// CommandDecoder is meant to be used by the server to decoder commands from the client.
type CommandDecoder struct {
	cfg     Config
	bufPool BufferPool
}

// NewCommandDecoder returns a new CommandDecoder.
func NewCommandDecoder(cfg Config) *CommandDecoder {
	assert.NonNil(cfg.BufPool, "BufferPool is required")

	// Set defaults
	cfg = cfg.CombineWith(DefaultConfig)

	assert.OK(cfg.MaxStreamNameLength > 0, "MaxStreamNameLength must be > 0, got %d", cfg.MaxStreamNameLength)
	assert.OK(cfg.MaxRecordsPerAppend > 0, "MaxRecordsPerAppend must be > 0, got %d", cfg.MaxRecordsPerAppend)
	assert.OK(cfg.MaxRecordSize > 0, "MaxRecordSize must be > 0, got %d", cfg.MaxRecordSize)
	assert.OK(cfg.MaxAppendPayloadSize > 0, "MaxAppendPayloadSize must be > 0, got %d", cfg.MaxAppendPayloadSize)
	assert.OK(cfg.MaxReadCount > 0, "MaxReadCount must be > 0, got %d", cfg.MaxReadCount)
	assert.OK(cfg.MaxReadBlock > 0, "MaxReadBlock must be > 0, got %d", cfg.MaxReadBlock)

	return &CommandDecoder{
		cfg:     cfg,
		bufPool: cfg.BufPool,
	}
}

func (d *CommandDecoder) DecodeNextCommand(reader *bufio.Reader) (CommandSpec, error) {
	buf := d.bufPool.Get()
	defer d.bufPool.Put(buf)

	arrLength, err := readArrayLength(reader)
	if err != nil {
		return CommandSpec{}, err
	}

	// Minimum number of arguments for a command is 3.
	// E.g. Command name + stream name + options.
	if arrLength < 3 {
		return CommandSpec{}, Error{ErrCodeBadFormat, "top-level command array must have at least 3 elements"}
	}

	cmdName, err := readBulkString(reader, buf)
	if err != nil {
		return CommandSpec{}, err
	}

	return CommandSpec{
		Name:       cmdName,
		ArgsLength: arrLength - 1,
	}, nil
}

// DecodeCreateCommand decodes a CREATE command from the reader.
// Pre-condition: d.DecodeNextCommand() read a CREATE spec with argsLength == 2.
func (d *CommandDecoder) DecodeCreateCommand(reader *bufio.Reader) (CreateCommand, error) {
	buf := d.bufPool.Get()
	defer d.bufPool.Put(buf)

	// Slice the buffer to guarantee we have the exact space we need since downstream calls will try to fill the buffer
	// and use its length as the limit for how many bytes they can read.
	buf = buf[:d.cfg.MaxStreamNameLength]

	streamName, err := readBulkString(reader, buf)
	if err != nil {
		return CreateCommand{}, err
	}

	// Options array
	arrLength, err := readArrayLength(reader)
	if err != nil {
		return CreateCommand{}, err
	}

	// All entries must come in key/value pairs
	if !isPair(arrLength) {
		return CreateCommand{}, Error{ErrCodeBadFormat, "options array must have an even number of elements"}
	}

	// CREATE command with default options
	cmd := CreateCommand{
		StreamName: streamName,
		Options: CreateCommandOptions{
			TimestampStrategy: optionTimestampStrategyServer,
		},
	}

	// Read each key/value pair in options and apply to cmd
	for range numberOfPairs(arrLength) {
		var key, value string

		if key, err = readBulkString(reader, buf); err != nil {
			return CreateCommand{}, err
		}
		if value, err = readBulkString(reader, buf); err != nil {
			return CreateCommand{}, err
		}

		key = strings.ToUpper(key)
		value = strings.ToLower(value)

		// Match known options and values
		switch key {
		case optionTimestampStrategy:
			if value != optionTimestampStrategyServer && value != optionTimestampStrategyClient {
				return CreateCommand{}, Error{ErrCodeBadFormat, fmt.Sprintf("invalid value for option %s: %s", key, value)}
			}
			cmd.Options.TimestampStrategy = value
		default:
			return CreateCommand{}, Error{ErrCodeBadFormat, "unknown option: " + key}
		}
	}

	return cmd, nil
}

// DecodeAppendCommand decodes an APPEND command from the reader.
// Pre-condition: d.DecodeNextCommand() read an APPEND spec with argsLength == 3.
func (d *CommandDecoder) DecodeAppendCommand(reader *bufio.Reader) (AppendCommand, error) {
	buf := d.bufPool.Get()
	defer d.bufPool.Put(buf)

	// Slice the buffer to guarantee we have the exact space we need since downstream calls will try to fill the buffer
	// and use its length as the limit for how many bytes they can read.
	buf = buf[:d.cfg.MaxStreamNameLength]

	streamName, err := readBulkString(reader, buf)
	if err != nil {
		return AppendCommand{}, err
	}

	// Options array
	optionsLength, err := readArrayLength(reader)
	if err != nil {
		return AppendCommand{}, err
	}

	// All entries must come in key/value pairs
	if !isPair(optionsLength) {
		return AppendCommand{}, Error{ErrCodeBadFormat, "options array must have an even number of elements"}
	}

	cmd := AppendCommand{
		StreamName: streamName,
	}

	// Read each key/value pair in options and apply to cmd
	for range numberOfPairs(optionsLength) {
		var key, value string

		if key, err = readBulkString(reader, buf); err != nil {
			return AppendCommand{}, err
		}
		if value, err = readBulkString(reader, buf); err != nil {
			return AppendCommand{}, err
		}

		key = strings.ToUpper(key)
		value = strings.ToLower(value)

		switch key {
		case optionTimestamp:
			if !isValidTimestamp(value) {
				return AppendCommand{}, Error{ErrCodeBadFormat, fmt.Sprintf("invalid value for option %s: %s", key, value)}
			}
			cmd.Options.Timestamp = value
		default:
			return AppendCommand{}, Error{ErrCodeBadFormat, "unknown option: " + key}
		}
	}

	// Records array
	recordCount, err := readArrayLength(reader)
	if err != nil {
		return AppendCommand{}, err
	}

	if recordCount < 1 {
		return AppendCommand{}, Error{ErrCodeBadFormat, "records array must have at least 1 element"}
	}

	if recordCount > d.cfg.MaxRecordsPerAppend {
		return AppendCommand{}, Error{ErrCodeLimits, fmt.Sprintf("records array must have at most %d elements", d.cfg.MaxRecordsPerAppend)}
	}

	// Loop variables
	limit := d.cfg.MaxAppendPayloadSize
	records := make([][]byte, 0, recordCount)

	// Register a defer to put the buffer back in the pool if an error is found.
	var errFound bool
	defer func() {
		if errFound {
			// Returns all the buffers used in processing previous (successfull) records.
			for i := range len(records) {
				d.bufPool.Put(records[i])
			}
		}
	}()

	// Read each record
	for range recordCount {
		// Get a buffer from the pool and apply the max record size limit
		bufRecord := d.bufPool.Get()[:d.cfg.MaxRecordSize]

		// Read the record
		record, err := readBulkBytesWithLimit(reader, bufRecord, limit)
		if err != nil {
			errFound = true
			// Returns the buffer used in processing the current failed record, which never got added to the records slice
			// and therefore wouldn't get returned by the defer.
			d.bufPool.Put(bufRecord)
			return AppendCommand{}, err
		}

		// Update the limit
		limit -= len(record)

		// Add the record to the records slice
		records = append(records, record)
	}

	// Set the records
	cmd.Records = records

	return cmd, nil
}

// DecodeReadCommand decodes a READ command from the reader.
// Pre-condition: d.DecodeNextCommand() read a READ spec with argsLength == 2.
func (d *CommandDecoder) DecodeReadCommand(reader *bufio.Reader) (ReadCommand, error) {
	buf := d.bufPool.Get()
	defer d.bufPool.Put(buf)

	// Slice the buffer to guarantee we have the exact space we need since downstream calls will try to fill the buffer
	// and use its length as the limit for how many bytes they can read.
	buf = buf[:d.cfg.MaxStreamNameLength]

	streamName, err := readBulkString(reader, buf)
	if err != nil {
		return ReadCommand{}, err
	}

	// Options array
	optionsLength, err := readArrayLength(reader)
	if err != nil {
		return ReadCommand{}, err
	}

	// All entries must come in key/value pairs
	if !isPair(optionsLength) {
		return ReadCommand{}, Error{ErrCodeBadFormat, "options array must have an even number of elements"}
	}

	cmd := ReadCommand{
		StreamName: streamName,
		Options: ReadCommandOptions{
			Count:        d.cfg.MaxReadCount,
			MinTimestamp: "0-0",
		},
	}

	// Read each key/value pair in options and apply to cmd
	for range numberOfPairs(optionsLength) {
		var key, value string

		if key, err = readBulkString(reader, buf); err != nil {
			return ReadCommand{}, err
		}
		if value, err = readBulkString(reader, buf); err != nil {
			return ReadCommand{}, err
		}

		key = strings.ToUpper(key)
		value = strings.ToLower(value)

		switch key {
		case optionCount:
			var count int
			if count, err = strconv.Atoi(value); err != nil {
				return ReadCommand{}, Error{ErrCodeBadFormat, fmt.Sprintf("invalid value for option %s: %s", key, value)}
			}
			if count < 1 || count > d.cfg.MaxReadCount {
				return ReadCommand{}, Error{ErrCodeLimits, fmt.Sprintf("%s must be between 1 and %d, got %d", key, d.cfg.MaxReadCount, count)}
			}
			cmd.Options.Count = count
		case optionBlock:
			var block time.Duration
			if block, err = time.ParseDuration(value); err != nil {
				return ReadCommand{}, Error{ErrCodeBadFormat, fmt.Sprintf("invalid value for option %s: %s", key, value)}
			}
			if block < 0 || block > d.cfg.MaxReadBlock {
				return ReadCommand{}, Error{ErrCodeLimits, fmt.Sprintf("%s must be between 0 and %d, got %d", key, d.cfg.MaxReadBlock, block)}
			}
			cmd.Options.Block = block
		case optionMinTimestamp:
			if !isValidTimestamp(value) {
				return ReadCommand{}, Error{ErrCodeBadFormat, fmt.Sprintf("invalid value for option %s: %s", key, value)}
			}
			cmd.Options.MinTimestamp = value
		default:
			return ReadCommand{}, Error{ErrCodeBadFormat, "unknown option: " + key}
		}
	}

	return cmd, nil
}

// DecodeTrimCommand decodes a TRIM command from the reader.
// Pre-condition: d.DecodeNextCommand() read a TRIM spec with argsLength == 2.
func (d *CommandDecoder) DecodeTrimCommand(reader *bufio.Reader) (TrimCommand, error) {
	buf := d.bufPool.Get()
	defer d.bufPool.Put(buf)

	// Slice the buffer to guarantee we have the exact space we need since downstream calls will try to fill the buffer
	// and use its length as the limit for how many bytes they can read.
	buf = buf[:d.cfg.MaxStreamNameLength]

	streamName, err := readBulkString(reader, buf)
	if err != nil {
		return TrimCommand{}, err
	}

	// Options array
	optionsLength, err := readArrayLength(reader)
	if err != nil {
		return TrimCommand{}, err
	}

	// All entries must come in key/value pairs
	if !isPair(optionsLength) {
		return TrimCommand{}, Error{ErrCodeBadFormat, "options array must have an even number of elements"}
	}

	cmd := TrimCommand{
		StreamName: streamName,
	}

	// Read each key/value pair in options and apply to cmd
	for range numberOfPairs(optionsLength) {
		var key, value string

		if key, err = readBulkString(reader, buf); err != nil {
			return TrimCommand{}, err
		}
		if value, err = readBulkString(reader, buf); err != nil {
			return TrimCommand{}, err
		}

		key = strings.ToUpper(key)
		value = strings.ToLower(value)

		switch key {
		case optionUntil:
			if !isValidTimestamp(value) {
				return TrimCommand{}, Error{ErrCodeBadFormat, fmt.Sprintf("invalid value for option %s: %s", key, value)}
			}
			cmd.Options.Until = value
		default:
			return TrimCommand{}, Error{ErrCodeBadFormat, "unknown option: " + key}
		}
	}

	if cmd.Options.Until == "" {
		return TrimCommand{}, Error{ErrCodeBadFormat, "option " + optionUntil + " is required"}
	}

	return cmd, nil
}

// DecodeDeleteCommand decodes a DELETE command from the reader.
// Pre-condition: d.DecodeNextCommand() read a DELETE spec with argsLength == 2.
func (d *CommandDecoder) DecodeDeleteCommand(reader *bufio.Reader) (DeleteCommand, error) {
	buf := d.bufPool.Get()
	defer d.bufPool.Put(buf)

	// Slice the buffer to guarantee we have the exact space we need since downstream calls will try to fill the buffer
	// and use its length as the limit for how many bytes they can read.
	buf = buf[:d.cfg.MaxStreamNameLength]

	streamName, err := readBulkString(reader, buf)
	if err != nil {
		return DeleteCommand{}, err
	}

	// Options array
	optionsLength, err := readArrayLength(reader)
	if err != nil {
		return DeleteCommand{}, err
	}

	// All entries must come in key/value pairs
	if !isPair(optionsLength) {
		return DeleteCommand{}, Error{ErrCodeBadFormat, "options array must have an even number of elements"}
	}

	cmd := DeleteCommand{
		StreamName: streamName,
	}

	// Read each key/value pair in options and apply to cmd
	for range numberOfPairs(optionsLength) {
		var key, value string

		if key, err = readBulkString(reader, buf); err != nil {
			return DeleteCommand{}, err
		}
		if value, err = readBulkString(reader, buf); err != nil {
			return DeleteCommand{}, err
		}

		key = strings.ToUpper(key)
		value = strings.ToLower(value)

		switch key {
		// No options for DELETE command yet
		default:
			return DeleteCommand{}, Error{ErrCodeBadFormat, "unknown option: " + key}
		}
	}

	return cmd, nil
}
