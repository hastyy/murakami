package protocol

import (
	"bufio"
	"io"
	"strings"
	"testing"

	"github.com/hastyy/murakami/internal/util"
	"github.com/stretchr/testify/require"
)

func TestDecodeNextCommand(t *testing.T) {
	require := require.New(t)

	d := NewCommandDecoder(Config{
		BufPool: &mockBufferPool{},
	})

	tests := []struct {
		name         string
		reader       *bufio.Reader
		expectedSpec CommandSpec
		expectedErr  error
	}{
		{
			name:   "successfully decode CREATE command spec",
			reader: readerFrom("*3\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{
				Name:       "CREATE",
				ArgsLength: 2,
			},
			expectedErr: nil,
		},
		{
			name:   "successfully decode APPEND command spec",
			reader: readerFrom("*4\r\n$6\r\nAPPEND\r\n$6\r\nstream\r\n*0\r\n$6\r\nrecord\r\n"),
			expectedSpec: CommandSpec{
				Name:       "APPEND",
				ArgsLength: 3,
			},
			expectedErr: nil,
		},
		{
			name:   "successfully decode READ command spec",
			reader: readerFrom("*3\r\n$4\r\nREAD\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{
				Name:       "READ",
				ArgsLength: 2,
			},
			expectedErr: nil,
		},
		{
			name:   "successfully decode TRIM command spec",
			reader: readerFrom("*3\r\n$4\r\nTRIM\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{
				Name:       "TRIM",
				ArgsLength: 2,
			},
			expectedErr: nil,
		},
		{
			name:   "successfully decode DELETE command spec",
			reader: readerFrom("*3\r\n$6\r\nDELETE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{
				Name:       "DELETE",
				ArgsLength: 2,
			},
			expectedErr: nil,
		},
		{
			name:   "successfully decode unknown command spec",
			reader: readerFrom("*3\r\n$7\r\nUNKNOWN\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{
				Name:       "UNKNOWN",
				ArgsLength: 2,
			},
			expectedErr: nil,
		},
		{
			name:         "command not starting with * should return an error",
			reader:       readerFrom("1\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "found unexpected byte(1) while expecting byte(*)"},
		},
		{
			name:         "non-numeric array length should return an error",
			reader:       readerFrom("*a\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "invalid length: a"},
		},
		{
			name:         "empty array length should return an error",
			reader:       readerFrom("*\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "unexpected empty line"},
		},
		{
			name:         "negative array length should return an error",
			reader:       readerFrom("*-1\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "invalid length: -1"},
		},
		{
			name:         "non-integer array length should return an error",
			reader:       readerFrom("*1.5\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "invalid length: 1.5"},
		},
		{
			name:         "top-level array with less than 3 elements should return an error",
			reader:       readerFrom("*2\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "top-level command array must have at least 3 elements"},
		},
		{
			name:         "command name not a bulk string should return an error",
			reader:       readerFrom("*3\r\n1\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "found unexpected byte(1) while expecting byte($)"},
		},
		{
			name:         "zero-length bulk string should return an error",
			reader:       readerFrom("*3\r\n$0\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "bulk string length must be greater than 0, got 0"},
		},
		{
			name:         "bad length - larger bulk string should return an error",
			reader:       readerFrom("*3\r\n$6\r\nCREATEasd\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "found unexpected byte(a) while expecting byte(\r)"},
		},
		{
			name:         "bad length - smaller bulk string should return an error",
			reader:       readerFrom("*3\r\n$6\r\nCREAT\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "found unexpected byte(\n) while expecting byte(\r)"},
		},
		{
			name:         "only CRLF is valid, not CR-only",
			reader:       readerFrom("*3\r\n$6\r\nCREATE\r$6\r\nstream\r\n*0\r"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "found unexpected byte($) while expecting byte(\n)"},
		},
		{
			name:         "only CRLF is valid, not LF-only",
			reader:       readerFrom("*3\r\n$6\r\nCREATE\n$6\r\nstream\r\n*0\n"),
			expectedSpec: CommandSpec{},
			expectedErr:  Error{ErrCodeBadFormat, "found unexpected byte(\n) while expecting byte(\r)"},
		},
		{
			name:         "reader byte stream ends too early",
			reader:       readerFrom("*3\r\n$6\r\nCREATE\r"),
			expectedSpec: CommandSpec{},
			expectedErr:  io.EOF,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec, err := d.DecodeNextCommand(test.reader)

			if test.expectedErr != nil {
				require.Error(err)
				require.Equal(test.expectedErr.Error(), err.Error())
			} else {
				require.NoError(err)
			}

			// should be "" and 0 respectively if there was an error
			require.Equal(test.expectedSpec.Name, spec.Name)
			require.Equal(test.expectedSpec.ArgsLength, spec.ArgsLength)
		})
	}
}

func TestDecodeCreateCommand(t *testing.T) {
	require := require.New(t)

	d := NewCommandDecoder(Config{
		BufPool: &mockBufferPool{},
	})

	tests := []struct {
		name        string
		reader      *bufio.Reader
		expectedCmd CreateCommand
		expectedErr error
	}{
		{
			name:   "successfully decode CREATE command with default options",
			reader: readerFrom("$6\r\nstream\r\n*0\r\n"),
			expectedCmd: CreateCommand{
				StreamName: "stream",
				Options: CreateCommandOptions{
					TimestampStrategy: optionTimestampStrategyServer,
				},
			},
			expectedErr: nil,
		},
		{
			name:   "successfully decode CREATE command with client timestamp strategy",
			reader: readerFrom("$6\r\nstream\r\n*2\r\n$18\r\nTIMESTAMP_STRATEGY\r\n$6\r\nclient\r\n"),
			expectedCmd: CreateCommand{
				StreamName: "stream",
				Options: CreateCommandOptions{
					TimestampStrategy: optionTimestampStrategyClient,
				},
			},
			expectedErr: nil,
		},
		{
			name:   "successfully decode CREATE command with server timestamp strategy",
			reader: readerFrom("$6\r\nstream\r\n*2\r\n$18\r\nTIMESTAMP_STRATEGY\r\n$6\r\nserver\r\n"),
			expectedCmd: CreateCommand{
				StreamName: "stream",
				Options: CreateCommandOptions{
					TimestampStrategy: optionTimestampStrategyServer,
				},
			},
			expectedErr: nil,
		},
		{
			name:   "successfully decode CREATE command with mixed case timestamp strategy option key",
			reader: readerFrom("$6\r\nstream\r\n*2\r\n$18\r\ntImEsTaMp_StRaTeGy\r\n$6\r\nclient\r\n"),
			expectedCmd: CreateCommand{
				StreamName: "stream",
				Options: CreateCommandOptions{
					TimestampStrategy: optionTimestampStrategyClient,
				},
			},
			expectedErr: nil,
		},
		{
			name:   "successfully decode CREATE command with mixed case timestamp strategy option value",
			reader: readerFrom("$6\r\nstream\r\n*2\r\n$18\r\nTIMESTAMP_STRATEGY\r\n$6\r\nSeRvEr\r\n"),
			expectedCmd: CreateCommand{
				StreamName: "stream",
				Options: CreateCommandOptions{
					TimestampStrategy: optionTimestampStrategyServer,
				},
			},
			expectedErr: nil,
		},
		{
			name:        "should fail when creating stream with empty name",
			reader:      readerFrom("$0\r\n\r\n*0\r\n"),
			expectedCmd: CreateCommand{},
			expectedErr: Error{ErrCodeBadFormat, "bulk string length must be greater than 0, got 0"},
		},
		{
			name:        "should fail when creating stream with name length greater than configured max length",
			reader:      readerFrom("$257\r\n" + strings.Repeat("a", 257) + "\r\n*0\r\n"),
			expectedCmd: CreateCommand{},
			expectedErr: Error{ErrCodeLimits, "bulk string length must be less than or equal to 256, got 257"},
		},
		{
			name:        "should fail when options array has odd number of elements",
			reader:      readerFrom("$6\r\nstream\r\n*3\r\n$18\r\nTIMESTAMP_STRATEGY\r\n$6\r\nclient\r\n$7\r\ninvalid\r\n"),
			expectedCmd: CreateCommand{},
			expectedErr: Error{ErrCodeBadFormat, "options array must have an even number of elements"},
		},
		{
			name:        "should fail when options contains an invalid key",
			reader:      readerFrom("$6\r\nstream\r\n*2\r\n$11\r\nINVALID_KEY\r\n$5\r\nvalue\r\n"),
			expectedCmd: CreateCommand{},
			expectedErr: Error{ErrCodeBadFormat, "unknown option: INVALID_KEY"},
		},
		{
			name:        "should fail when timestamp strategy has an invalid value",
			reader:      readerFrom("$6\r\nstream\r\n*2\r\n$18\r\nTIMESTAMP_STRATEGY\r\n$7\r\ninvalid\r\n"),
			expectedCmd: CreateCommand{},
			expectedErr: Error{ErrCodeBadFormat, "invalid value for option TIMESTAMP_STRATEGY: invalid"},
		},
		{
			name:   "should use last value when same key appears multiple times",
			reader: readerFrom("$6\r\nstream\r\n*4\r\n$18\r\nTIMESTAMP_STRATEGY\r\n$6\r\nserver\r\n$18\r\nTIMESTAMP_STRATEGY\r\n$6\r\nclient\r\n"),
			expectedCmd: CreateCommand{
				StreamName: "stream",
				Options: CreateCommandOptions{
					TimestampStrategy: optionTimestampStrategyClient,
				},
			},
			expectedErr: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := d.DecodeCreateCommand(test.reader)

			if test.expectedErr != nil {
				require.Error(err)
				require.Equal(test.expectedErr.Error(), err.Error())
			} else {
				require.NoError(err)
			}

			require.Equal(test.expectedCmd.StreamName, cmd.StreamName)
			require.Equal(test.expectedCmd.Options.TimestampStrategy, cmd.Options.TimestampStrategy)
		})
	}
}

func TestDecodeAppendCommand(t *testing.T) {
	require := require.New(t)

	d := NewCommandDecoder(Config{
		BufPool:              &mockBufferPool{},
		MaxRecordsPerAppend:  5,
		MaxRecordSize:        128 * util.Byte,
		MaxAppendPayloadSize: 256 * util.Byte,
	})

	tests := []struct {
		name        string
		reader      *bufio.Reader
		expectedCmd AppendCommand
		expectedErr error
	}{
		{
			name:   "successfully decode APPEND with no options and 1 record",
			reader: readerFrom("$6\r\nstream\r\n*0\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{
				StreamName: "stream",
				Options:    AppendCommandOptions{},
				Records:    [][]byte{[]byte("hello")},
			},
			expectedErr: nil,
		},
		{
			name:   "successfully decode APPEND with timestamp option and multiple records",
			reader: readerFrom("$6\r\nstream\r\n*2\r\n$9\r\nTIMESTAMP\r\n$15\r\n1700000001234-0\r\n*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n$3\r\nfoo\r\n"),
			expectedCmd: AppendCommand{
				StreamName: "stream",
				Options: AppendCommandOptions{
					Timestamp: "1700000001234-0",
				},
				Records: [][]byte{[]byte("hello"), []byte("world"), []byte("foo")},
			},
			expectedErr: nil,
		},
		{
			name:   "should work correctly with mixed case timestamp option key",
			reader: readerFrom("$6\r\nstream\r\n*2\r\n$9\r\nTiMeStAmP\r\n$15\r\n1700000001234-0\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{
				StreamName: "stream",
				Options: AppendCommandOptions{
					Timestamp: "1700000001234-0",
				},
				Records: [][]byte{[]byte("hello")},
			},
			expectedErr: nil,
		},
		{
			name:   "should work well with a large timestamp",
			reader: readerFrom("$6\r\nstream\r\n*2\r\n$9\r\nTIMESTAMP\r\n$21\r\n9999999999999999-9999\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{
				StreamName: "stream",
				Options: AppendCommandOptions{
					Timestamp: "9999999999999999-9999",
				},
				Records: [][]byte{[]byte("hello")},
			},
			expectedErr: nil,
		},
		{
			name:        "should fail when appending to stream with empty name",
			reader:      readerFrom("$0\r\n\r\n*0\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeBadFormat, "bulk string length must be greater than 0, got 0"},
		},
		{
			name:        "should fail when appending to stream with name length greater than configured max length",
			reader:      readerFrom("$257\r\n" + strings.Repeat("a", 257) + "\r\n*0\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeLimits, "bulk string length must be less than or equal to 256, got 257"},
		},
		{
			name:        "should fail when appending with a batch of records over the configured limit",
			reader:      readerFrom("$6\r\nstream\r\n*0\r\n*6\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeLimits, "records array must have at most 5 elements"},
		},
		{
			name:        "should fail when a record goes over the configured max size",
			reader:      readerFrom("$6\r\nstream\r\n*0\r\n*1\r\n$129\r\n" + strings.Repeat("x", 129) + "\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeLimits, "bulk string length must be less than or equal to 128, got 129"},
		},
		{
			name:        "should fail when total bytes in records exceeds the configured aggregate limit",
			reader:      readerFrom("$6\r\nstream\r\n*0\r\n*3\r\n$100\r\n" + strings.Repeat("a", 100) + "\r\n$100\r\n" + strings.Repeat("b", 100) + "\r\n$57\r\n" + strings.Repeat("c", 57) + "\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeLimits, "bulk string bytes limit reached"},
		},
		{
			name:   "should use last value when same option key appears multiple times",
			reader: readerFrom("$6\r\nstream\r\n*4\r\n$9\r\nTIMESTAMP\r\n$15\r\n1700000001234-0\r\n$9\r\nTIMESTAMP\r\n$15\r\n1700000005678-1\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{
				StreamName: "stream",
				Options: AppendCommandOptions{
					Timestamp: "1700000005678-1",
				},
				Records: [][]byte{[]byte("hello")},
			},
			expectedErr: nil,
		},
		{
			name:        "should fail when options contains an unknown key",
			reader:      readerFrom("$6\r\nstream\r\n*2\r\n$11\r\nUNKNOWN_KEY\r\n$5\r\nvalue\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeBadFormat, "unknown option: UNKNOWN_KEY"},
		},
		{
			name:        "should fail with odd number of elements in options array",
			reader:      readerFrom("$6\r\nstream\r\n*3\r\n$9\r\nTIMESTAMP\r\n$15\r\n1700000001234-0\r\n$7\r\ninvalid\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeBadFormat, "options array must have an even number of elements"},
		},
		{
			name:        "should fail with invalid timestamp value missing dash",
			reader:      readerFrom("$6\r\nstream\r\n*2\r\n$9\r\nTIMESTAMP\r\n$13\r\n1700000001234\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeBadFormat, "invalid value for option TIMESTAMP: 1700000001234"},
		},
		{
			name:        "should fail with invalid timestamp value missing number after dash",
			reader:      readerFrom("$6\r\nstream\r\n*2\r\n$9\r\nTIMESTAMP\r\n$14\r\n1700000001234-\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeBadFormat, "invalid value for option TIMESTAMP: 1700000001234-"},
		},
		{
			name:        "should fail with invalid timestamp value containing letters",
			reader:      readerFrom("$6\r\nstream\r\n*2\r\n$9\r\nTIMESTAMP\r\n$15\r\n1700000abc234-0\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeBadFormat, "invalid value for option TIMESTAMP: 1700000abc234-0"},
		},
		{
			name:        "should fail with record count being 0",
			reader:      readerFrom("$6\r\nstream\r\n*0\r\n*0\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeBadFormat, "records array must have at least 1 element"},
		},
		{
			name:        "should fail with negative record count",
			reader:      readerFrom("$6\r\nstream\r\n*0\r\n*-1\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeBadFormat, "invalid length: -1"},
		},
		{
			name:        "should fail with a zero length record",
			reader:      readerFrom("$6\r\nstream\r\n*0\r\n*2\r\n$5\r\nhello\r\n$0\r\n\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeBadFormat, "bulk string length must be greater than 0, got 0"},
		},
		{
			name:        "should fail if one of the records is not a bulk string",
			reader:      readerFrom("$6\r\nstream\r\n*0\r\n*2\r\n$5\r\nhello\r\n+world\r\n"),
			expectedCmd: AppendCommand{},
			expectedErr: Error{ErrCodeBadFormat, "found unexpected byte(+) while expecting byte($)"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := d.DecodeAppendCommand(test.reader)

			if test.expectedErr != nil {
				require.Error(err)
				require.Equal(test.expectedErr.Error(), err.Error())
			} else {
				require.NoError(err)
			}

			require.Equal(test.expectedCmd.StreamName, cmd.StreamName)
			require.Equal(test.expectedCmd.Options.Timestamp, cmd.Options.Timestamp)
			require.Equal(len(test.expectedCmd.Records), len(cmd.Records))
			for i := range test.expectedCmd.Records {
				require.Equal(test.expectedCmd.Records[i], cmd.Records[i])
			}
		})
	}
}

func TestDecodeAppendCommand_BufferPool_ReceivesAllBuffersBackUponError(t *testing.T) {
	require := require.New(t)

	pool := &mockBufferPool{}
	d := NewCommandDecoder(Config{
		BufPool: pool,
	})

	// Create APPEND with 2 valid records and 1 broken record (zero-length)
	reader := readerFrom("$6\r\nstream\r\n*0\r\n*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n$0\r\n\r\n")

	cmd, err := d.DecodeAppendCommand(reader)

	require.Error(err)
	require.Equal(Error{ErrCodeBadFormat, "bulk string length must be greater than 0, got 0"}.Error(), err.Error())
	require.Equal("", cmd.StreamName)
	require.Equal(0, len(cmd.Records))

	// Should have Put back: 1 for stream name buffer + 2 for the successfully read records + 1 for the broken record buffer
	require.Equal(4, pool.putCallsCount)

	// Confirm all buffers have been returned to the pool
	require.Equal(pool.putCallsCount, pool.getCallsCount)
}

func TestDecodeReadCommand(t *testing.T) {}

func TestDecodeTrimCommand(t *testing.T) {}

func TestDecodeDeleteCommand(t *testing.T) {}

func readerFrom(str string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(str))
}

type mockBufferPool struct {
	getCallsCount int
	putCallsCount int
}

func (m *mockBufferPool) Get() []byte {
	m.getCallsCount++
	return make([]byte, 1024)
}

func (m *mockBufferPool) Put(b []byte) {
	m.putCallsCount++
}
