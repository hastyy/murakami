package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDecodeNextCommand(t *testing.T) {
	d := NewCommandDecoder(&mockBufferPool{})

	tests := []struct {
		name            string
		reader          *bufio.Reader
		expectedSpec    CommandSpec
		expectedErrCode ErrorCode
	}{
		{
			name:   "successfully decode CREATE command spec",
			reader: reader("*3\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{
				Name:       "CREATE",
				ArgsLength: 2,
			},
		},
		{
			name:   "successfully decode APPEND command spec",
			reader: reader("*4\r\n$6\r\nAPPEND\r\n$6\r\nstream\r\n*0\r\n$6\r\nrecord\r\n"),
			expectedSpec: CommandSpec{
				Name:       "APPEND",
				ArgsLength: 3,
			},
		},
		{
			name:   "successfully decode READ command spec",
			reader: reader("*3\r\n$4\r\nREAD\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{
				Name:       "READ",
				ArgsLength: 2,
			},
		},
		{
			name:   "successfully decode TRIM command spec",
			reader: reader("*3\r\n$4\r\nTRIM\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{
				Name:       "TRIM",
				ArgsLength: 2,
			},
		},
		{
			name:   "successfully decode DELETE command spec",
			reader: reader("*3\r\n$6\r\nDELETE\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{
				Name:       "DELETE",
				ArgsLength: 2,
			},
		},
		{
			name:   "successfully decode unknown command spec",
			reader: reader("*3\r\n$7\r\nUNKNOWN\r\n$6\r\nstream\r\n*0\r\n"),
			expectedSpec: CommandSpec{
				Name:       "UNKNOWN",
				ArgsLength: 2,
			},
		},
		{
			name:            "top-level array with less than 3 elements should return an error",
			reader:          reader("*2\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n"),
			expectedSpec:    CommandSpec{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "command name length exceeds the maximum length",
			reader:          reader(fmt.Sprintf("*3\r\n$257\r\n%s\r\n$6\r\nstream\r\n*0\r\n", strings.Repeat("a", 257))),
			expectedSpec:    CommandSpec{},
			expectedErrCode: ErrCodeLimits,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			var errCode ErrorCode
			spec, err := d.DecodeNextCommand(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					t.Fatalf("expected protocol error, got %v", err)
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			// should be "" and 0 respectively if there was an error
			require.Equal(test.expectedSpec.Name, spec.Name)
			require.Equal(test.expectedSpec.ArgsLength, spec.ArgsLength)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		reader := reader("*3\r\n$6\r\nCREATE\r")
		_, err := d.DecodeNextCommand(reader)

		require.Error(err)
		require.Equal(io.EOF, err)
	})
}

func TestDecodeCreateCommand(t *testing.T) {
	d := NewCommandDecoder(&mockBufferPool{})

	tests := []struct {
		name            string
		reader          *bufio.Reader
		expectedCommand CreateCommand
		expectedErrCode ErrorCode
	}{
		{
			name:   "successfully decode CREATE command",
			reader: reader("$6\r\nstream\r\n*0\r\n"),
			expectedCommand: CreateCommand{
				StreamName: "stream",
			},
		},
		{
			name:            "stream name length exceeds the maximum length",
			reader:          reader(fmt.Sprintf("$257\r\n%s\r\n*0\r\n", strings.Repeat("a", 257))),
			expectedCommand: CreateCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "options array with odd number of elements",
			reader:          reader("$6\r\nstream\r\n*1\r\n$7\r\nUNKNOWN\r\n"),
			expectedCommand: CreateCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "options array with unknown option",
			reader:          reader("$6\r\nstream\r\n*2\r\n$7\r\nUNKNOWN\r\n$7\r\nUNKNOWN\r\n"),
			expectedCommand: CreateCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "stream name length 0",
			reader:          reader("$0\r\n*0\r\n"),
			expectedCommand: CreateCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "option key length exceeds the maximum length",
			reader:          reader(fmt.Sprintf("$6\r\nstream\r\n*2\r\n$257\r\n%s\r\n$7\r\nUNKNOWN\r\n", strings.Repeat("a", 257))),
			expectedCommand: CreateCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "option value length exceeds the maximum length",
			reader:          reader(fmt.Sprintf("$6\r\nstream\r\n*2\r\n$7\r\nUNKNOWN\r\n$257\r\n%s\r\n", strings.Repeat("a", 257))),
			expectedCommand: CreateCommand{},
			expectedErrCode: ErrCodeLimits,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			var errCode ErrorCode
			cmd, err := d.DecodeCreateCommand(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					t.Fatalf("expected protocol error, got %v", err)
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			// should be "" if there was an error
			require.Equal(test.expectedCommand.StreamName, cmd.StreamName)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		reader := reader("$6\r\nstream\r\n*0\r")
		_, err := d.DecodeCreateCommand(reader)

		require.Error(err)
		require.Equal(io.EOF, err)
	})
}

func TestDecodeAppendCommand(t *testing.T) {
	tests := []struct {
		name                         string
		reader                       *bufio.Reader
		expectedCommand              AppendCommand
		expectedErrCode              ErrorCode
		expectedBufPoolGetCallsCount int
		expectedBufPoolPutCallsCount int
	}{
		{
			name:   "successfully decode APPEND with no options and 1 record",
			reader: reader("$6\r\nstream\r\n*0\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCommand: AppendCommand{
				StreamName: "stream",
				Options:    AppendCommandOptions{},
				Records:    [][]byte{[]byte("hello")},
			},
			expectedBufPoolGetCallsCount: 1,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:   "successfully decode APPEND with id option and multiple records",
			reader: reader("$6\r\nstream\r\n*2\r\n$2\r\nID\r\n$13\r\n1700000001234\r\n*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n$3\r\nfoo\r\n"),
			expectedCommand: AppendCommand{
				StreamName: "stream",
				Options: AppendCommandOptions{
					MillisID: "1700000001234",
				},
				Records: [][]byte{[]byte("hello"), []byte("world"), []byte("foo")},
			},
			expectedBufPoolGetCallsCount: 3,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:   "should work correctly with mixed case ID option key",
			reader: reader("$6\r\nstream\r\n*2\r\n$2\r\nId\r\n$13\r\n1700000001234\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCommand: AppendCommand{
				StreamName: "stream",
				Options: AppendCommandOptions{
					MillisID: "1700000001234",
				},
				Records: [][]byte{[]byte("hello")},
			},
			expectedBufPoolGetCallsCount: 1,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:   "should use last option value when same option key appears multiple times",
			reader: reader("$6\r\nstream\r\n*4\r\n$2\r\nID\r\n$13\r\n1700000001234\r\n$2\r\nID\r\n$13\r\n1700000005678\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCommand: AppendCommand{
				StreamName: "stream",
				Options: AppendCommandOptions{
					MillisID: "1700000005678",
				},
				Records: [][]byte{[]byte("hello")},
			},
			expectedBufPoolGetCallsCount: 1,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:   "should work well with a large ID (within limits)",
			reader: reader("$6\r\nstream\r\n*2\r\n$2\r\nID\r\n$20\r\n12345678901234567890\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCommand: AppendCommand{
				StreamName: "stream",
				Options: AppendCommandOptions{
					MillisID: "12345678901234567890",
				},
				Records: [][]byte{[]byte("hello")},
			},
			expectedBufPoolGetCallsCount: 1,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:                         "should fail when stream name is empty",
			reader:                       reader("$0\r\n\r\n*0\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCommand:              AppendCommand{},
			expectedErrCode:              ErrCodeLimits,
			expectedBufPoolGetCallsCount: 0,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:                         "should fail when specified stream name is larger than limit",
			reader:                       reader(fmt.Sprintf("$257\r\n%s\r\n*0\r\n*1\r\n$5\r\nhello\r\n", strings.Repeat("a", 257))),
			expectedCommand:              AppendCommand{},
			expectedErrCode:              ErrCodeLimits,
			expectedBufPoolGetCallsCount: 0,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:                         "should fail when a record goes over the configured max size",
			reader:                       reader(fmt.Sprintf("$6\r\nstream\r\n*0\r\n*1\r\n$1048577\r\n%s\r\n", strings.Repeat("x", 1048577))),
			expectedCommand:              AppendCommand{},
			expectedErrCode:              ErrCodeLimits,
			expectedBufPoolGetCallsCount: 0, // Length check fails before buffer allocation
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:                         "should fail when appending with a batch of records over the configured limit",
			reader:                       reader(fmt.Sprintf("$6\r\nstream\r\n*0\r\n*1001\r\n%s", strings.Repeat("$1\r\na\r\n", 1001))),
			expectedCommand:              AppendCommand{},
			expectedErrCode:              ErrCodeLimits,
			expectedBufPoolGetCallsCount: 0,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:                         "should fail when total bytes in records exceeds the configured aggregate limit",
			reader:                       reader(fmt.Sprintf("$6\r\nstream\r\n*0\r\n*2\r\n$524288\r\n%s\r\n$524289\r\n%s\r\n", strings.Repeat("a", 524288), strings.Repeat("b", 524289))),
			expectedCommand:              AppendCommand{},
			expectedErrCode:              ErrCodeLimits,
			expectedBufPoolGetCallsCount: 1, // First record succeeds, second fails at length check
			expectedBufPoolPutCallsCount: 1,
		},
		{
			name:                         "should fail when there are no records",
			reader:                       reader("$6\r\nstream\r\n*0\r\n*0\r\n"),
			expectedCommand:              AppendCommand{},
			expectedErrCode:              ErrCodeLimits,
			expectedBufPoolGetCallsCount: 0,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:                         "should fail when options contains an unknown key",
			reader:                       reader("$6\r\nstream\r\n*2\r\n$7\r\nUNKNOWN\r\n$5\r\nvalue\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCommand:              AppendCommand{},
			expectedErrCode:              ErrCodeBadFormat,
			expectedBufPoolGetCallsCount: 0,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:                         "should fail with odd number of elements in options array",
			reader:                       reader("$6\r\nstream\r\n*3\r\n$2\r\nID\r\n$13\r\n1700000001234\r\n$7\r\ninvalid\r\n*1\r\n$5\r\nhello\r\n"),
			expectedCommand:              AppendCommand{},
			expectedErrCode:              ErrCodeBadFormat,
			expectedBufPoolGetCallsCount: 0,
			expectedBufPoolPutCallsCount: 0,
		},
		{
			name:                         "should fail with a zero length record",
			reader:                       reader("$6\r\nstream\r\n*0\r\n*2\r\n$5\r\nhello\r\n$0\r\n\r\n"),
			expectedCommand:              AppendCommand{},
			expectedErrCode:              ErrCodeLimits,
			expectedBufPoolGetCallsCount: 1, // First record succeeds, second fails at length check (zero length)
			expectedBufPoolPutCallsCount: 1,
		},
		{
			name:                         "should fail if one of the records is not a bulk string",
			reader:                       reader("$6\r\nstream\r\n*0\r\n*2\r\n$5\r\nhello\r\n+world\r\n"),
			expectedCommand:              AppendCommand{},
			expectedErrCode:              ErrCodeBadFormat,
			expectedBufPoolGetCallsCount: 1, // First record succeeds, second fails at bulk string symbol check
			expectedBufPoolPutCallsCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			bufPool := &mockBufferPool{}
			d := NewCommandDecoder(bufPool)

			var errCode ErrorCode
			cmd, err := d.DecodeAppendCommand(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					t.Fatalf("expected protocol error, got %v", err)
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			// should all be zero if there was an error
			require.Equal(test.expectedCommand.StreamName, cmd.StreamName)
			require.Equal(test.expectedCommand.Options.MillisID, cmd.Options.MillisID)
			require.Equal(test.expectedCommand.Records, cmd.Records)

			require.Equal(test.expectedBufPoolGetCallsCount, bufPool.getCallsCount)
			require.Equal(test.expectedBufPoolPutCallsCount, bufPool.putCallsCount)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		bufPool := &mockBufferPool{}
		d := NewCommandDecoder(bufPool)
		reader := reader("$6\r\nstream\r\n*0\r\n*1\r\n$5\r\nhello\r")
		_, err := d.DecodeAppendCommand(reader)

		require.Error(err)
		require.Equal(io.EOF, err)

		require.Equal(bufPool.getCallsCount, bufPool.putCallsCount)
	})
}

func TestDecodeReadCommand(t *testing.T) {
	d := NewCommandDecoder(&mockBufferPool{})

	tests := []struct {
		name            string
		reader          *bufio.Reader
		expectedCommand ReadCommand
		expectedErrCode ErrorCode
	}{
		{
			name:   "successfully decode READ command with default options",
			reader: reader("$6\r\nstream\r\n*0\r\n"),
			expectedCommand: ReadCommand{
				StreamName: "stream",
				Options: ReadCommandOptions{
					Count: DEFAULT_MAX_READ_COUNT,
					Block: 0,
					MinID: "0-0",
				},
			},
		},
		{
			name:   "successfully decode READ command with COUNT option",
			reader: reader("$6\r\nstream\r\n*2\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n"),
			expectedCommand: ReadCommand{
				StreamName: "stream",
				Options: ReadCommandOptions{
					Count: 10,
					Block: 0,
					MinID: "0-0",
				},
			},
		},
		{
			name:   "successfully decode READ command with BLOCK option",
			reader: reader("$6\r\nstream\r\n*2\r\n$5\r\nBLOCK\r\n$4\r\n5000\r\n"),
			expectedCommand: ReadCommand{
				StreamName: "stream",
				Options: ReadCommandOptions{
					Count: DEFAULT_MAX_READ_COUNT,
					Block: 5000 * time.Millisecond,
					MinID: "0-0",
				},
			},
		},
		{
			name:   "successfully decode READ command with MIN_ID option",
			reader: reader("$6\r\nstream\r\n*2\r\n$6\r\nMIN_ID\r\n$15\r\n1700000001234-0\r\n"),
			expectedCommand: ReadCommand{
				StreamName: "stream",
				Options: ReadCommandOptions{
					Count: DEFAULT_MAX_READ_COUNT,
					Block: 0,
					MinID: "1700000001234-0",
				},
			},
		},
		{
			name:   "successfully decode READ command with all options",
			reader: reader("$6\r\nstream\r\n*6\r\n$5\r\nCOUNT\r\n$2\r\n50\r\n$5\r\nBLOCK\r\n$4\r\n1000\r\n$6\r\nMIN_ID\r\n$5\r\n10-20\r\n"),
			expectedCommand: ReadCommand{
				StreamName: "stream",
				Options: ReadCommandOptions{
					Count: 50,
					Block: 1000 * time.Millisecond,
					MinID: "10-20",
				},
			},
		},
		{
			name:   "should work correctly with mixed case option keys",
			reader: reader("$6\r\nstream\r\n*2\r\n$5\r\nCoUnT\r\n$2\r\n25\r\n"),
			expectedCommand: ReadCommand{
				StreamName: "stream",
				Options: ReadCommandOptions{
					Count: 25,
					Block: 0,
					MinID: "0-0",
				},
			},
		},
		{
			name:            "should fail when reading stream with empty stream name",
			reader:          reader("$0\r\n\r\n*0\r\n"),
			expectedCommand: ReadCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "should fail when reading stream with stream name length greater than configured max length",
			reader:          reader(fmt.Sprintf("$257\r\n%s\r\n*0\r\n", strings.Repeat("a", 257))),
			expectedCommand: ReadCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "should fail with odd number of elements in options array",
			reader:          reader("$6\r\nstream\r\n*3\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n$7\r\ninvalid\r\n"),
			expectedCommand: ReadCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "should fail when options contains an unknown key",
			reader:          reader("$6\r\nstream\r\n*2\r\n$7\r\nUNKNOWN\r\n$5\r\nvalue\r\n"),
			expectedCommand: ReadCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "should fail with invalid COUNT value (non-numeric)",
			reader:          reader("$6\r\nstream\r\n*2\r\n$5\r\nCOUNT\r\n$3\r\nabc\r\n"),
			expectedCommand: ReadCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "should fail with invalid COUNT value (zero)",
			reader:          reader("$6\r\nstream\r\n*2\r\n$5\r\nCOUNT\r\n$1\r\n0\r\n"),
			expectedCommand: ReadCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "should fail with invalid COUNT value (negative)",
			reader:          reader("$6\r\nstream\r\n*2\r\n$5\r\nCOUNT\r\n$2\r\n-5\r\n"),
			expectedCommand: ReadCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "should fail with invalid BLOCK value (non-numeric)",
			reader:          reader("$6\r\nstream\r\n*2\r\n$5\r\nBLOCK\r\n$3\r\nxyz\r\n"),
			expectedCommand: ReadCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "should fail with invalid BLOCK value (negative)",
			reader:          reader("$6\r\nstream\r\n*2\r\n$5\r\nBLOCK\r\n$3\r\n-10\r\n"),
			expectedCommand: ReadCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "should fail with invalid MIN_ID (missing dash)",
			reader:          reader("$6\r\nstream\r\n*2\r\n$6\r\nMIN_ID\r\n$13\r\n1700000001234\r\n"),
			expectedCommand: ReadCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:   "should use last value when same option key appears multiple times",
			reader: reader("$6\r\nstream\r\n*4\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n$5\r\nCOUNT\r\n$2\r\n20\r\n"),
			expectedCommand: ReadCommand{
				StreamName: "stream",
				Options: ReadCommandOptions{
					Count: 20,
					Block: 0,
					MinID: "0-0",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			var errCode ErrorCode
			cmd, err := d.DecodeReadCommand(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					t.Fatalf("expected protocol error, got %v", err)
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			// should all be zero if there was an error
			require.Equal(test.expectedCommand.StreamName, cmd.StreamName)
			require.Equal(test.expectedCommand.Options.Count, cmd.Options.Count)
			require.Equal(test.expectedCommand.Options.Block, cmd.Options.Block)
			require.Equal(test.expectedCommand.Options.MinID, cmd.Options.MinID)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		reader := reader("$6\r\nstream\r\n*0\r")
		_, err := d.DecodeReadCommand(reader)

		require.Error(err)
		require.Equal(io.EOF, err)
	})
}

func TestDecodeTrimCommand(t *testing.T) {
	d := NewCommandDecoder(&mockBufferPool{})

	tests := []struct {
		name            string
		reader          *bufio.Reader
		expectedCommand TrimCommand
		expectedErrCode ErrorCode
	}{
		{
			name:   "successfully decode TRIM command with MIN_ID option",
			reader: reader("$6\r\nstream\r\n*2\r\n$6\r\nMIN_ID\r\n$15\r\n1700000001234-0\r\n"),
			expectedCommand: TrimCommand{
				StreamName: "stream",
				Options: TrimCommandOptions{
					MinID: "1700000001234-0",
				},
			},
		},
		{
			name:   "should work correctly with mixed case option key",
			reader: reader("$6\r\nstream\r\n*2\r\n$6\r\nMiN_Id\r\n$15\r\n1700000005678-1\r\n"),
			expectedCommand: TrimCommand{
				StreamName: "stream",
				Options: TrimCommandOptions{
					MinID: "1700000005678-1",
				},
			},
		},
		{
			name:   "should work well with a large ID (within limits)",
			reader: reader("$6\r\nstream\r\n*2\r\n$6\r\nMIN_ID\r\n$41\r\n12345678901234567890-12345678901234567890\r\n"),
			expectedCommand: TrimCommand{
				StreamName: "stream",
				Options: TrimCommandOptions{
					MinID: "12345678901234567890-12345678901234567890",
				},
			},
		},
		{
			name:            "should fail when MIN_ID option is missing",
			reader:          reader("$6\r\nstream\r\n*0\r\n"),
			expectedCommand: TrimCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "should fail when stream name is empty",
			reader:          reader("$0\r\n\r\n*2\r\n$6\r\nMIN_ID\r\n$3\r\n0-0\r\n"),
			expectedCommand: TrimCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "should fail when stream name is greater than limit",
			reader:          reader(fmt.Sprintf("$257\r\n%s\r\n*2\r\n$6\r\nMIN_ID\r\n$3\r\n0-0\r\n", strings.Repeat("a", 257))),
			expectedCommand: TrimCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "should fail with odd number of elements in options array",
			reader:          reader("$6\r\nstream\r\n*3\r\n$6\r\nMIN_ID\r\n$15\r\n1700000001234-0\r\n$7\r\ninvalid\r\n"),
			expectedCommand: TrimCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "should fail when options contains an unknown key",
			reader:          reader("$6\r\nstream\r\n*2\r\n$7\r\nUNKNOWN\r\n$5\r\nvalue\r\n"),
			expectedCommand: TrimCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "should fail with invalid MIN_ID format (missing dash)",
			reader:          reader("$6\r\nstream\r\n*2\r\n$6\r\nMIN_ID\r\n$13\r\n1700000001234\r\n"),
			expectedCommand: TrimCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "should fail with invalid MIN_ID format (missing number after dash)",
			reader:          reader("$6\r\nstream\r\n*2\r\n$6\r\nMIN_ID\r\n$14\r\n1700000001234-\r\n"),
			expectedCommand: TrimCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "should fail with invalid MIN_ID format (contains letters)",
			reader:          reader("$6\r\nstream\r\n*2\r\n$6\r\nMIN_ID\r\n$15\r\n1700000abc234-0\r\n"),
			expectedCommand: TrimCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:   "should use last value when MIN_ID option appears multiple times",
			reader: reader("$6\r\nstream\r\n*4\r\n$6\r\nMIN_ID\r\n$15\r\n1700000001234-0\r\n$6\r\nMIN_ID\r\n$15\r\n1700000005678-1\r\n"),
			expectedCommand: TrimCommand{
				StreamName: "stream",
				Options: TrimCommandOptions{
					MinID: "1700000005678-1",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			var errCode ErrorCode
			cmd, err := d.DecodeTrimCommand(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					t.Fatalf("expected protocol error, got %v", err)
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			// should all be zero if there was an error
			require.Equal(test.expectedCommand.StreamName, cmd.StreamName)
			require.Equal(test.expectedCommand.Options.MinID, cmd.Options.MinID)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		reader := reader("$6\r\nstream\r\n*2\r")
		_, err := d.DecodeTrimCommand(reader)

		require.Error(err)
		require.Equal(io.EOF, err)
	})
}

func TestDecodeDeleteCommand(t *testing.T) {
	d := NewCommandDecoder(&mockBufferPool{})

	tests := []struct {
		name            string
		reader          *bufio.Reader
		expectedCommand DeleteCommand
		expectedErrCode ErrorCode
	}{
		{
			name:   "successfully decode DELETE command",
			reader: reader("$6\r\nstream\r\n*0\r\n"),
			expectedCommand: DeleteCommand{
				StreamName: "stream",
			},
		},
		{
			name:            "stream name length 0",
			reader:          reader("$0\r\n\r\n*0\r\n"),
			expectedCommand: DeleteCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "stream name length exceeds the maximum length",
			reader:          reader(fmt.Sprintf("$257\r\n%s\r\n*0\r\n", strings.Repeat("a", 257))),
			expectedCommand: DeleteCommand{},
			expectedErrCode: ErrCodeLimits,
		},
		{
			name:            "options array with odd number of elements",
			reader:          reader("$6\r\nstream\r\n*3\r\n$3\r\nKEY\r\n$5\r\nvalue\r\n$7\r\ninvalid\r\n"),
			expectedCommand: DeleteCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
		{
			name:            "options array with unknown option",
			reader:          reader("$6\r\nstream\r\n*2\r\n$7\r\nUNKNOWN\r\n$5\r\nvalue\r\n"),
			expectedCommand: DeleteCommand{},
			expectedErrCode: ErrCodeBadFormat,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			var errCode ErrorCode
			cmd, err := d.DecodeDeleteCommand(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					t.Fatalf("expected protocol error, got %v", err)
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			// should be "" if there was an error
			require.Equal(test.expectedCommand.StreamName, cmd.StreamName)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		reader := reader("$6\r\nstream\r\n*0\r")
		_, err := d.DecodeDeleteCommand(reader)

		require.Error(err)
		require.Equal(io.EOF, err)
	})
}

type mockBufferPool struct {
	getCallsCount int
	putCallsCount int
}

func (m *mockBufferPool) Get(bufferSize int) []byte {
	m.getCallsCount++
	// Always return a 2MiB buffer to accommodate large test records
	return make([]byte, 2*1024*1024)
}

func (m *mockBufferPool) Put(b []byte) {
	m.putCallsCount++
}
