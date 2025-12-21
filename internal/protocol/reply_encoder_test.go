package protocol

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeOK(t *testing.T) {
	require := require.New(t)
	encoder := NewReplyEncoder()

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	err := encoder.EncodeOK(writer)
	require.NoError(err)

	err = writer.Flush()
	require.NoError(err)

	require.Equal("+OK\r\n", buf.String())
}

func TestEncodeError(t *testing.T) {
	encoder := NewReplyEncoder()

	tests := []struct {
		name           string
		error          Error
		expectedOutput string
	}{
		{
			name:           "successfully encodes ERR_BAD_FORMAT error",
			error:          BadFormatErrorf("malformed command"),
			expectedOutput: "-ERR_BAD_FORMAT malformed command\r\n",
		},
		{
			name:           "successfully encodes ERR_LIMITS error",
			error:          LimitsErrorf("stream name too long"),
			expectedOutput: "-ERR_LIMITS stream name too long\r\n",
		},
		{
			name:           "successfully encodes ERR_STREAM_EXISTS error",
			error:          StreamExistsErrorf("stream already exists"),
			expectedOutput: "-ERR_STREAM_EXISTS stream already exists\r\n",
		},
		{
			name:           "successfully encodes ERR_UNKNOWN_STREAM error",
			error:          UnknownStreamErrorf("stream not found"),
			expectedOutput: "-ERR_UNKNOWN_STREAM stream not found\r\n",
		},
		{
			name:           "successfully encodes ERR_NON_MONOTONIC_ID error",
			error:          NonMonotonicIDErrorf("ID not monotonic"),
			expectedOutput: "-ERR_NON_MONOTONIC_ID ID not monotonic\r\n",
		},
		{
			name:           "successfully encodes error with special characters in message",
			error:          BadFormatErrorf("unexpected byte(\\n) while expecting byte(\\r)"),
			expectedOutput: "-ERR_BAD_FORMAT unexpected byte(\\n) while expecting byte(\\r)\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)

			err := encoder.EncodeError(writer, test.error)
			require.NoError(err)

			err = writer.Flush()
			require.NoError(err)

			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestEncodeBulkString(t *testing.T) {
	encoder := NewReplyEncoder()

	tests := []struct {
		name           string
		value          string
		expectedOutput string
	}{
		{
			name:           "successfully encodes simple bulk string",
			value:          "hello",
			expectedOutput: "$5\r\nhello\r\n",
		},
		{
			name:           "successfully encodes bulk string with spaces",
			value:          "hello world",
			expectedOutput: "$11\r\nhello world\r\n",
		},
		{
			name:           "successfully encodes bulk string with special characters",
			value:          "hello\nworld\ttab",
			expectedOutput: "$15\r\nhello\nworld\ttab\r\n",
		},
		{
			name:           "successfully encodes bulk string with CRLF",
			value:          "line1\r\nline2",
			expectedOutput: "$12\r\nline1\r\nline2\r\n",
		},
		{
			name:           "successfully encodes long bulk string",
			value:          "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.",
			expectedOutput: "$191\r\nLorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.\r\n",
		},
		{
			name:           "successfully encodes ID format string",
			value:          "1700000001234-0",
			expectedOutput: "$15\r\n1700000001234-0\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)

			err := encoder.EncodeBulkString(writer, test.value)
			require.NoError(err)

			err = writer.Flush()
			require.NoError(err)

			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestEncodeRecords(t *testing.T) {
	encoder := NewReplyEncoder()

	tests := []struct {
		name           string
		records        []Record
		expectedOutput string
	}{
		{
			name:           "successfully encodes empty records array",
			records:        []Record{},
			expectedOutput: "*0\r\n",
		},
		{
			name: "successfully encodes single record",
			records: []Record{
				{ID: "1700000001234-0", Value: []byte("hello")},
			},
			expectedOutput: "*2\r\n$15\r\n1700000001234-0\r\n$5\r\nhello\r\n",
		},
		{
			name: "successfully encodes multiple records",
			records: []Record{
				{ID: "1700000001234-0", Value: []byte("hello")},
				{ID: "1700000001235-0", Value: []byte("world")},
			},
			expectedOutput: "*4\r\n$15\r\n1700000001234-0\r\n$5\r\nhello\r\n$15\r\n1700000001235-0\r\n$5\r\nworld\r\n",
		},
		{
			name: "successfully encodes multiple records in order",
			records: []Record{
				{ID: "1700000001234-0", Value: []byte("first")},
				{ID: "1700000001235-0", Value: []byte("second")},
				{ID: "1700000001236-0", Value: []byte("third")},
			},
			expectedOutput: "*6\r\n$15\r\n1700000001234-0\r\n$5\r\nfirst\r\n$15\r\n1700000001235-0\r\n$6\r\nsecond\r\n$15\r\n1700000001236-0\r\n$5\r\nthird\r\n",
		},
		{
			name: "successfully encodes records with binary data",
			records: []Record{
				{ID: "1700000001234-0", Value: []byte{0x00, 0xFF, 0x01, 0xFE}},
				{ID: "1700000001235-0", Value: []byte{0xDE, 0xAD, 0xBE, 0xEF}},
			},
			expectedOutput: "*4\r\n$15\r\n1700000001234-0\r\n$4\r\n\x00\xFF\x01\xFE\r\n$15\r\n1700000001235-0\r\n$4\r\n\xDE\xAD\xBE\xEF\r\n",
		},
		{
			name: "successfully encodes records with varying lengths",
			records: []Record{
				{ID: "1-0", Value: []byte("a")},
				{ID: "12345678901234567890-12345678901234567890", Value: []byte("this is a much longer record value with more content")},
			},
			expectedOutput: "*4\r\n$3\r\n1-0\r\n$1\r\na\r\n$41\r\n12345678901234567890-12345678901234567890\r\n$52\r\nthis is a much longer record value with more content\r\n",
		},
		{
			name: "successfully encodes records with special characters in values",
			records: []Record{
				{ID: "1700000001234-0", Value: []byte("line1\r\nline2\ttab")},
				{ID: "1700000001235-0", Value: []byte("special: !@#$%^&*()")},
			},
			expectedOutput: "*4\r\n$15\r\n1700000001234-0\r\n$16\r\nline1\r\nline2\ttab\r\n$15\r\n1700000001235-0\r\n$19\r\nspecial: !@#$%^&*()\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)

			err := encoder.EncodeRecords(writer, test.records)
			require.NoError(err)

			err = writer.Flush()
			require.NoError(err)

			require.Equal(test.expectedOutput, buf.String())
		})
	}
}
