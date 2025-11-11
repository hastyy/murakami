package protocol

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeOK(t *testing.T) {
	require := require.New(t)

	encoder := NewReplyEncoder()

	tests := []struct {
		name           string
		expectedOutput string
	}{
		{
			name:           "should encode OK reply correctly",
			expectedOutput: "+OK\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := encoder.EncodeOK(&buf)

			require.NoError(err)
			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestEncodeError(t *testing.T) {
	require := require.New(t)

	encoder := NewReplyEncoder()

	tests := []struct {
		name           string
		error          Error
		expectedOutput string
	}{
		{
			name: "should encode ERR_BAD_FORMAT correctly",
			error: Error{
				Code:    ErrCodeBadFormat,
				Message: "invalid command",
			},
			expectedOutput: "-ERR_BAD_FORMAT invalid command\r\n",
		},
		{
			name: "should encode ERR_STREAM_EXISTS correctly",
			error: Error{
				Code:    ErrCodeStreamExists,
				Message: "stream already exists",
			},
			expectedOutput: "-ERR_STREAM_EXISTS stream already exists\r\n",
		},
		{
			name: "should encode ERR_UNKNOWN_STREAM correctly",
			error: Error{
				Code:    ErrCodeUnknownStream,
				Message: "stream not found",
			},
			expectedOutput: "-ERR_UNKNOWN_STREAM stream not found\r\n",
		},
		{
			name: "should encode ERR_LIMITS correctly",
			error: Error{
				Code:    ErrCodeLimits,
				Message: "record too large",
			},
			expectedOutput: "-ERR_LIMITS record too large\r\n",
		},
		{
			name: "should encode error with long message",
			error: Error{
				Code:    ErrCodeBadFormat,
				Message: "this is a very long error message that contains multiple words and provides detailed information",
			},
			expectedOutput: "-ERR_BAD_FORMAT this is a very long error message that contains multiple words and provides detailed information\r\n",
		},
		{
			name: "should encode error with special characters in message",
			error: Error{
				Code:    ErrCodeBadFormat,
				Message: "found unexpected byte($) while expecting byte(*)",
			},
			expectedOutput: "-ERR_BAD_FORMAT found unexpected byte($) while expecting byte(*)\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := encoder.EncodeError(&buf, test.error)

			require.NoError(err)
			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestEncodeBulkString(t *testing.T) {
	require := require.New(t)

	encoder := NewReplyEncoder()

	tests := []struct {
		name           string
		value          string
		expectedOutput string
	}{
		{
			name:           "should encode simple bulk string",
			value:          "hello",
			expectedOutput: "$5\r\nhello\r\n",
		},
		{
			name:           "should encode bulk string with spaces",
			value:          "hello world",
			expectedOutput: "$11\r\nhello world\r\n",
		},
		{
			name:           "should encode single character bulk string",
			value:          "a",
			expectedOutput: "$1\r\na\r\n",
		},
		{
			name:           "should encode bulk string with special characters",
			value:          "hello\nworld\r\ntest",
			expectedOutput: "$17\r\nhello\nworld\r\ntest\r\n",
		},
		{
			name:           "should encode timestamp-like bulk string",
			value:          "1700000001234-0",
			expectedOutput: "$15\r\n1700000001234-0\r\n",
		},
		{
			name:           "should encode bulk string with numbers",
			value:          "12345",
			expectedOutput: "$5\r\n12345\r\n",
		},
		{
			name:           "should encode bulk string with mixed case",
			value:          "HeLLo WoRLd",
			expectedOutput: "$11\r\nHeLLo WoRLd\r\n",
		},
		{
			name:           "should encode bulk string with punctuation",
			value:          "Hello, World!",
			expectedOutput: "$13\r\nHello, World!\r\n",
		},
		{
			name:           "should encode large bulk string",
			value:          strings.Repeat("x", 1000),
			expectedOutput: "$1000\r\n" + strings.Repeat("x", 1000) + "\r\n",
		},
		{
			name:           "should encode bulk string with null bytes (binary safe)",
			value:          "hello\x00world",
			expectedOutput: "$11\r\nhello\x00world\r\n",
		},
		{
			name:           "should encode bulk string with control characters",
			value:          "test\x01\x02\x03data",
			expectedOutput: "$11\r\ntest\x01\x02\x03data\r\n",
		},
		{
			name:           "should encode bulk string with tab",
			value:          "hello\tworld",
			expectedOutput: "$11\r\nhello\tworld\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := encoder.EncodeBulkString(&buf, test.value)

			require.NoError(err)
			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestEncodeBulkBytesArray(t *testing.T) {
	require := require.New(t)

	encoder := NewReplyEncoder()

	tests := []struct {
		name           string
		values         [][]byte
		expectedOutput string
	}{
		{
			name:           "should encode empty array",
			values:         [][]byte{},
			expectedOutput: "*0\r\n",
		},
		{
			name:           "should encode array with one element",
			values:         [][]byte{[]byte("hello")},
			expectedOutput: "*1\r\n$5\r\nhello\r\n",
		},
		{
			name:           "should encode array with two elements",
			values:         [][]byte{[]byte("hello"), []byte("world")},
			expectedOutput: "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
		},
		{
			name:           "should encode array with multiple elements",
			values:         [][]byte{[]byte("one"), []byte("two"), []byte("three")},
			expectedOutput: "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n",
		},
		{
			name: "should encode READ reply with timestamp-record pairs",
			values: [][]byte{
				[]byte("1700000001234-0"),
				[]byte("hello"),
				[]byte("1700000001235-0"),
				[]byte("world"),
			},
			expectedOutput: "*4\r\n$15\r\n1700000001234-0\r\n$5\r\nhello\r\n$15\r\n1700000001235-0\r\n$5\r\nworld\r\n",
		},
		{
			name:           "should encode array with elements of different sizes",
			values:         [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")},
			expectedOutput: "*3\r\n$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n",
		},
		{
			name:           "should encode array with binary data (null bytes)",
			values:         [][]byte{[]byte("hello\x00world"), []byte("test\x00data")},
			expectedOutput: "*2\r\n$11\r\nhello\x00world\r\n$9\r\ntest\x00data\r\n",
		},
		{
			name:           "should encode array with control characters",
			values:         [][]byte{[]byte("test\x01\x02"), []byte("data\x03\x04")},
			expectedOutput: "*2\r\n$6\r\ntest\x01\x02\r\n$6\r\ndata\x03\x04\r\n",
		},
		{
			name:           "should encode array with newlines and carriage returns",
			values:         [][]byte{[]byte("line1\nline2"), []byte("line3\r\nline4")},
			expectedOutput: "*2\r\n$11\r\nline1\nline2\r\n$12\r\nline3\r\nline4\r\n",
		},
		{
			name:           "should encode array with large elements",
			values:         [][]byte{[]byte(strings.Repeat("x", 100)), []byte(strings.Repeat("y", 200))},
			expectedOutput: "*2\r\n$100\r\n" + strings.Repeat("x", 100) + "\r\n$200\r\n" + strings.Repeat("y", 200) + "\r\n",
		},
		{
			name: "should encode array with many elements",
			values: [][]byte{
				[]byte("1"), []byte("2"), []byte("3"), []byte("4"), []byte("5"),
				[]byte("6"), []byte("7"), []byte("8"), []byte("9"), []byte("10"),
			},
			expectedOutput: "*10\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n$1\r\n6\r\n$1\r\n7\r\n$1\r\n8\r\n$1\r\n9\r\n$2\r\n10\r\n",
		},
		{
			name:           "should encode array with UTF-8 characters",
			values:         [][]byte{[]byte("hello"), []byte("世界"), []byte("🌍")},
			expectedOutput: "*3\r\n$5\r\nhello\r\n$6\r\n世界\r\n$4\r\n🌍\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := encoder.EncodeBulkBytesArray(&buf, test.values)

			require.NoError(err)
			require.Equal(test.expectedOutput, buf.String())
		})
	}
}
