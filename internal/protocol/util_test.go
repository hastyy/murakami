package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUtil_ExpectNextByte(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name              string
		inputReader       *bufio.Reader
		inputExpectedByte byte
		OutputExpectedErr error
	}{
		{
			name:              "next byte is the expected one",
			inputReader:       reader("*3"),
			inputExpectedByte: '*',
			OutputExpectedErr: nil,
		},
		{
			name:              "next byte is not the expected one",
			inputReader:       reader("$3"),
			inputExpectedByte: '*',
			OutputExpectedErr: BadFormatErrorf("found unexpected byte(%c) in stream while expecting byte(%c)", '$', '*'),
		},
		{
			name:              "reader returns an error",
			inputReader:       reader(""),
			inputExpectedByte: '*',
			OutputExpectedErr: io.EOF,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := expectNextByte(test.inputReader, test.inputExpectedByte)
			require.Equal(test.OutputExpectedErr, err)
		})
	}
}

func TestUtil_ReadLine(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name               string
		inputReader        *bufio.Reader
		outputExpectedLine string
		outputExpectedErr  error
	}{
		{
			name:               "reads the remainder of the line of ASCII encoded string",
			inputReader:        reader("123\r\n"),
			outputExpectedLine: "123",
			outputExpectedErr:  nil,
		},
		{
			name:               "reads only up to the first CRLF",
			inputReader:        reader("123\r\n456\r\n"),
			outputExpectedLine: "123",
			outputExpectedErr:  nil,
		},
		{
			name:               "returns an error if we stop reading before the CRLF",
			inputReader:        reader("123"),
			outputExpectedLine: "",
			outputExpectedErr:  io.EOF,
		},
		{
			name:               "returns an error if the line is empty",
			inputReader:        reader("\r\n"),
			outputExpectedLine: "",
			outputExpectedErr:  BadFormatErrorf("unexpected empty line"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			line, err := readLine(test.inputReader)
			require.Equal(test.outputExpectedLine, line)
			require.Equal(test.outputExpectedErr, err)
		})
	}
}

func TestUtil_ReadLength(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name                 string
		inputReader          *bufio.Reader
		outputExpectedLength int
		outputExpectedErr    error
	}{
		{
			name:                 "reads a valid length",
			inputReader:          reader("123\r\n"),
			outputExpectedLength: 123,
			outputExpectedErr:    nil,
		},
		{
			name:                 "returns an error if the length is not a valid integer",
			inputReader:          reader("a123\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("invalid length: a123"),
		},
		{
			name:                 "returns an error if the length is negative",
			inputReader:          reader("-123\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("invalid length: -123"),
		},
		{
			name:                 "returns an error if the length is decimal",
			inputReader:          reader("123.5\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("invalid length: 123.5"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			length, err := readLength(test.inputReader)
			require.Equal(test.outputExpectedLength, length)
			require.Equal(test.outputExpectedErr, err)
		})
	}
}

func TestUtil_ReadArrayLength(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name                 string
		inputReader          *bufio.Reader
		outputExpectedLength int
		outputExpectedErr    error
	}{
		{
			name:                 "reads a valid array length",
			inputReader:          reader("*3\r\n"),
			outputExpectedLength: 3,
			outputExpectedErr:    nil,
		},
		{
			name:                 "returns an error if the array length is not a valid integer",
			inputReader:          reader("*3a\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("invalid length: 3a"),
		},
		{
			name:                 "returns an error if the array length is negative",
			inputReader:          reader("*-3\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("invalid length: -3"),
		},
		{
			name:                 "returns an error if the array length is decimal",
			inputReader:          reader("*3.5\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("invalid length: 3.5"),
		},
		{
			name:                 "returns an error if it does not find the array symbol at the beginning of the line",
			inputReader:          reader("3\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("found unexpected byte(%c) in stream while expecting byte(%c)", '3', '*'),
		},
		{
			name:                 "returns an error if the reader returns an error",
			inputReader:          reader(""),
			outputExpectedLength: 0,
			outputExpectedErr:    io.EOF,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			length, err := readArrayLength(test.inputReader)
			require.Equal(test.outputExpectedLength, length)
			require.Equal(test.outputExpectedErr, err)
		})
	}
}

func TestUtil_readBulkStringLength(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name                 string
		inputReader          *bufio.Reader
		outputExpectedLength int
		outputExpectedErr    error
	}{
		{
			name:                 "reads a valid bulk string length",
			inputReader:          reader("$5\r\n"),
			outputExpectedLength: 5,
			outputExpectedErr:    nil,
		},
		{
			name:                 "reads zero length",
			inputReader:          reader("$0\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    nil,
		},
		{
			name:                 "reads large length",
			inputReader:          reader("$1048576\r\n"),
			outputExpectedLength: 1048576,
			outputExpectedErr:    nil,
		},
		{
			name:                 "returns an error if the bulk string length is not a valid integer",
			inputReader:          reader("$5a\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("invalid length: 5a"),
		},
		{
			name:                 "returns an error if the bulk string length is negative",
			inputReader:          reader("$-5\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("invalid length: -5"),
		},
		{
			name:                 "returns an error if the bulk string length is decimal",
			inputReader:          reader("$5.5\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("invalid length: 5.5"),
		},
		{
			name:                 "returns an error if it does not find the bulk string symbol at the beginning",
			inputReader:          reader("5\r\n"),
			outputExpectedLength: 0,
			outputExpectedErr:    BadFormatErrorf("found unexpected byte(%c) in stream while expecting byte(%c)", '5', '$'),
		},
		{
			name:                 "returns an error if the reader returns an error",
			inputReader:          reader(""),
			outputExpectedLength: 0,
			outputExpectedErr:    io.EOF,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			length, err := readBulkStringLength(test.inputReader)
			require.Equal(test.outputExpectedLength, length)
			require.Equal(test.outputExpectedErr, err)
		})
	}
}

func TestUtil_readBytes(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name              string
		inputReader       *bufio.Reader
		inputN            int
		outputExpectedErr error
		outputExpected    []byte
	}{
		{
			name:              "reads exact number of bytes",
			inputReader:       reader("hello"),
			inputN:            5,
			outputExpectedErr: nil,
			outputExpected:    []byte("hello"),
		},
		{
			name:              "reads zero bytes",
			inputReader:       reader("hello"),
			inputN:            0,
			outputExpectedErr: nil,
			outputExpected:    []byte{},
		},
		{
			name:              "reads single byte",
			inputReader:       reader("x"),
			inputN:            1,
			outputExpectedErr: nil,
			outputExpected:    []byte("x"),
		},
		{
			name:              "reads binary data",
			inputReader:       reader("\x00\x01\x02\x03\x04"),
			inputN:            5,
			outputExpectedErr: nil,
			outputExpected:    []byte{0x00, 0x01, 0x02, 0x03, 0x04},
		},
		{
			name:              "returns error when not enough bytes available",
			inputReader:       reader("hi"),
			inputN:            5,
			outputExpectedErr: io.ErrUnexpectedEOF,
			outputExpected:    nil,
		},
		{
			name:              "returns error when reader is empty",
			inputReader:       reader(""),
			inputN:            1,
			outputExpectedErr: io.EOF,
			outputExpected:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := readBytes(test.inputReader, test.inputN)
			if test.outputExpectedErr != nil {
				require.Error(err)
				require.Equal(test.outputExpectedErr, err)
				require.Nil(result)
			} else {
				require.NoError(err)
				require.Equal(test.outputExpected, result)
			}
		})
	}
}

func TestUtil_ConsumeCRLF(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name              string
		inputReader       *bufio.Reader
		outputExpectedErr error
	}{
		{
			name:              "consumes the CRLF separator",
			inputReader:       reader("\r\n"),
			outputExpectedErr: nil,
		},
		{
			name:              "returns an error if the CRLF separator is not found",
			inputReader:       reader("abc"),
			outputExpectedErr: BadFormatErrorf("found unexpected byte(%c) in stream while expecting byte(%c)", 'a', '\r'),
		},
		{
			name:              "returns an error if the CRLF separator is incomplete",
			inputReader:       reader("\ra"),
			outputExpectedErr: BadFormatErrorf("found unexpected byte(%c) in stream while expecting byte(%c)", 'a', '\n'),
		},
		{
			name:              "returns an error if the reader returns an error",
			inputReader:       reader("\r"),
			outputExpectedErr: io.EOF,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := consumeCRLF(test.inputReader)
			require.Equal(test.outputExpectedErr, err)
		})
	}
}

func TestUtil_ReadBulkString(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name                 string
		inputReader          *bufio.Reader
		inputLimit           int
		outputExpectedString string
		outputExpectedErr    error
	}{
		{
			name:                 "reads a valid bulk string",
			inputReader:          reader("$3\r\nabc\r\n"),
			inputLimit:           3,
			outputExpectedString: "abc",
			outputExpectedErr:    nil,
		},
		{
			name:                 "returns an error if the bulk string is too long",
			inputReader:          reader("$3\r\nabc\r\n"),
			inputLimit:           2,
			outputExpectedString: "",
			outputExpectedErr:    LimitsErrorf("bulk string length must be less than or equal to 2, got 3"),
		},
		{
			name:                 "returns an error if the bulk string is empty",
			inputReader:          reader("$0\r\n\r\n"),
			inputLimit:           1,
			outputExpectedString: "",
			outputExpectedErr:    LimitsErrorf("bulk string length must be greater than 0, got 0"),
		},
		{
			name:                 "returns an error if the bulk string length is not a valid integer",
			inputReader:          reader("$a\r\nabc\r\n"),
			inputLimit:           3,
			outputExpectedString: "",
			outputExpectedErr:    BadFormatErrorf("invalid length: a"),
		},
		{
			name:                 "returns an error if the bulk string length is negative",
			inputReader:          reader("$-3\r\nabc\r\n"),
			inputLimit:           3,
			outputExpectedString: "",
			outputExpectedErr:    BadFormatErrorf("invalid length: -3"),
		},
		{
			name:                 "returns an error if the bulk string length is decimal",
			inputReader:          reader("$3.5\r\nabc\r\n"),
			inputLimit:           3,
			outputExpectedString: "",
			outputExpectedErr:    BadFormatErrorf("invalid length: 3.5"),
		},
		{
			name:                 "returns an error if it does not find the bulk string symbol at the beginning of the line",
			inputReader:          reader("3\r\nabc\r\n"),
			inputLimit:           3,
			outputExpectedString: "",
			outputExpectedErr:    BadFormatErrorf("found unexpected byte(%c) in stream while expecting byte(%c)", '3', '$'),
		},
		{
			name:                 "returns an error if the reader returns an error",
			inputReader:          reader("$3\r\n"),
			inputLimit:           3,
			outputExpectedString: "",
			outputExpectedErr:    io.EOF,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			string, err := readBulkString(test.inputReader, test.inputLimit)
			require.Equal(test.outputExpectedString, string)
			require.Equal(test.outputExpectedErr, err)
		})
	}
}

func TestUtil_ReadBulkBytes(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name                    string
		bytesToRead             []byte
		inputReader             *bufio.Reader
		inputBuffer             []byte
		inputLimit              int
		outputExpectedBytesRead int
		outputExpectedErr       error
	}{
		{
			name:                    "reads a valid bulk of bytes",
			bytesToRead:             []byte("abc"),
			inputReader:             reader("$3\r\nabc\r\n"),
			inputBuffer:             make([]byte, 3),
			inputLimit:              3,
			outputExpectedBytesRead: 3,
			outputExpectedErr:       nil,
		},
		{
			name:                    "returns an error if the bulk bytes is too long",
			bytesToRead:             []byte{},
			inputReader:             reader("$3\r\nabc\r\n"),
			inputBuffer:             make([]byte, 3),
			inputLimit:              2,
			outputExpectedBytesRead: 0,
			outputExpectedErr:       LimitsErrorf("bulk string length must be less than or equal to 2, got 3"),
		},
		{
			name:                    "returns an error if the bulk bytes is empty",
			bytesToRead:             []byte{},
			inputReader:             reader("$0\r\n\r\n"),
			inputBuffer:             make([]byte, 1),
			inputLimit:              1,
			outputExpectedBytesRead: 0,
			outputExpectedErr:       LimitsErrorf("bulk string length must be greater than 0, got 0"),
		},
		{
			name:                    "returns an error if the bulk bytes length is not a valid integer",
			bytesToRead:             []byte{},
			inputReader:             reader("$a\r\nabc\r\n"),
			inputBuffer:             make([]byte, 3),
			inputLimit:              3,
			outputExpectedBytesRead: 0,
			outputExpectedErr:       BadFormatErrorf("invalid length: a"),
		},
		{
			name:                    "returns an error if the bulk bytes length is negative",
			bytesToRead:             []byte{},
			inputReader:             reader("$-3\r\nabc\r\n"),
			inputBuffer:             make([]byte, 3),
			inputLimit:              3,
			outputExpectedBytesRead: 0,
			outputExpectedErr:       BadFormatErrorf("invalid length: -3"),
		},
		{
			name:                    "returns an error if the bulk bytes length is decimal",
			bytesToRead:             []byte{},
			inputReader:             reader("$3.5\r\nabc\r\n"),
			inputBuffer:             make([]byte, 3),
			inputLimit:              3,
			outputExpectedBytesRead: 0,
			outputExpectedErr:       BadFormatErrorf("invalid length: 3.5"),
		},
		{
			name:                    "returns an error if it does not find the bulk string symbol at the beginning of the line",
			bytesToRead:             []byte{},
			inputReader:             reader("3\r\nabc\r\n"),
			inputBuffer:             make([]byte, 3),
			inputLimit:              3,
			outputExpectedBytesRead: 0,
			outputExpectedErr:       BadFormatErrorf("found unexpected byte(%c) in stream while expecting byte(%c)", '3', '$'),
		},
		{
			name:                    "returns an error if the reader returns an error",
			bytesToRead:             []byte{},
			inputReader:             reader("$3\r\n"),
			inputBuffer:             make([]byte, 3),
			inputLimit:              3,
			outputExpectedBytesRead: 0,
			outputExpectedErr:       io.EOF,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bytesRead, err := readBulkBytes(test.inputReader, test.inputBuffer, test.inputLimit)
			require.Equal(test.outputExpectedBytesRead, bytesRead)
			require.Equal(test.outputExpectedErr, err)
			require.Equal(test.bytesToRead, test.inputBuffer[:bytesRead])
		})
	}
}

func TestUtil_IsValidID(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name           string
		inputID        string
		outputExpected bool
	}{
		{
			name:           "returns true if the ID is valid",
			inputID:        "123-456",
			outputExpected: true,
		},
		{
			name:           "returns false if the ID has more than one hyphen",
			inputID:        "123-456-789",
			outputExpected: false,
		},
		{
			name:           "returns false if the ID only contains milliseconds",
			inputID:        "123456789",
			outputExpected: false,
		},
		{
			name:           "returns false if the ID is empty",
			inputID:        "",
			outputExpected: false,
		},
		{
			name:           "returns false if the sequence number is not a valid integer",
			inputID:        "123-abc",
			outputExpected: false,
		},
		{
			name:           "returns false if the milliseconds are not a valid integer",
			inputID:        "abc-123",
			outputExpected: false,
		},
		{
			name:           "returns false if the milliseconds are negative",
			inputID:        "-123-456",
			outputExpected: false,
		},
		{
			name:           "returns false if the sequence number is negative",
			inputID:        "123--456",
			outputExpected: false,
		},
		{
			name:           "returns false if the milliseconds are decimal",
			inputID:        "123.456-456",
			outputExpected: false,
		},
		{
			name:           "returns false if the sequence number is decimal",
			inputID:        "123-456.789",
			outputExpected: false,
		},
		{
			name:           "returns false if the milliseconds are too large",
			inputID:        "112345678901234567890-456",
			outputExpected: false,
		},
		{
			name:           "returns false if the sequence number is too large",
			inputID:        "123-112345678901234567890",
			outputExpected: false,
		},
		{
			name:           "returns false if the milliseconds are empty",
			inputID:        "-456",
			outputExpected: false,
		},
		{
			name:           "returns false if the sequence number is empty",
			inputID:        "123-",
			outputExpected: false,
		},
		{
			name:           "returns false if both the milliseconds and sequence number are empty",
			inputID:        "-",
			outputExpected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isValid := IsValidID(test.inputID)
			require.Equal(test.outputExpected, isValid)
		})
	}
}

func TestUtil_IsValidIDMillis(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name           string
		inputMillis    string
		outputExpected bool
	}{
		{
			name:           "returns true if the milliseconds part is valid",
			inputMillis:    "1234567890",
			outputExpected: true,
		},
		{
			name:           "returns false if the milliseconds part is not a valid integer",
			inputMillis:    "abc",
			outputExpected: false,
		},
		{
			name:           "returns false if the milliseconds part is negative",
			inputMillis:    "-1234567890",
			outputExpected: false,
		},
		{
			name:           "returns false if the milliseconds part is decimal",
			inputMillis:    "1234567890.1234567890",
			outputExpected: false,
		},
		{
			name:           "returns false if the milliseconds part is too large",
			inputMillis:    "1112345678901234567890",
			outputExpected: false,
		},
		{
			name:           "returns false if the milliseconds part is empty",
			inputMillis:    "",
			outputExpected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isValid := IsValidIDMillis(test.inputMillis)
			require.Equal(test.outputExpected, isValid)
		})
	}
}

func TestUtil_ReadSimpleString(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name                 string
		inputReader          *bufio.Reader
		outputExpectedString string
		outputExpectedErr    error
	}{
		{
			name:                 "reads a valid simple string",
			inputReader:          reader("+OK\r\n"),
			outputExpectedString: "OK",
			outputExpectedErr:    nil,
		},
		{
			name:                 "reads a simple string with spaces",
			inputReader:          reader("+hello world\r\n"),
			outputExpectedString: "hello world",
			outputExpectedErr:    nil,
		},
		{
			name:                 "reads a simple string with special characters",
			inputReader:          reader("+test-123_abc\r\n"),
			outputExpectedString: "test-123_abc",
			outputExpectedErr:    nil,
		},
		{
			name:                 "reads an empty simple string",
			inputReader:          reader("+\r\n"),
			outputExpectedString: "",
			outputExpectedErr:    BadFormatErrorf("unexpected empty line"),
		},
		{
			name:                 "returns an error if it does not find the simple string symbol at the beginning",
			inputReader:          reader("OK\r\n"),
			outputExpectedString: "",
			outputExpectedErr:    BadFormatErrorf("found unexpected byte(%c) in stream while expecting byte(%c)", 'O', '+'),
		},
		{
			name:                 "returns an error if the reader returns an error before CRLF",
			inputReader:          reader("+OK"),
			outputExpectedString: "",
			outputExpectedErr:    io.EOF,
		},
		{
			name:                 "returns an error if missing the + symbol",
			inputReader:          reader(""),
			outputExpectedString: "",
			outputExpectedErr:    io.EOF,
		},
		{
			name:                 "reads single character",
			inputReader:          reader("+a\r\n"),
			outputExpectedString: "a",
			outputExpectedErr:    nil,
		},
		{
			name:                 "reads long string",
			inputReader:          reader("+this is a longer string with multiple words\r\n"),
			outputExpectedString: "this is a longer string with multiple words",
			outputExpectedErr:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			str, err := readSimpleString(test.inputReader)
			require.Equal(test.outputExpectedString, str)
			require.Equal(test.outputExpectedErr, err)
		})
	}
}

func TestUtil_ReadError(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name              string
		inputReader       *bufio.Reader
		outputExpectedErr Error
		outputDecodeErr   error
	}{
		{
			name:        "reads a valid error with code and message",
			inputReader: reader("-ERR_BAD_FORMAT invalid command\r\n"),
			outputExpectedErr: Error{
				Code:        ErrCodeBadFormat,
				Message:     "invalid command",
				recoverable: false,
			},
			outputDecodeErr: nil,
		},
		{
			name:        "reads a valid recoverable error",
			inputReader: reader("-ERR_STREAM_EXISTS stream already exists\r\n"),
			outputExpectedErr: Error{
				Code:        ErrCodeStreamExists,
				Message:     "stream already exists",
				recoverable: true,
			},
			outputDecodeErr: nil,
		},
		{
			name:              "returns an error if no space separates code and message",
			inputReader:       reader("-ERR_LIMITS\r\n"),
			outputExpectedErr: Error{},
			outputDecodeErr:   fmt.Errorf("bad server reply format: expected error code and message separated by space, got: ERR_LIMITS"),
		},
		{
			name:              "returns an error for unknown error code",
			inputReader:       reader("-ERR_UNKNOWN something went wrong\r\n"),
			outputExpectedErr: Error{},
			outputDecodeErr:   fmt.Errorf("unrecognized error code: ERR_UNKNOWN"),
		},
		{
			name:        "reads an error with multiple spaces in message",
			inputReader: reader("-ERR_BAD_FORMAT this is a longer message\r\n"),
			outputExpectedErr: Error{
				Code:        ErrCodeBadFormat,
				Message:     "this is a longer message",
				recoverable: false,
			},
			outputDecodeErr: nil,
		},
		{
			name:              "returns an error if it does not find the error symbol at the beginning",
			inputReader:       reader("ERR_BAD_FORMAT message\r\n"),
			outputExpectedErr: Error{},
			outputDecodeErr:   BadFormatErrorf("found unexpected byte(%c) in stream while expecting byte(%c)", 'E', '-'),
		},
		{
			name:              "returns an error if the reader returns an error before CRLF",
			inputReader:       reader("-ERR_BAD_FORMAT"),
			outputExpectedErr: Error{},
			outputDecodeErr:   io.EOF,
		},
		{
			name:              "returns an error if missing the - symbol",
			inputReader:       reader(""),
			outputExpectedErr: Error{},
			outputDecodeErr:   io.EOF,
		},
		{
			name:        "reads ERR_UNKNOWN_STREAM",
			inputReader: reader("-ERR_UNKNOWN_STREAM no such stream\r\n"),
			outputExpectedErr: Error{
				Code:        ErrCodeUnknownStream,
				Message:     "no such stream",
				recoverable: true,
			},
			outputDecodeErr: nil,
		},
		{
			name:        "reads ERR_NON_MONOTONIC_ID",
			inputReader: reader("-ERR_NON_MONOTONIC_ID id not monotonic\r\n"),
			outputExpectedErr: Error{
				Code:        ErrCodeNonMonotonicID,
				Message:     "id not monotonic",
				recoverable: true,
			},
			outputDecodeErr: nil,
		},
		{
			name:              "returns an error for empty error line",
			inputReader:       reader("-\r\n"),
			outputExpectedErr: Error{},
			outputDecodeErr:   BadFormatErrorf("unexpected empty line"),
		},
		{
			name:        "reads ERR_LIMITS",
			inputReader: reader("-ERR_LIMITS record size too large\r\n"),
			outputExpectedErr: Error{
				Code:        ErrCodeLimits,
				Message:     "record size too large",
				recoverable: false,
			},
			outputDecodeErr: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err, decodeErr := readError(test.inputReader)
			require.Equal(test.outputExpectedErr, err)
			require.Equal(test.outputDecodeErr, decodeErr)
		})
	}
}

func TestUtil_writeArrayHeader(t *testing.T) {
	require := require.New(t)

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	err := writeArrayHeader(writer, 1000)
	require.NoError(err)

	err = writer.Flush()
	require.NoError(err)

	require.Equal("*1000\r\n", buf.String())
}

func TestUtil_writeArrayHeader_WriterError(t *testing.T) {
	require := require.New(t)

	// Create a writer with a very small buffer to force immediate writes
	failingWriter := &failingWriter{failAfter: 0}
	writer := bufio.NewWriterSize(failingWriter, 1)

	err := writeArrayHeader(writer, 3)
	require.Error(err)
	require.Equal(io.ErrShortWrite, err)
}

func TestUtil_writeBulkString(t *testing.T) {
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

			err := writeBulkString(writer, test.value)
			require.NoError(err)

			err = writer.Flush()
			require.NoError(err)

			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestUtil_writeBulkString_WriterError(t *testing.T) {
	require := require.New(t)

	// Create a writer with a very small buffer to force immediate writes
	failingWriter := &failingWriter{failAfter: 0}
	writer := bufio.NewWriterSize(failingWriter, 1)

	err := writeBulkString(writer, "test")
	require.Error(err)
	require.Equal(io.ErrShortWrite, err)
}

func TestUtil_writeBulkBytes(t *testing.T) {
	tests := []struct {
		name           string
		value          []byte
		expectedOutput string
	}{
		{
			name:           "simple bytes",
			value:          []byte("hello"),
			expectedOutput: "$5\r\nhello\r\n",
		},
		{
			name:           "empty bytes",
			value:          []byte{},
			expectedOutput: "$0\r\n\r\n",
		},
		{
			name:           "bytes with CRLF",
			value:          []byte("line1\r\nline2"),
			expectedOutput: "$12\r\nline1\r\nline2\r\n",
		},
		{
			name:           "binary data",
			value:          []byte{0x00, 0x01, 0x02, 0xFF},
			expectedOutput: "$4\r\n\x00\x01\x02\xFF\r\n",
		},
		{
			name:           "unicode bytes",
			value:          []byte("hello世界"),
			expectedOutput: "$11\r\nhello世界\r\n",
		},
		{
			name:           "single byte",
			value:          []byte("a"),
			expectedOutput: "$1\r\na\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)

			err := writeBulkBytes(writer, test.value)
			require.NoError(err)

			err = writer.Flush()
			require.NoError(err)

			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestUtil_writeBulkBytes_WriterError(t *testing.T) {
	require := require.New(t)

	// Create a writer with a very small buffer to force immediate writes
	failingWriter := &failingWriter{failAfter: 0}
	writer := bufio.NewWriterSize(failingWriter, 1)

	err := writeBulkBytes(writer, []byte("test"))
	require.Error(err)
	require.Equal(io.ErrShortWrite, err)
}

// failingWriter is a mock writer that always fails
type failingWriter struct {
	writeCount int
	failAfter  int
}

func (fw *failingWriter) Write(p []byte) (n int, err error) {
	if fw.writeCount >= fw.failAfter {
		return 0, io.ErrShortWrite
	}
	fw.writeCount++
	return len(p), nil
}

// reader creates a buffered reader from a string.
func reader(str string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(str))
}
