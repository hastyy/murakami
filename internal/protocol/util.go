package protocol

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
)

const (
	// Known symbols
	symbolArray        = '*'
	symbolBulkString   = '$'
	symbolSimpleString = '+'
	symbolError        = '-'

	// CRLF
	separatorCRLF = "\r\n"
)

var timestampRegex = regexp.MustCompile(`^\d+-\d+$`)

// readArrayLength expects the next byte from the reader to be '*'.
// It then attempts to read a length and returns an error if the length is not a valid integer >= 0.
//
// readArrayLength returns the length of the array.
// It expects the next byte from the reader to be '*', meaning it should be invoked when the next expected type in the reader byte stream is an array.
// It then attempts to read a valid length (integer >= 0). If not valid, it returns an error.
func readArrayLength(reader *bufio.Reader) (int, error) {
	if err := expectNextByte(reader, symbolArray); err != nil {
		return 0, err
	}

	length, err := readLength(reader)
	if err != nil {
		return 0, err
	}

	return length, nil
}

// expectNextByte consumes the next byte from the reader with an expectation of which byte it should be.
// Returns an error if the byte is not the expected one.
func expectNextByte(reader *bufio.Reader, expected byte) error {
	actual, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if actual != expected {
		return Error{ErrCodeBadFormat, fmt.Sprintf("found unexpected byte(%c) while expecting byte(%c)", actual, expected)}
	}
	return nil
}

// readLength expects to read a full line of digits from the reader.
// Returns an error if the line is not a valid integer >= 0.
func readLength(reader *bufio.Reader) (int, error) {
	line, err := readLine(reader)
	if err != nil {
		return 0, err
	}
	length, err := strconv.ParseInt(line, 10, 64)
	if err != nil || length < 0 {
		return 0, Error{ErrCodeBadFormat, fmt.Sprintf("invalid length: %s", line)}
	}
	return int(length), nil
}

// readLine reads the remainder of the line content from the reader.
// This is only used for lines we expect to only contain ASCII encoded strings.
func readLine(reader *bufio.Reader) (string, error) {
	// Reads until the end of the line (should end with CRLF)
	// bufio.Reader.ReadSlice() does not allocate any new memory but instead returns a slice from the internal buffer.
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return "", err
	}

	// Line should not be empty
	if len(line)-len(separatorCRLF) < 1 {
		return "", Error{ErrCodeBadFormat, "unexpected empty line"}
	}

	// Return line content without CRLF
	return string(line[:len(line)-len(separatorCRLF)]), nil
}

// readBulkString reads a bulk string from the reader into the provided buffer.
func readBulkString(reader *bufio.Reader, buf []byte) (string, error) {
	bulkBytes, err := readBulkBytes(reader, buf)
	if err != nil {
		return "", err
	}
	return string(bulkBytes), nil
}

// readBulkBytes reads a bulk string's bytes from the reader into the provided buffer.
func readBulkBytes(reader *bufio.Reader, buf []byte) ([]byte, error) {
	return readBulkBytesWithLimit(reader, buf, len(buf))
}

// readBulkBytesWithLimit is readBulkBytes but with a limit on the length of the bulk string.
func readBulkBytesWithLimit(reader *bufio.Reader, buf []byte, limit int) ([]byte, error) {
	// Read $
	if err := expectNextByte(reader, symbolBulkString); err != nil {
		return nil, err
	}

	// Read length
	length, err := readLength(reader)
	if err != nil {
		return nil, err
	}

	// Length must be greater than 0
	if length < 1 {
		return nil, Error{ErrCodeBadFormat, fmt.Sprintf("bulk string length must be greater than 0, got %d", length)}
	}

	// Length must be less than or equal to the buffer size
	if length > len(buf) {
		return nil, Error{ErrCodeLimits, fmt.Sprintf("bulk string length must be less than or equal to %d, got %d", len(buf), length)}
	}

	// Length must be less than or equal to the limit
	if length > limit {
		return nil, Error{ErrCodeLimits, "bulk string bytes limit reached"}
	}

	// We use a slice of the buffer up to length because that's how many
	// bytes we need to read and io.ReadFull() will read len(bytes) bytes.
	bytes := buf[:length]
	if _, err := io.ReadFull(reader, bytes); err != nil {
		return nil, err
	}

	// Consume CRLF from the reader
	if err := consumeCRLF(reader); err != nil {
		return nil, err
	}

	// Return the read bytes
	return bytes, nil
}

// consumeCRLF consumes the CRLF separator from the reader.
// Returns an error if the CRLF separator is not found.
func consumeCRLF(reader *bufio.Reader) error {
	for _, b := range separatorCRLF {
		if err := expectNextByte(reader, byte(b)); err != nil {
			return err
		}
	}
	return nil
}

func isPair(arrLength int) bool {
	return arrLength%2 == 0
}

func numberOfPairs(arrLength int) int {
	return arrLength / 2
}

func isValidTimestamp(timestamp string) bool {
	return timestampRegex.MatchString(timestamp)
}
