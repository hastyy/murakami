package protocol

import (
	"bufio"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/hastyy/murakami/internal/assert"
)

var (
	// idRegex is a regular expression that matches a valid ID string.
	idRegex = regexp.MustCompile(`^\d{1,20}-\d{1,20}$`)

	// idMillisRegex is a regular expression that matches a valid ID milliseconds part.
	idMillisRegex = regexp.MustCompile(`^\d{1,20}$`)
)

// expectNextByte consumes the next byte from the reader with an expectation of which byte it should be.
// Returns an error if the byte is not the expected one.
func expectNextByte(r *bufio.Reader, expected byte) error {
	actual, err := r.ReadByte()
	if err != nil {
		return err
	}
	if actual != expected {
		return BadFormatErrorf("found unexpected byte(%c) in stream while expecting byte(%c)", actual, expected)
	}
	return nil
}

// readLine reads the remainder of the line content from the reader.
// This is used for lines we expect to only contain ASCII encoded strings.
func readLine(r *bufio.Reader) (string, error) {
	// Reads until the end of the line (should end with CRLF)
	//
	// We use ReadString() instead of ReadSlice() because:
	// - ReadSlice() returns []byte, which requires casting to string anyway (allocates)
	// - ReadString() internally uses a StringBuilder and converts to string using unsafe,
	//   avoiding an extra copy while handling edge cases where data exceeds buffer size
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}

	// Line should not be empty
	if len(line)-len(separatorCRLF) < 1 {
		return "", BadFormatErrorf("unexpected empty line")
	}

	// Return line content without CRLF.
	//
	// String slicing creates a new string header but shares the underlying byte array,
	// so no extra allocations happen here.
	return line[:len(line)-len(separatorCRLF)], nil
}

// readLength expects to read a full line of digits from the reader.
// Returns an error if the line is not a valid integer >= 0.
func readLength(r *bufio.Reader) (int, error) {
	line, err := readLine(r)
	if err != nil {
		return 0, err
	}

	length, err := strconv.Atoi(line)
	if err != nil || length < 0 {
		return 0, BadFormatErrorf("invalid length: %s", line)
	}

	return length, nil
}

// readArrayLength reads an array length from the protocol stream.
// Expects the array prefix byte ('*') followed by a valid length (integer >= 0).
// Returns an error if the format is invalid.
func readArrayLength(r *bufio.Reader) (int, error) {
	if err := expectNextByte(r, symbolArray); err != nil {
		return 0, err
	}

	length, err := readLength(r)
	if err != nil {
		return 0, err
	}

	return length, nil
}

// consumeCRLF consumes the CRLF separator from the reader.
// Returns an error if the CRLF separator is not found.
func consumeCRLF(r *bufio.Reader) error {
	for _, b := range separatorCRLF {
		if err := expectNextByte(r, byte(b)); err != nil {
			return err
		}
	}
	return nil
}

// readBulkString reads a bulk string from the protocol stream.
// It expects two lines: the first line must have the bulk string symbol ($) followed by
// a valid length (integer >= 0), and the second line must contain exactly length bytes.
// The limit parameter specifies the maximum number of bytes this function is willing to read.
// Returns an error if length > limit or if the format is invalid.
func readBulkString(r *bufio.Reader, limit int) (string, error) {
	assert.OK(limit > 0, "limit must be greater than 0")

	err := expectNextByte(r, symbolBulkString)
	if err != nil {
		return "", err
	}

	length, err := readLength(r)
	if err != nil {
		return "", err
	}

	if length < 1 {
		return "", LimitsErrorf("bulk string length must be greater than 0, got %d", length)
	}

	if length > limit {
		return "", LimitsErrorf("bulk string length must be less than or equal to %d, got %d", limit, length)
	}

	var sb strings.Builder
	sb.Grow(length)

	n, err := io.CopyN(&sb, r, int64(length))
	if err != nil {
		return "", err
	}

	assert.OK(n == int64(length), "io.CopyN() did not read the expected number of bytes, got %d expected %d", n, length)

	if err := consumeCRLF(r); err != nil {
		return "", err
	}

	return sb.String(), nil
}

// isPair checks if the array length is even.
func isPair(arrLength int) bool {
	return arrLength%2 == 0
}

// numberOfPairs returns the number of pairs in the array.
func numberOfPairs(arrLength int) int {
	return arrLength / 2
}

// readBulkBytes reads a bulk of bytes from the protocol stream.
// It expects two lines: the first line must have the bulk string symbol ($) followed by
// a valid length (integer >= 0), and the second line must contain exactly length bytes.
// The limit parameter specifies the maximum number of bytes this function is willing to read.
// Returns an error if length > limit or if the format is invalid.
// It follows the io.Writer pattern of retuning the number of bytes written and an error.
// The buffer is used to store the bytes read from the protocol stream. So the caller should
// slice the buffer up n in order to get the slice of bytes that was read.
func readBulkBytes(r *bufio.Reader, buf []byte, limit int) (int, error) {
	if err := expectNextByte(r, symbolBulkString); err != nil {
		return 0, err
	}

	length, err := readLength(r)
	if err != nil {
		return 0, err
	}

	if length < 1 {
		return 0, LimitsErrorf("bulk string length must be greater than 0, got %d", length)
	}

	if length > limit {
		return 0, LimitsErrorf("bulk string length must be less than or equal to %d, got %d", limit, length)
	}

	n, err := io.ReadFull(r, buf[:length])
	if err != nil {
		return 0, err
	}

	assert.OK(n == length, "io.ReadFull() did not read the expected number of bytes, got %d expected %d", n, length)

	if err := consumeCRLF(r); err != nil {
		return 0, err
	}

	return length, nil
}

// isValidID checks if the ID string is valid.
// The ID string is expected to be in the format of "<ms>-<seq>",
// where <ms> and <seq> are base-10 unsigned integers without leading sign.
func IsValidID(id string) bool {
	return idRegex.MatchString(id)
}

// isValidIDMillis checks if the ID milliseconds part is valid.
// The ID milliseconds part is expected to be a base-10 unsigned integer without leading sign.
func IsValidIDMillis(millis string) bool {
	return idMillisRegex.MatchString(millis)
}
