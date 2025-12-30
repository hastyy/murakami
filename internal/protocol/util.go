package protocol

import (
	"bufio"
	"fmt"
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
// TODO: Evaluate if this should take in a limit parameter to prevent attack vectors.
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

// readBulkStringLength reads a bulk string length from the protocol stream.
// Expects the bulk string prefix byte ('$') followed by a valid length (integer >= 0).
// Returns an error if the format is invalid.
func readBulkStringLength(r *bufio.Reader) (int, error) {
	if err := expectNextByte(r, symbolBulkString); err != nil {
		return 0, err
	}

	length, err := readLength(r)
	if err != nil {
		return 0, err
	}

	return length, nil
}

// readBulkBytesLengthWithLimit reads a bulk string length from the protocol stream with validation.
// Expects the bulk string prefix byte ('$') followed by a valid length (integer > 0).
// The limit parameter specifies the maximum allowed length.
// Returns an error if length < 1, length > limit, or if the format is invalid.
func readBulkBytesLengthWithLimit(r *bufio.Reader, limit int) (int, error) {
	assert.OK(limit > 0, "limit must be greater than 0")

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

	return length, nil
}

// readNBulkBytes reads exactly n bytes from the reader into the provided buffer.
// The buffer must be large enough to hold n bytes.
// Returns the number of bytes read and any error encountered.
func readNBulkBytes(r *bufio.Reader, buf []byte, n int) (int, error) {
	assert.OK(n > 0, "n must be greater than 0")

	// TODO: look into bug here where if the underlying buffered readeer buffer doesn't contain all the requested bytes at once, instead of pulling more from the underlying reader, it will return an error.
	nread, err := io.ReadFull(r, buf[:n])
	if err != nil {
		return 0, err
	}

	assert.OK(nread == n, "io.ReadFull() did not read the expected number of bytes, got %d expected %d", nread, n)

	return nread, nil
}

// readBytes reads exactly n bytes from the reader.
// Returns an error if fewer than n bytes are available or if an I/O error occurs.
func readBytes(r *bufio.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)

	bytesRead, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	assert.OK(bytesRead == n, "io.ReadFull() did not read the expected number of bytes, got %d expected %d", bytesRead, n)

	return buf, nil
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
	assert.OK(limit > 0, "limit must be greater than 0")

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

// readSimpleString reads a simple string from the protocol stream.
// It expects two parts: the first byte must be the simple string symbol (+),
// followed by ASCII text terminated by CRLF.
// Returns an error if the format is invalid.
func readSimpleString(r *bufio.Reader) (string, error) {
	err := expectNextByte(r, symbolSimpleString)
	if err != nil {
		return "", err
	}

	line, err := readLine(r)
	if err != nil {
		return "", err
	}

	return line, nil
}

// readError reads a protocol error from the protocol stream.
// It expects the error format: -<ERROR_CODE> <message>\r\n
// Returns a protocol Error if successfully parsed, or a decoding error if the format is invalid.
func readError(r *bufio.Reader) (Error, error) {
	err := expectNextByte(r, symbolError)
	if err != nil {
		return Error{}, err
	}

	line, err := readLine(r)
	if err != nil {
		return Error{}, err
	}

	// Parse error code and message
	// Format: <ERROR_CODE> <message>
	// Error code is up to the first space
	spaceIdx := strings.Index(line, " ")
	if spaceIdx == -1 {
		return Error{}, fmt.Errorf("bad server reply format: expected error code and message separated by space, got: %s", line)
	}

	code := ErrorCode(line[:spaceIdx])
	message := line[spaceIdx+1:]

	// Match known error codes and construct appropriate protocol Error
	switch code {
	case ErrCodeBadFormat:
		return BadFormatErrorf("%s", message), nil
	case ErrCodeLimits:
		return LimitsErrorf("%s", message), nil
	case ErrCodeStreamExists:
		return StreamExistsErrorf("%s", message), nil
	case ErrCodeUnknownStream:
		return UnknownStreamErrorf("%s", message), nil
	case ErrCodeNonMonotonicID:
		return NonMonotonicIDErrorf("%s", message), nil
	default:
		return Error{}, fmt.Errorf("unrecognized error code: %s", code)
	}
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

// writeArrayHeader writes an array header to the provided writer.
// The format is: *<length>\r\n
// This is used by both the command encoder (client-side) and reply encoder (server-side)
// to write array headers before encoding array elements.
// Returns any I/O error encountered during writing.
func writeArrayHeader(w *bufio.Writer, length int) error {
	err := w.WriteByte(symbolArray)
	if err != nil {
		return err
	}

	_, err = w.WriteString(strconv.Itoa(length))
	if err != nil {
		return err
	}

	_, err = w.WriteString(separatorCRLF)
	if err != nil {
		return err
	}

	return nil
}

// writeBulkString writes a string as a bulk string to the provided writer.
// The format is: $<length>\r\n<value>\r\n
// This is used by both the command encoder (client-side) and reply encoder (server-side).
// Returns any I/O error encountered during writing.
func writeBulkString(w *bufio.Writer, value string) error {
	err := w.WriteByte(symbolBulkString)
	if err != nil {
		return err
	}

	_, err = w.WriteString(strconv.Itoa(len(value)))
	if err != nil {
		return err
	}

	_, err = w.WriteString(separatorCRLF)
	if err != nil {
		return err
	}

	_, err = w.WriteString(value)
	if err != nil {
		return err
	}

	_, err = w.WriteString(separatorCRLF)
	if err != nil {
		return err
	}

	return nil
}

// writeBulkBytes writes a byte slice as a bulk string to the provided writer.
// The format is: $<length>\r\n<bytes>\r\n
// This is the counterpart to readBulkBytes and is used to encode binary record data.
// Returns any I/O error encountered during writing.
func writeBulkBytes(w *bufio.Writer, value []byte) error {
	err := w.WriteByte(symbolBulkString)
	if err != nil {
		return err
	}

	_, err = w.WriteString(strconv.Itoa(len(value)))
	if err != nil {
		return err
	}

	_, err = w.WriteString(separatorCRLF)
	if err != nil {
		return err
	}

	_, err = w.Write(value)
	if err != nil {
		return err
	}

	_, err = w.WriteString(separatorCRLF)
	if err != nil {
		return err
	}

	return nil
}
