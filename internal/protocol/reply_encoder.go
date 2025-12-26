package protocol

import (
	"bufio"
	"strconv"
)

const ReplyOK = "+OK\r\n"

// ReplyEncoder is the server-side protocol encoder that serializes S3P responses to client byte streams.
// It encodes success responses (OK, bulk strings, records) and error responses according to the
// S3P protocol specification. All methods write to a buffered writer and return any I/O errors encountered.
// The caller is responsible for flushing the writer after encoding.
type ReplyEncoder struct{}

// NewReplyEncoder creates a new ReplyEncoder.
func NewReplyEncoder() *ReplyEncoder {
	return &ReplyEncoder{}
}

// EncodeOK encodes a simple OK response ("+OK\r\n").
// Used for successful CREATE, TRIM, and DELETE operations that don't return data.
func (e *ReplyEncoder) EncodeOK(w *bufio.Writer) error {
	_, err := w.WriteString(ReplyOK)
	return err
}

// EncodeError encodes a protocol error response in the format "-ERROR_CODE message\r\n".
// The error code and message are extracted from the protocol.Error.
// Used when operations fail with expected error conditions (stream exists, limits exceeded, etc.).
func (e *ReplyEncoder) EncodeError(w *bufio.Writer, error Error) error {
	_, err := w.WriteString(error.Error())
	return err
}

// EncodeBulkString encodes a bulk string response in the format "$length\r\nvalue\r\n".
// Used for successful APPEND operations to return the generated record ID.
func (e *ReplyEncoder) EncodeBulkString(w *bufio.Writer, value string) error {
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

// EncodeRecords encodes an array of records in the format "*count*2\r\n$id_len\r\nid\r\n$val_len\r\nvalue\r\n...".
// Each record is encoded as two consecutive bulk strings: the ID followed by the value.
// The array length is records * 2 to account for this pairing.
// Used for successful READ operations to return matching records.
func (e *ReplyEncoder) EncodeRecords(w *bufio.Writer, records []Record) error {
	err := w.WriteByte(symbolArray)
	if err != nil {
		return err
	}
	// Array length is records * 2 because we encode [id1, record1, id2, record2, ...]
	_, err = w.WriteString(strconv.Itoa(len(records) * 2))
	if err != nil {
		return err
	}
	_, err = w.WriteString(separatorCRLF)
	if err != nil {
		return err
	}

	for _, record := range records {
		// Encode ID as bulk string
		err := w.WriteByte(symbolBulkString)
		if err != nil {
			return err
		}
		_, err = w.WriteString(strconv.Itoa(len(record.ID)))
		if err != nil {
			return err
		}
		_, err = w.WriteString(separatorCRLF)
		if err != nil {
			return err
		}
		_, err = w.WriteString(record.ID)
		if err != nil {
			return err
		}
		_, err = w.WriteString(separatorCRLF)
		if err != nil {
			return err
		}

		// Encode Value as bulk string
		err = w.WriteByte(symbolBulkString)
		if err != nil {
			return err
		}
		_, err = w.WriteString(strconv.Itoa(len(record.Value)))
		if err != nil {
			return err
		}
		_, err = w.WriteString(separatorCRLF)
		if err != nil {
			return err
		}
		_, err = w.Write(record.Value)
		if err != nil {
			return err
		}
		_, err = w.WriteString(separatorCRLF)
		if err != nil {
			return err
		}
	}

	return nil
}
