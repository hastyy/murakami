package protocol

import (
	"fmt"
	"io"
)

var (
	// +OK\r\n
	ReplyOK = fmt.Sprintf("%cOK%s", symbolSimpleString, separatorCRLF)
)

// ReplyEncoder is meant to be used by the server to encode replies to the client.
type ReplyEncoder struct{}

// NewReplyEncoder returns a new ReplyEncoder.
func NewReplyEncoder() *ReplyEncoder {
	return &ReplyEncoder{}
}

func (e *ReplyEncoder) EncodeOK(writer io.Writer) error {
	_, err := writer.Write([]byte(ReplyOK))
	return err
}

func (e *ReplyEncoder) EncodeError(writer io.Writer, error Error) error {
	_, err := writer.Write([]byte(error.Error()))
	return err
}

func (e *ReplyEncoder) EncodeBulkString(writer io.Writer, value string) error {
	return e.encodeBulkBytes(writer, []byte(value))
}

func (e *ReplyEncoder) EncodeBulkBytesArray(writer io.Writer, values [][]byte) error {
	_, err := fmt.Fprintf(writer, "%c%d", symbolArray, len(values))
	if err != nil {
		return err
	}

	_, err = writer.Write([]byte(separatorCRLF))
	if err != nil {
		return err
	}

	for _, value := range values {
		err := e.encodeBulkBytes(writer, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *ReplyEncoder) encodeBulkBytes(writer io.Writer, value []byte) error {
	_, err := fmt.Fprintf(writer, "%c%d", symbolBulkString, len(value))
	if err != nil {
		return err
	}

	_, err = writer.Write([]byte(separatorCRLF))
	if err != nil {
		return err
	}

	_, err = writer.Write(value)
	if err != nil {
		return err
	}

	_, err = writer.Write([]byte(separatorCRLF))
	if err != nil {
		return err
	}

	return nil
}
