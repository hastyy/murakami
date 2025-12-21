package protocol

import (
	"bufio"
	"strconv"
)

const ReplyOK = "+OK\r\n"

type ReplyEncoder struct{}

func NewReplyEncoder() *ReplyEncoder {
	return &ReplyEncoder{}
}

func (e *ReplyEncoder) EncodeOK(w *bufio.Writer) error {
	_, err := w.WriteString(ReplyOK)
	return err
}

func (e *ReplyEncoder) EncodeError(w *bufio.Writer, error Error) error {
	_, err := w.WriteString(error.Error())
	return err
}

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
