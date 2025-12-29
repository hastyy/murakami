package protocol

import (
	"bufio"
	"fmt"
)

type ReplyDecoder struct{}

func NewReplyDecoder() *ReplyDecoder {
	return &ReplyDecoder{}
}

func (d *ReplyDecoder) DecodeCreateReply(r *bufio.Reader) (CreateReply, error) {
	peek, err := r.Peek(1)
	if err != nil {
		return CreateReply{}, err
	}
	nextByte := peek[0]

	switch nextByte {
	case symbolSimpleString:
		str, err := readSimpleString(r)
		if err != nil {
			return CreateReply{}, err
		}
		if str != ReplyOK {
			return CreateReply{}, fmt.Errorf("expected %s, got %s", ReplyOK, str)
		}
		return CreateReply{Ok: true}, nil
	case symbolError:
		error, err := readError(r)
		if err != nil {
			return CreateReply{}, err
		}
		return CreateReply{Ok: false, Err: error}, nil
	default:
		return CreateReply{}, fmt.Errorf("expected simple string or error byte, got %c", nextByte)
	}
}

func (d *ReplyDecoder) DecodeAppendReply(r *bufio.Reader) (AppendReply, error) {
	peek, err := r.Peek(1)
	if err != nil {
		return AppendReply{}, err
	}
	nextByte := peek[0]

	switch nextByte {
	case symbolBulkString:
		// We pass a max length of 41 because that's just enough to hold the maximum ID string length.
		str, err := readBulkString(r, 41)
		if err != nil {
			return AppendReply{}, err
		}
		return AppendReply{ID: str}, nil
	case symbolError:
		error, err := readError(r)
		if err != nil {
			return AppendReply{}, err
		}
		return AppendReply{Err: error}, nil
	default:
		return AppendReply{}, fmt.Errorf("expected bulk string or error byte, got %c", nextByte)
	}
}

func (d *ReplyDecoder) DecodeReadReply(r *bufio.Reader) (ReadReply, error) {
	peek, err := r.Peek(1)
	if err != nil {
		return ReadReply{}, err
	}
	nextByte := peek[0]

	switch nextByte {
	case symbolArray:
		arrLength, err := readArrayLength(r)
		if err != nil {
			return ReadReply{}, err
		}
		if !isPair(arrLength) {
			return ReadReply{}, fmt.Errorf("expected even array length, got %d", arrLength)
		}

		records := make([]Record, 0, numberOfPairs(arrLength))
		for range numberOfPairs(arrLength) {
			id, err := readBulkString(r, 41)
			if err != nil {
				return ReadReply{}, err
			}
			bsLength, err := readBulkStringLength(r)
			if err != nil {
				return ReadReply{}, err
			}
			bs, err := readBytes(r, bsLength)
			if err != nil {
				return ReadReply{}, err
			}
			err = consumeCRLF(r)
			if err != nil {
				return ReadReply{}, err
			}
			records = append(records, Record{ID: id, Value: bs})
		}

		return ReadReply{Records: records}, nil
	case symbolError:
		error, err := readError(r)
		if err != nil {
			return ReadReply{}, err
		}
		return ReadReply{Err: error}, nil
	default:
		return ReadReply{}, fmt.Errorf("expected array or error byte, got %c", nextByte)
	}
}

func (d *ReplyDecoder) DecodeTrimReply(r *bufio.Reader) (TrimReply, error) {
	peek, err := r.Peek(1)
	if err != nil {
		return TrimReply{}, err
	}
	nextByte := peek[0]

	switch nextByte {
	case symbolSimpleString:
		str, err := readSimpleString(r)
		if err != nil {
			return TrimReply{}, err
		}
		if str != ReplyOK {
			return TrimReply{}, fmt.Errorf("expected %s, got %s", ReplyOK, str)
		}
		return TrimReply{Ok: true}, nil
	case symbolError:
		error, err := readError(r)
		if err != nil {
			return TrimReply{}, err
		}
		return TrimReply{Ok: false, Err: error}, nil
	default:
		return TrimReply{}, fmt.Errorf("expected simple string or error byte, got %c", nextByte)
	}
}

func (d *ReplyDecoder) DecodeDeleteReply(r *bufio.Reader) (DeleteReply, error) {
	peek, err := r.Peek(1)
	if err != nil {
		return DeleteReply{}, err
	}
	nextByte := peek[0]

	switch nextByte {
	case symbolSimpleString:
		str, err := readSimpleString(r)
		if err != nil {
			return DeleteReply{}, err
		}
		if str != ReplyOK {
			return DeleteReply{}, fmt.Errorf("expected %s, got %s", ReplyOK, str)
		}
		return DeleteReply{Ok: true}, nil
	case symbolError:
		error, err := readError(r)
		if err != nil {
			return DeleteReply{}, err
		}
		return DeleteReply{Ok: false, Err: error}, nil
	default:
		return DeleteReply{}, fmt.Errorf("expected simple string or error byte, got %c", nextByte)
	}
}
