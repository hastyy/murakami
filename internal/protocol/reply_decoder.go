package protocol

import (
	"bufio"
	"errors"
)

type ReplyDecoder struct{}

func NewReplyDecoder() *ReplyDecoder {
	return &ReplyDecoder{}
}

func (d *ReplyDecoder) DecodeCreateReply(r *bufio.Reader) (CreateReply, error) {
	return CreateReply{}, errors.New("not implemented")
}

func (d *ReplyDecoder) DecodeAppendReply(r *bufio.Reader) (AppendReply, error) {
	return AppendReply{}, errors.New("not implemented")
}

func (d *ReplyDecoder) DecodeReadReply(r *bufio.Reader) (ReadReply, error) {
	return ReadReply{}, errors.New("not implemented")
}

func (d *ReplyDecoder) DecodeTrimReply(r *bufio.Reader) (TrimReply, error) {
	return TrimReply{}, errors.New("not implemented")
}

func (d *ReplyDecoder) DecodeDeleteReply(r *bufio.Reader) (DeleteReply, error) {
	return DeleteReply{}, errors.New("not implemented")
}
