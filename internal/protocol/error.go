package protocol

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrCodeBadFormat     = ErrorCode("ERR_BAD_FORMAT")
	ErrCodeStreamExists  = ErrorCode("ERR_STREAM_EXISTS")
	ErrCodeUnknownStream = ErrorCode("ERR_UNKNOWN_STREAM")
	ErrCodeLimits        = ErrorCode("ERR_LIMITS")
)

type Error struct {
	Code    ErrorCode
	Message string
}

func (e Error) Error() string {
	return fmt.Sprintf("%c%s %s%s", symbolError, e.Code, e.Message, separatorCRLF)
}

func IsProtocolError(err error) (Error, bool) {
	var e Error
	if errors.As(err, &e) {
		return e, true
	}
	return Error{}, false
}
