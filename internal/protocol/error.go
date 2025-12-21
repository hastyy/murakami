package protocol

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrCodeBadFormat      = ErrorCode("ERR_BAD_FORMAT")
	ErrCodeLimits         = ErrorCode("ERR_LIMITS")
	ErrCodeStreamExists   = ErrorCode("ERR_STREAM_EXISTS")
	ErrCodeUnknownStream  = ErrorCode("ERR_UNKNOWN_STREAM")
	ErrCodeNonMonotonicID = ErrorCode("ERR_NON_MONOTONIC_ID")
)

type Error struct {
	Code        ErrorCode
	Message     string
	recoverable bool
}

func (e Error) Error() string {
	return fmt.Sprintf("%c%s %s%s", symbolError, e.Code, e.Message, separatorCRLF)
}

func (e Error) IsRecoverable() bool {
	return e.recoverable
}

func BadFormatErrorf(format string, a ...any) Error {
	return Error{Code: ErrCodeBadFormat, recoverable: false, Message: fmt.Sprintf(format, a...)}
}

func LimitsErrorf(format string, a ...any) Error {
	return Error{Code: ErrCodeLimits, recoverable: false, Message: fmt.Sprintf(format, a...)}
}

func StreamExistsErrorf(format string, a ...any) Error {
	return Error{Code: ErrCodeStreamExists, recoverable: true, Message: fmt.Sprintf(format, a...)}
}

func UnknownStreamErrorf(format string, a ...any) Error {
	return Error{Code: ErrCodeUnknownStream, recoverable: true, Message: fmt.Sprintf(format, a...)}
}

func NonMonotonicIDErrorf(format string, a ...any) Error {
	return Error{Code: ErrCodeNonMonotonicID, recoverable: true, Message: fmt.Sprintf(format, a...)}
}

func IsProtocolError(err error) (Error, bool) {
	var e Error
	if errors.As(err, &e) {
		return e, true
	}
	return Error{}, false
}
