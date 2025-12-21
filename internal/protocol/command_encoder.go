package protocol

import (
	"bufio"
	"errors"
)

type CommandEncoder struct{}

func NewCommandEncoder() *CommandEncoder {
	return &CommandEncoder{}
}

func (e *CommandEncoder) EncodeCreateCommand(w *bufio.Writer, cmd CreateCommand) error {
	return errors.New("not implemented")
}

func (e *CommandEncoder) EncodeAppendCommand(w *bufio.Writer, cmd AppendCommand) error {
	return errors.New("not implemented")
}

func (e *CommandEncoder) EncodeReadCommand(w *bufio.Writer, cmd ReadCommand) error {
	return errors.New("not implemented")
}

func (e *CommandEncoder) EncodeTrimCommand(w *bufio.Writer, cmd TrimCommand) error {
	return errors.New("not implemented")
}

func (e *CommandEncoder) EncodeDeleteCommand(w *bufio.Writer, cmd DeleteCommand) error {
	return errors.New("not implemented")
}
