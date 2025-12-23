package controller

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/hastyy/murakami/internal/protocol"
	"github.com/hastyy/murakami/internal/tcp"
	"github.com/stretchr/testify/require"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// mockTimeoutError is a mock net.Error that represents a timeout
type mockTimeoutError struct{}

func (e *mockTimeoutError) Error() string   { return "timeout" }
func (e *mockTimeoutError) Timeout() bool   { return true }
func (e *mockTimeoutError) Temporary() bool { return true }

type mockStreamStore struct {
	createStreamError  error
	createStreamCalled bool

	appendRecordsID     string
	appendRecordsError  error
	appendRecordsCalled bool

	readRecordsRecords []protocol.Record
	readRecordsError   error
	readRecordsCalled  bool

	trimStreamError  error
	trimStreamCalled bool

	deleteStreamError  error
	deleteStreamCalled bool
}

func (s *mockStreamStore) CreateStream(ctx context.Context, cmd protocol.CreateCommand) error {
	s.createStreamCalled = true
	return s.createStreamError
}

func (s *mockStreamStore) AppendRecords(ctx context.Context, cmd protocol.AppendCommand) (string, error) {
	s.appendRecordsCalled = true
	return s.appendRecordsID, s.appendRecordsError
}

func (s *mockStreamStore) ReadRecords(ctx context.Context, cmd protocol.ReadCommand) ([]protocol.Record, error) {
	s.readRecordsCalled = true
	return s.readRecordsRecords, s.readRecordsError
}

func (s *mockStreamStore) TrimStream(ctx context.Context, cmd protocol.TrimCommand) error {
	s.trimStreamCalled = true
	return s.trimStreamError
}

func (s *mockStreamStore) DeleteStream(ctx context.Context, cmd protocol.DeleteCommand) error {
	s.deleteStreamCalled = true
	return s.deleteStreamError
}

type mockCommandDecoder struct {
	spec                   protocol.CommandSpec
	decodeNextCommandError error

	createCommand            protocol.CreateCommand
	decodeCreateCommandError error

	appendCommand            protocol.AppendCommand
	decodeAppendCommandError error

	readCommand            protocol.ReadCommand
	decodeReadCommandError error

	trimCommand            protocol.TrimCommand
	decodeTrimCommandError error

	deleteCommand            protocol.DeleteCommand
	decodeDeleteCommandError error
}

func (d *mockCommandDecoder) DecodeNextCommand(r *bufio.Reader) (protocol.CommandSpec, error) {
	return d.spec, d.decodeNextCommandError
}

func (d *mockCommandDecoder) DecodeCreateCommand(r *bufio.Reader) (protocol.CreateCommand, error) {
	return d.createCommand, d.decodeCreateCommandError
}

func (d *mockCommandDecoder) DecodeAppendCommand(r *bufio.Reader) (protocol.AppendCommand, error) {
	return d.appendCommand, d.decodeAppendCommandError
}

func (d *mockCommandDecoder) DecodeReadCommand(r *bufio.Reader) (protocol.ReadCommand, error) {
	return d.readCommand, d.decodeReadCommandError
}

func (d *mockCommandDecoder) DecodeTrimCommand(r *bufio.Reader) (protocol.TrimCommand, error) {
	return d.trimCommand, d.decodeTrimCommandError
}

func (d *mockCommandDecoder) DecodeDeleteCommand(r *bufio.Reader) (protocol.DeleteCommand, error) {
	return d.deleteCommand, d.decodeDeleteCommandError
}

type mockReplyEncoder struct {
	encodeOKError         error
	encodeErrorError      error
	encodeBulkStringError error
	encodeRecordsError    error

	calledEncodeOK         bool
	calledEncodeError      bool
	calledEncodeBulkString bool
	calledEncodeRecords    bool

	encodedError      protocol.Error
	encodedBulkString string
	encodedRecords    []protocol.Record
}

func (e *mockReplyEncoder) EncodeOK(w *bufio.Writer) error {
	e.calledEncodeOK = true
	return e.encodeOKError
}

func (e *mockReplyEncoder) EncodeError(w *bufio.Writer, err protocol.Error) error {
	e.calledEncodeError = true
	e.encodedError = err
	return e.encodeErrorError
}

func (e *mockReplyEncoder) EncodeBulkString(w *bufio.Writer, value string) error {
	e.calledEncodeBulkString = true
	e.encodedBulkString = value
	return e.encodeBulkStringError
}

func (e *mockReplyEncoder) EncodeRecords(w *bufio.Writer, records []protocol.Record) error {
	e.calledEncodeRecords = true
	e.encodedRecords = records
	return e.encodeRecordsError
}

func TestHandle_CreateStream_Success(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       "CREATE",
			ArgsLength: 2,
		},
		createCommand: protocol.CreateCommand{
			StreamName: "test",
		},
	}
	store := &mockStreamStore{
		createStreamError: nil,
	}
	encoder := &mockReplyEncoder{}

	controller := New(store, decoder, encoder, discardLogger)
	require.NotNil(controller)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.False(close)

	require.True(encoder.calledEncodeOK)
	require.False(encoder.calledEncodeError)
	require.False(encoder.calledEncodeBulkString)
	require.False(encoder.calledEncodeRecords)

	require.NoError(store.createStreamError)
}

func TestHandle_DecodeNextCommand_Error(t *testing.T) {
	tests := []struct {
		name          string
		decodeError   error
		expectedClose bool
	}{
		{
			name:          "protocol error closes connection",
			decodeError:   protocol.BadFormatErrorf("invalid command"),
			expectedClose: true,
		},
		{
			name:          "limits error closes connection",
			decodeError:   protocol.LimitsErrorf("command name too long"),
			expectedClose: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			decoder := &mockCommandDecoder{
				decodeNextCommandError: test.decodeError,
			}
			store := &mockStreamStore{}
			encoder := &mockReplyEncoder{}

			controller := New(store, decoder, encoder, discardLogger)

			conn := tcp.NewConnection(1024, 1024)
			close := controller.Handle(t.Context(), conn)

			require.Equal(test.expectedClose, close)
			require.True(encoder.calledEncodeError)
		})
	}
}

func TestHandle_DecodeNextCommand_NonProtocolError(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		decodeNextCommandError: io.EOF,
	}
	store := &mockStreamStore{}
	encoder := &mockReplyEncoder{}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.True(close)
	require.False(encoder.calledEncodeError) // Non-protocol errors don't encode errors
}

func TestHandle_DecodeNextCommand_TimeoutError(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		decodeNextCommandError: &mockTimeoutError{},
	}
	store := &mockStreamStore{}
	encoder := &mockReplyEncoder{}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.False(close) // Timeout errors don't close the connection
	require.False(encoder.calledEncodeError)
}

func TestHandle_BadSpec(t *testing.T) {
	tests := []struct {
		name        string
		commandName string
		argsLength  int
	}{
		{
			name:        "bad spec for CREATE command",
			commandName: protocol.CommandCreate,
			argsLength:  1,
		},
		{
			name:        "bad spec for APPEND command",
			commandName: protocol.CommandAppend,
			argsLength:  2,
		},
		{
			name:        "bad spec for READ command",
			commandName: protocol.CommandRead,
			argsLength:  1,
		},
		{
			name:        "bad spec for TRIM command",
			commandName: protocol.CommandTrim,
			argsLength:  3,
		},
		{
			name:        "bad spec for DELETE command",
			commandName: protocol.CommandDelete,
			argsLength:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			decoder := &mockCommandDecoder{
				spec: protocol.CommandSpec{
					Name:       test.commandName,
					ArgsLength: test.argsLength,
				},
			}
			store := &mockStreamStore{}
			encoder := &mockReplyEncoder{}

			controller := New(store, decoder, encoder, discardLogger)

			conn := tcp.NewConnection(1024, 1024)
			close := controller.Handle(t.Context(), conn)

			require.True(close)
			require.True(encoder.calledEncodeError)
			require.Equal(protocol.ErrCodeBadFormat, encoder.encodedError.Code)
		})
	}
}

func TestHandle_UnknownCommand(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       "UNKNOWN",
			ArgsLength: 2,
		},
	}
	store := &mockStreamStore{}
	encoder := &mockReplyEncoder{}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.True(close)
	require.True(encoder.calledEncodeError)
	require.Equal(protocol.ErrCodeBadFormat, encoder.encodedError.Code)
}

func TestHandle_DecodeCommand_Errors(t *testing.T) {
	tests := []struct {
		name          string
		commandName   string
		argsLength    int
		decodeError   error
		expectedClose bool
	}{
		{
			name:          "error decoding CREATE command (non-recoverable)",
			commandName:   protocol.CommandCreate,
			argsLength:    2,
			decodeError:   protocol.BadFormatErrorf("invalid format"),
			expectedClose: true,
		},
		{
			name:          "error decoding CREATE command (recoverable)",
			commandName:   protocol.CommandCreate,
			argsLength:    2,
			decodeError:   protocol.StreamExistsErrorf("stream exists"),
			expectedClose: false,
		},
		{
			name:          "error decoding APPEND command (non-recoverable)",
			commandName:   protocol.CommandAppend,
			argsLength:    3,
			decodeError:   protocol.LimitsErrorf("record too large"),
			expectedClose: true,
		},
		{
			name:          "error decoding READ command (non-recoverable)",
			commandName:   protocol.CommandRead,
			argsLength:    2,
			decodeError:   protocol.BadFormatErrorf("invalid option"),
			expectedClose: true,
		},
		{
			name:          "error decoding TRIM command (non-recoverable)",
			commandName:   protocol.CommandTrim,
			argsLength:    2,
			decodeError:   protocol.BadFormatErrorf("missing MIN_ID"),
			expectedClose: true,
		},
		{
			name:          "error decoding DELETE command (non-recoverable)",
			commandName:   protocol.CommandDelete,
			argsLength:    2,
			decodeError:   protocol.LimitsErrorf("stream name too long"),
			expectedClose: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			decoder := &mockCommandDecoder{
				spec: protocol.CommandSpec{
					Name:       test.commandName,
					ArgsLength: test.argsLength,
				},
				decodeCreateCommandError: func() error {
					if test.commandName == protocol.CommandCreate {
						return test.decodeError
					}
					return nil
				}(),
				decodeAppendCommandError: func() error {
					if test.commandName == protocol.CommandAppend {
						return test.decodeError
					}
					return nil
				}(),
				decodeReadCommandError: func() error {
					if test.commandName == protocol.CommandRead {
						return test.decodeError
					}
					return nil
				}(),
				decodeTrimCommandError: func() error {
					if test.commandName == protocol.CommandTrim {
						return test.decodeError
					}
					return nil
				}(),
				decodeDeleteCommandError: func() error {
					if test.commandName == protocol.CommandDelete {
						return test.decodeError
					}
					return nil
				}(),
			}
			store := &mockStreamStore{}
			encoder := &mockReplyEncoder{}

			controller := New(store, decoder, encoder, discardLogger)

			conn := tcp.NewConnection(1024, 1024)
			close := controller.Handle(t.Context(), conn)

			require.Equal(test.expectedClose, close)
			require.True(encoder.calledEncodeError)
		})
	}
}

func TestHandle_CreateStream_Errors(t *testing.T) {
	tests := []struct {
		name          string
		storeError    error
		expectedClose bool
	}{
		{
			name:          "stream already exists (recoverable)",
			storeError:    protocol.StreamExistsErrorf("stream already exists"),
			expectedClose: false,
		},
		{
			name:          "limits error (non-recoverable)",
			storeError:    protocol.LimitsErrorf("stream name too long"),
			expectedClose: true,
		},
		{
			name:          "bad format error (non-recoverable)",
			storeError:    protocol.BadFormatErrorf("invalid stream name"),
			expectedClose: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			decoder := &mockCommandDecoder{
				spec: protocol.CommandSpec{
					Name:       protocol.CommandCreate,
					ArgsLength: 2,
				},
				createCommand: protocol.CreateCommand{StreamName: "test"},
			}
			store := &mockStreamStore{
				createStreamError: test.storeError,
			}
			encoder := &mockReplyEncoder{}

			controller := New(store, decoder, encoder, discardLogger)

			conn := tcp.NewConnection(1024, 1024)
			close := controller.Handle(t.Context(), conn)

			require.Equal(test.expectedClose, close)
			require.True(encoder.calledEncodeError)
			require.False(encoder.calledEncodeOK)
			require.True(store.createStreamCalled)
		})
	}
}

func TestHandle_CreateStream_EncodeOKError(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       protocol.CommandCreate,
			ArgsLength: 2,
		},
		createCommand: protocol.CreateCommand{StreamName: "test"},
	}
	store := &mockStreamStore{}
	encoder := &mockReplyEncoder{
		encodeOKError: protocol.BadFormatErrorf("write error"),
	}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.True(close)
	require.True(encoder.calledEncodeOK)
	require.True(store.createStreamCalled)
}

func TestHandle_AppendRecords_Errors(t *testing.T) {
	tests := []struct {
		name          string
		storeError    error
		expectedClose bool
	}{
		{
			name:          "unknown stream (recoverable)",
			storeError:    protocol.UnknownStreamErrorf("stream not found"),
			expectedClose: false,
		},
		{
			name:          "non-monotonic ID (recoverable)",
			storeError:    protocol.NonMonotonicIDErrorf("ID not monotonic"),
			expectedClose: false,
		},
		{
			name:          "limits error (non-recoverable)",
			storeError:    protocol.LimitsErrorf("record too large"),
			expectedClose: true,
		},
		{
			name:          "bad format error (non-recoverable)",
			storeError:    protocol.BadFormatErrorf("invalid record"),
			expectedClose: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			decoder := &mockCommandDecoder{
				spec: protocol.CommandSpec{
					Name:       protocol.CommandAppend,
					ArgsLength: 3,
				},
				appendCommand: protocol.AppendCommand{
					StreamName: "test",
					Records:    [][]byte{[]byte("data")},
				},
			}
			store := &mockStreamStore{
				appendRecordsError: test.storeError,
			}
			encoder := &mockReplyEncoder{}

			controller := New(store, decoder, encoder, discardLogger)

			conn := tcp.NewConnection(1024, 1024)
			close := controller.Handle(t.Context(), conn)

			require.Equal(test.expectedClose, close)
			require.True(encoder.calledEncodeError)
			require.False(encoder.calledEncodeBulkString)
			require.True(store.appendRecordsCalled)
		})
	}
}

func TestHandle_AppendRecords_EncodeBulkStringError(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       protocol.CommandAppend,
			ArgsLength: 3,
		},
		appendCommand: protocol.AppendCommand{
			StreamName: "test",
			Records:    [][]byte{[]byte("data")},
		},
	}
	store := &mockStreamStore{
		appendRecordsID: "1234567890-0",
	}
	encoder := &mockReplyEncoder{
		encodeBulkStringError: protocol.BadFormatErrorf("write error"),
	}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.True(close)
	require.True(encoder.calledEncodeBulkString)
	require.True(store.appendRecordsCalled)
}

func TestHandle_AppendRecords_Success(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       protocol.CommandAppend,
			ArgsLength: 3,
		},
		appendCommand: protocol.AppendCommand{
			StreamName: "test",
			Records:    [][]byte{[]byte("data")},
		},
	}
	store := &mockStreamStore{
		appendRecordsID: "1234567890-0",
	}
	encoder := &mockReplyEncoder{}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.False(close)
	require.True(encoder.calledEncodeBulkString)
	require.Equal("1234567890-0", encoder.encodedBulkString)
	require.False(encoder.calledEncodeError)
	require.True(store.appendRecordsCalled)
}

func TestHandle_ReadRecords_Errors(t *testing.T) {
	tests := []struct {
		name          string
		storeError    error
		expectedClose bool
	}{
		{
			name:          "unknown stream (recoverable)",
			storeError:    protocol.UnknownStreamErrorf("stream not found"),
			expectedClose: false,
		},
		{
			name:          "limits error (non-recoverable)",
			storeError:    protocol.LimitsErrorf("count out of range"),
			expectedClose: true,
		},
		{
			name:          "bad format error (non-recoverable)",
			storeError:    protocol.BadFormatErrorf("invalid MIN_ID"),
			expectedClose: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			decoder := &mockCommandDecoder{
				spec: protocol.CommandSpec{
					Name:       protocol.CommandRead,
					ArgsLength: 2,
				},
				readCommand: protocol.ReadCommand{
					StreamName: "test",
				},
			}
			store := &mockStreamStore{
				readRecordsError: test.storeError,
			}
			encoder := &mockReplyEncoder{}

			controller := New(store, decoder, encoder, discardLogger)

			conn := tcp.NewConnection(1024, 1024)
			close := controller.Handle(t.Context(), conn)

			require.Equal(test.expectedClose, close)
			require.True(encoder.calledEncodeError)
			require.False(encoder.calledEncodeRecords)
			require.True(store.readRecordsCalled)
		})
	}
}

func TestHandle_ReadRecords_EncodeRecordsError(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       protocol.CommandRead,
			ArgsLength: 2,
		},
		readCommand: protocol.ReadCommand{
			StreamName: "test",
		},
	}
	store := &mockStreamStore{
		readRecordsRecords: []protocol.Record{
			{ID: "1234567890-0", Value: []byte("data")},
		},
	}
	encoder := &mockReplyEncoder{
		encodeRecordsError: protocol.BadFormatErrorf("write error"),
	}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.True(close)
	require.True(encoder.calledEncodeRecords)
	require.True(store.readRecordsCalled)
}

func TestHandle_ReadRecords_Success(t *testing.T) {
	require := require.New(t)

	expectedRecords := []protocol.Record{
		{ID: "1234567890-0", Value: []byte("data1")},
		{ID: "1234567890-1", Value: []byte("data2")},
	}

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       protocol.CommandRead,
			ArgsLength: 2,
		},
		readCommand: protocol.ReadCommand{
			StreamName: "test",
		},
	}
	store := &mockStreamStore{
		readRecordsRecords: expectedRecords,
	}
	encoder := &mockReplyEncoder{}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.False(close)
	require.True(encoder.calledEncodeRecords)
	require.Equal(expectedRecords, encoder.encodedRecords)
	require.False(encoder.calledEncodeError)
	require.True(store.readRecordsCalled)
}

func TestHandle_TrimStream_Errors(t *testing.T) {
	tests := []struct {
		name          string
		storeError    error
		expectedClose bool
	}{
		{
			name:          "unknown stream (recoverable)",
			storeError:    protocol.UnknownStreamErrorf("stream not found"),
			expectedClose: false,
		},
		{
			name:          "limits error (non-recoverable)",
			storeError:    protocol.LimitsErrorf("invalid MIN_ID"),
			expectedClose: true,
		},
		{
			name:          "bad format error (non-recoverable)",
			storeError:    protocol.BadFormatErrorf("invalid MIN_ID format"),
			expectedClose: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			decoder := &mockCommandDecoder{
				spec: protocol.CommandSpec{
					Name:       protocol.CommandTrim,
					ArgsLength: 2,
				},
				trimCommand: protocol.TrimCommand{
					StreamName: "test",
				},
			}
			store := &mockStreamStore{
				trimStreamError: test.storeError,
			}
			encoder := &mockReplyEncoder{}

			controller := New(store, decoder, encoder, discardLogger)

			conn := tcp.NewConnection(1024, 1024)
			close := controller.Handle(t.Context(), conn)

			require.Equal(test.expectedClose, close)
			require.True(encoder.calledEncodeError)
			require.False(encoder.calledEncodeOK)
			require.True(store.trimStreamCalled)
		})
	}
}

func TestHandle_TrimStream_EncodeOKError(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       protocol.CommandTrim,
			ArgsLength: 2,
		},
		trimCommand: protocol.TrimCommand{
			StreamName: "test",
		},
	}
	store := &mockStreamStore{}
	encoder := &mockReplyEncoder{
		encodeOKError: protocol.BadFormatErrorf("write error"),
	}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.True(close)
	require.True(encoder.calledEncodeOK)
	require.True(store.trimStreamCalled)
}

func TestHandle_TrimStream_Success(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       protocol.CommandTrim,
			ArgsLength: 2,
		},
		trimCommand: protocol.TrimCommand{
			StreamName: "test",
		},
	}
	store := &mockStreamStore{}
	encoder := &mockReplyEncoder{}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.False(close)
	require.True(encoder.calledEncodeOK)
	require.False(encoder.calledEncodeError)
	require.True(store.trimStreamCalled)
}

func TestHandle_DeleteStream_Errors(t *testing.T) {
	tests := []struct {
		name          string
		storeError    error
		expectedClose bool
	}{
		{
			name:          "unknown stream (recoverable)",
			storeError:    protocol.UnknownStreamErrorf("stream not found"),
			expectedClose: false,
		},
		{
			name:          "limits error (non-recoverable)",
			storeError:    protocol.LimitsErrorf("stream name too long"),
			expectedClose: true,
		},
		{
			name:          "bad format error (non-recoverable)",
			storeError:    protocol.BadFormatErrorf("invalid stream name"),
			expectedClose: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			decoder := &mockCommandDecoder{
				spec: protocol.CommandSpec{
					Name:       protocol.CommandDelete,
					ArgsLength: 2,
				},
				deleteCommand: protocol.DeleteCommand{
					StreamName: "test",
				},
			}
			store := &mockStreamStore{
				deleteStreamError: test.storeError,
			}
			encoder := &mockReplyEncoder{}

			controller := New(store, decoder, encoder, discardLogger)

			conn := tcp.NewConnection(1024, 1024)
			close := controller.Handle(t.Context(), conn)

			require.Equal(test.expectedClose, close)
			require.True(encoder.calledEncodeError)
			require.False(encoder.calledEncodeOK)
			require.True(store.deleteStreamCalled)
		})
	}
}

func TestHandle_DeleteStream_EncodeOKError(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       protocol.CommandDelete,
			ArgsLength: 2,
		},
		deleteCommand: protocol.DeleteCommand{
			StreamName: "test",
		},
	}
	store := &mockStreamStore{}
	encoder := &mockReplyEncoder{
		encodeOKError: protocol.BadFormatErrorf("write error"),
	}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.True(close)
	require.True(encoder.calledEncodeOK)
	require.True(store.deleteStreamCalled)
}

func TestHandle_DeleteStream_Success(t *testing.T) {
	require := require.New(t)

	decoder := &mockCommandDecoder{
		spec: protocol.CommandSpec{
			Name:       protocol.CommandDelete,
			ArgsLength: 2,
		},
		deleteCommand: protocol.DeleteCommand{
			StreamName: "test",
		},
	}
	store := &mockStreamStore{}
	encoder := &mockReplyEncoder{}

	controller := New(store, decoder, encoder, discardLogger)

	conn := tcp.NewConnection(1024, 1024)
	close := controller.Handle(t.Context(), conn)

	require.False(close)
	require.True(encoder.calledEncodeOK)
	require.False(encoder.calledEncodeError)
	require.True(store.deleteStreamCalled)
}
