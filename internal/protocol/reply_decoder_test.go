package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeCreateReply(t *testing.T) {
	d := NewReplyDecoder()

	tests := []struct {
		name            string
		reader          *bufio.Reader
		expectedReply   CreateReply
		expectedErrCode ErrorCode
	}{
		{
			name:   "successfully decode OK reply",
			reader: readerFrom("+OK\r\n"),
			expectedReply: CreateReply{
				Ok: true,
			},
		},
		{
			name:   "successfully decode ERR_STREAM_EXISTS error",
			reader: readerFrom("-ERR_STREAM_EXISTS stream already exists\r\n"),
			expectedReply: CreateReply{
				Ok:  false,
				Err: StreamExistsErrorf("stream already exists"),
			},
		},
		{
			name:   "successfully decode ERR_BAD_FORMAT error",
			reader: readerFrom("-ERR_BAD_FORMAT invalid stream name\r\n"),
			expectedReply: CreateReply{
				Ok:  false,
				Err: BadFormatErrorf("invalid stream name"),
			},
		},
		{
			name:   "successfully decode ERR_LIMITS error",
			reader: readerFrom("-ERR_LIMITS stream name too long\r\n"),
			expectedReply: CreateReply{
				Ok:  false,
				Err: LimitsErrorf("stream name too long"),
			},
		},
		{
			name:            "fails with invalid simple string value",
			reader:          readerFrom("+INVALID\r\n"),
			expectedReply:   CreateReply{},
			expectedErrCode: "",
		},
		{
			name:            "fails with invalid first byte",
			reader:          readerFrom("$5\r\nhello\r\n"),
			expectedReply:   CreateReply{},
			expectedErrCode: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			var errCode ErrorCode
			reply, err := d.DecodeCreateReply(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					// Non-protocol error (e.g., invalid format)
					require.Equal(test.expectedErrCode, errCode)
					return
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			require.Equal(test.expectedReply.Ok, reply.Ok)
			require.Equal(test.expectedReply.Err, reply.Err)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		reader := readerFrom("+OK\r")
		_, err := d.DecodeCreateReply(reader)

		require.Error(err)
		require.Equal(io.EOF, err)
	})
}

func TestDecodeAppendReply(t *testing.T) {
	d := NewReplyDecoder()

	tests := []struct {
		name            string
		reader          *bufio.Reader
		expectedReply   AppendReply
		expectedErrCode ErrorCode
	}{
		{
			name:   "successfully decode ID reply",
			reader: readerFrom("$15\r\n1700000001234-0\r\n"),
			expectedReply: AppendReply{
				ID: "1700000001234-0",
			},
		},
		{
			name:   "successfully decode ID reply with large ID",
			reader: readerFrom("$41\r\n12345678901234567890-12345678901234567890\r\n"),
			expectedReply: AppendReply{
				ID: "12345678901234567890-12345678901234567890",
			},
		},
		{
			name:   "successfully decode ERR_UNKNOWN_STREAM error",
			reader: readerFrom("-ERR_UNKNOWN_STREAM stream not found\r\n"),
			expectedReply: AppendReply{
				Err: UnknownStreamErrorf("stream not found"),
			},
		},
		{
			name:   "successfully decode ERR_NON_MONOTONIC_ID error",
			reader: readerFrom("-ERR_NON_MONOTONIC_ID id must be greater than last\r\n"),
			expectedReply: AppendReply{
				Err: NonMonotonicIDErrorf("id must be greater than last"),
			},
		},
		{
			name:   "successfully decode ERR_BAD_FORMAT error",
			reader: readerFrom("-ERR_BAD_FORMAT invalid record format\r\n"),
			expectedReply: AppendReply{
				Err: BadFormatErrorf("invalid record format"),
			},
		},
		{
			name:   "successfully decode ERR_LIMITS error",
			reader: readerFrom("-ERR_LIMITS record too large\r\n"),
			expectedReply: AppendReply{
				Err: LimitsErrorf("record too large"),
			},
		},
		{
			name:            "fails with invalid first byte",
			reader:          readerFrom("+OK\r\n"),
			expectedReply:   AppendReply{},
			expectedErrCode: "",
		},
		{
			// TODO: review
			name:            "fails when ID exceeds limit",
			reader:          readerFrom("$50\r\n12345678901234567890123456789012345678901234567890\r\n"),
			expectedReply:   AppendReply{},
			expectedErrCode: ErrCodeLimits,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			var errCode ErrorCode
			reply, err := d.DecodeAppendReply(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					// Non-protocol error (e.g., invalid format)
					require.Equal(test.expectedErrCode, errCode)
					return
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			require.Equal(test.expectedReply.ID, reply.ID)
			require.Equal(test.expectedReply.Err, reply.Err)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		reader := readerFrom("$15\r\n1700000001234-0\r")
		_, err := d.DecodeAppendReply(reader)

		require.Error(err)
		require.Equal(io.EOF, err)
	})
}

func TestDecodeReadReply(t *testing.T) {
	d := NewReplyDecoder()

	tests := []struct {
		name            string
		reader          *bufio.Reader
		expectedReply   ReadReply
		expectedErrCode ErrorCode
	}{
		{
			name:   "successfully decode empty records array",
			reader: readerFrom("*0\r\n"),
			expectedReply: ReadReply{
				Records: []Record{},
			},
		},
		{
			name:   "successfully decode single record",
			reader: readerFrom("*2\r\n$15\r\n1700000001234-0\r\n$5\r\nhello\r\n"),
			expectedReply: ReadReply{
				Records: []Record{
					{ID: "1700000001234-0", Value: []byte("hello")},
				},
			},
		},
		{
			name:   "successfully decode multiple records",
			reader: readerFrom("*4\r\n$15\r\n1700000001234-0\r\n$5\r\nhello\r\n$15\r\n1700000001235-0\r\n$5\r\nworld\r\n"),
			expectedReply: ReadReply{
				Records: []Record{
					{ID: "1700000001234-0", Value: []byte("hello")},
					{ID: "1700000001235-0", Value: []byte("world")},
				},
			},
		},
		{
			name:   "successfully decode record with binary data",
			reader: readerFrom("*2\r\n$3\r\n1-0\r\n$5\r\n\x00\x01\x02\x03\x04\r\n"),
			expectedReply: ReadReply{
				Records: []Record{
					{ID: "1-0", Value: []byte{0x00, 0x01, 0x02, 0x03, 0x04}},
				},
			},
		},
		{
			name:   "successfully decode record with large value",
			reader: readerFrom(fmt.Sprintf("*2\r\n$3\r\n1-0\r\n$1000\r\n%s\r\n", strings.Repeat("x", 1000))),
			expectedReply: ReadReply{
				Records: []Record{
					{ID: "1-0", Value: []byte(strings.Repeat("x", 1000))},
				},
			},
		},
		{
			name:   "successfully decode ERR_UNKNOWN_STREAM error",
			reader: readerFrom("-ERR_UNKNOWN_STREAM stream not found\r\n"),
			expectedReply: ReadReply{
				Err: UnknownStreamErrorf("stream not found"),
			},
		},
		{
			name:   "successfully decode ERR_BAD_FORMAT error",
			reader: readerFrom("-ERR_BAD_FORMAT invalid MIN_ID\r\n"),
			expectedReply: ReadReply{
				Err: BadFormatErrorf("invalid MIN_ID"),
			},
		},
		{
			name:   "successfully decode ERR_LIMITS error",
			reader: readerFrom("-ERR_LIMITS COUNT too large\r\n"),
			expectedReply: ReadReply{
				Err: LimitsErrorf("COUNT too large"),
			},
		},
		{
			name:            "fails with odd array length",
			reader:          readerFrom("*3\r\n$15\r\n1700000001234-0\r\n$5\r\nhello\r\n$3\r\n1-0\r\n"),
			expectedReply:   ReadReply{},
			expectedErrCode: "",
		},
		{
			name:            "fails with invalid first byte",
			reader:          readerFrom("+OK\r\n"),
			expectedReply:   ReadReply{},
			expectedErrCode: "",
		},
		{
			// TODO: review
			name:            "fails when ID exceeds limit",
			reader:          readerFrom("*2\r\n$50\r\n12345678901234567890123456789012345678901234567890\r\n$5\r\nhello\r\n"),
			expectedReply:   ReadReply{},
			expectedErrCode: ErrCodeLimits,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			var errCode ErrorCode
			reply, err := d.DecodeReadReply(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					// Non-protocol error (e.g., invalid format)
					require.Equal(test.expectedErrCode, errCode)
					return
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			require.Equal(len(test.expectedReply.Records), len(reply.Records))
			for i := range test.expectedReply.Records {
				require.Equal(test.expectedReply.Records[i].ID, reply.Records[i].ID)
				require.Equal(test.expectedReply.Records[i].Value, reply.Records[i].Value)
			}
			require.Equal(test.expectedReply.Err, reply.Err)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		reader := readerFrom("*2\r\n$15\r\n1700000001234-0\r")
		_, err := d.DecodeReadReply(reader)

		require.Error(err)
		require.Equal(io.EOF, err)
	})
}

func TestDecodeTrimReply(t *testing.T) {
	d := NewReplyDecoder()

	tests := []struct {
		name            string
		reader          *bufio.Reader
		expectedReply   TrimReply
		expectedErrCode ErrorCode
	}{
		{
			name:   "successfully decode OK reply",
			reader: readerFrom("+OK\r\n"),
			expectedReply: TrimReply{
				Ok: true,
			},
		},
		{
			name:   "successfully decode ERR_UNKNOWN_STREAM error",
			reader: readerFrom("-ERR_UNKNOWN_STREAM stream not found\r\n"),
			expectedReply: TrimReply{
				Ok:  false,
				Err: UnknownStreamErrorf("stream not found"),
			},
		},
		{
			name:   "successfully decode ERR_BAD_FORMAT error",
			reader: readerFrom("-ERR_BAD_FORMAT invalid MIN_ID\r\n"),
			expectedReply: TrimReply{
				Ok:  false,
				Err: BadFormatErrorf("invalid MIN_ID"),
			},
		},
		{
			name:   "successfully decode ERR_LIMITS error",
			reader: readerFrom("-ERR_LIMITS stream name too long\r\n"),
			expectedReply: TrimReply{
				Ok:  false,
				Err: LimitsErrorf("stream name too long"),
			},
		},
		{
			name:            "fails with invalid simple string value",
			reader:          readerFrom("+INVALID\r\n"),
			expectedReply:   TrimReply{},
			expectedErrCode: "",
		},
		{
			name:            "fails with invalid first byte",
			reader:          readerFrom("$5\r\nhello\r\n"),
			expectedReply:   TrimReply{},
			expectedErrCode: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			var errCode ErrorCode
			reply, err := d.DecodeTrimReply(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					// Non-protocol error (e.g., invalid format)
					require.Equal(test.expectedErrCode, errCode)
					return
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			require.Equal(test.expectedReply.Ok, reply.Ok)
			require.Equal(test.expectedReply.Err, reply.Err)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		reader := readerFrom("+OK\r")
		_, err := d.DecodeTrimReply(reader)

		require.Error(err)
		require.Equal(io.EOF, err)
	})
}

func TestDecodeDeleteReply(t *testing.T) {
	d := NewReplyDecoder()

	tests := []struct {
		name            string
		reader          *bufio.Reader
		expectedReply   DeleteReply
		expectedErrCode ErrorCode
	}{
		{
			name:   "successfully decode OK reply",
			reader: readerFrom("+OK\r\n"),
			expectedReply: DeleteReply{
				Ok: true,
			},
		},
		{
			name:   "successfully decode ERR_UNKNOWN_STREAM error",
			reader: readerFrom("-ERR_UNKNOWN_STREAM stream not found\r\n"),
			expectedReply: DeleteReply{
				Ok:  false,
				Err: UnknownStreamErrorf("stream not found"),
			},
		},
		{
			name:   "successfully decode ERR_BAD_FORMAT error",
			reader: readerFrom("-ERR_BAD_FORMAT invalid option\r\n"),
			expectedReply: DeleteReply{
				Ok:  false,
				Err: BadFormatErrorf("invalid option"),
			},
		},
		{
			name:   "successfully decode ERR_LIMITS error",
			reader: readerFrom("-ERR_LIMITS stream name too long\r\n"),
			expectedReply: DeleteReply{
				Ok:  false,
				Err: LimitsErrorf("stream name too long"),
			},
		},
		{
			name:            "fails with invalid simple string value",
			reader:          readerFrom("+INVALID\r\n"),
			expectedReply:   DeleteReply{},
			expectedErrCode: "",
		},
		{
			name:            "fails with invalid first byte",
			reader:          readerFrom("$5\r\nhello\r\n"),
			expectedReply:   DeleteReply{},
			expectedErrCode: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			var errCode ErrorCode
			reply, err := d.DecodeDeleteReply(test.reader)

			if err != nil {
				var protoErr Error
				if errors.As(err, &protoErr) {
					errCode = protoErr.Code
				} else {
					// Non-protocol error (e.g., invalid format)
					require.Equal(test.expectedErrCode, errCode)
					return
				}
			}

			// should be "" if there was no error
			require.Equal(test.expectedErrCode, errCode)

			require.Equal(test.expectedReply.Ok, reply.Ok)
			require.Equal(test.expectedReply.Err, reply.Err)
		})
	}

	t.Run("reader byte stream ends too early", func(t *testing.T) {
		require := require.New(t)
		reader := readerFrom("+OK\r")
		_, err := d.DecodeDeleteReply(reader)

		require.Error(err)
		require.Equal(io.EOF, err)
	})
}

func readerFrom(s string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(s))
}
