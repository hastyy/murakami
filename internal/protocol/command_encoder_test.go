package protocol

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEncodeCreateCommand(t *testing.T) {
	encoder := NewCommandEncoder()

	tests := []struct {
		name           string
		cmd            CreateCommand
		expectedOutput string
	}{
		{
			name:           "successfully encodes CREATE with simple stream name",
			cmd:            CreateCommand{StreamName: "orders"},
			expectedOutput: "*3\r\n$6\r\nCREATE\r\n$6\r\norders\r\n*0\r\n",
		},
		{
			name:           "successfully encodes CREATE with single character stream name",
			cmd:            CreateCommand{StreamName: "a"},
			expectedOutput: "*3\r\n$6\r\nCREATE\r\n$1\r\na\r\n*0\r\n",
		},
		{
			name:           "successfully encodes CREATE with stream name containing spaces",
			cmd:            CreateCommand{StreamName: "my stream"},
			expectedOutput: "*3\r\n$6\r\nCREATE\r\n$9\r\nmy stream\r\n*0\r\n",
		},
		{
			name:           "successfully encodes CREATE with stream name containing special characters",
			cmd:            CreateCommand{StreamName: "stream-name_123"},
			expectedOutput: "*3\r\n$6\r\nCREATE\r\n$15\r\nstream-name_123\r\n*0\r\n",
		},
		{
			name:           "successfully encodes CREATE with stream name containing CRLF",
			cmd:            CreateCommand{StreamName: "line1\r\nline2"},
			expectedOutput: "*3\r\n$6\r\nCREATE\r\n$12\r\nline1\r\nline2\r\n*0\r\n",
		},
		{
			name:           "successfully encodes CREATE with long stream name",
			cmd:            CreateCommand{StreamName: "this-is-a-very-long-stream-name-that-contains-many-characters-to-test-encoding"},
			expectedOutput: "*3\r\n$6\r\nCREATE\r\n$78\r\nthis-is-a-very-long-stream-name-that-contains-many-characters-to-test-encoding\r\n*0\r\n",
		},
		{
			name:           "successfully encodes CREATE with stream name containing unicode",
			cmd:            CreateCommand{StreamName: "stream-日本語"},
			expectedOutput: "*3\r\n$6\r\nCREATE\r\n$16\r\nstream-日本語\r\n*0\r\n",
		},
		{
			name:           "successfully encodes CREATE with stream name containing symbols",
			cmd:            CreateCommand{StreamName: "stream!@#$%"},
			expectedOutput: "*3\r\n$6\r\nCREATE\r\n$11\r\nstream!@#$%\r\n*0\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)

			err := encoder.EncodeCreateCommand(writer, test.cmd)
			require.NoError(err)

			err = writer.Flush()
			require.NoError(err)

			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestEncodeCreateCommand_WriterError(t *testing.T) {
	require := require.New(t)
	encoder := NewCommandEncoder()

	// Create a writer with a very small buffer to force immediate writes
	failingWriter := &failingWriter{failAfter: 0}
	writer := bufio.NewWriterSize(failingWriter, 1)

	cmd := CreateCommand{StreamName: "test"}
	err := encoder.EncodeCreateCommand(writer, cmd)
	require.Error(err)
	require.Equal(io.ErrShortWrite, err)
}

func TestEncodeAppendCommand(t *testing.T) {
	encoder := NewCommandEncoder()

	tests := []struct {
		name           string
		cmd            AppendCommand
		expectedOutput string
	}{
		{
			name: "single record with no options",
			cmd: AppendCommand{
				StreamName: "stream1",
				Records:    [][]byte{[]byte("record1")},
			},
			expectedOutput: "*4\r\n$6\r\nAPPEND\r\n$7\r\nstream1\r\n*0\r\n*1\r\n$7\r\nrecord1\r\n",
		},
		{
			name: "multiple records with no options",
			cmd: AppendCommand{
				StreamName: "stream1",
				Records:    [][]byte{[]byte("record1"), []byte("record2"), []byte("record3")},
			},
			expectedOutput: "*4\r\n$6\r\nAPPEND\r\n$7\r\nstream1\r\n*0\r\n*3\r\n$7\r\nrecord1\r\n$7\r\nrecord2\r\n$7\r\nrecord3\r\n",
		},
		{
			name: "single record with ID option",
			cmd: AppendCommand{
				StreamName: "stream1",
				Records:    [][]byte{[]byte("record1")},
				Options: AppendCommandOptions{
					MillisID: "1234567890",
				},
			},
			expectedOutput: "*4\r\n$6\r\nAPPEND\r\n$7\r\nstream1\r\n*2\r\n$2\r\nID\r\n$10\r\n1234567890\r\n*1\r\n$7\r\nrecord1\r\n",
		},
		{
			name: "multiple records with ID option",
			cmd: AppendCommand{
				StreamName: "mystream",
				Records:    [][]byte{[]byte("data1"), []byte("data2")},
				Options: AppendCommandOptions{
					MillisID: "9999999999",
				},
			},
			expectedOutput: "*4\r\n$6\r\nAPPEND\r\n$8\r\nmystream\r\n*2\r\n$2\r\nID\r\n$10\r\n9999999999\r\n*2\r\n$5\r\ndata1\r\n$5\r\ndata2\r\n",
		},
		{
			name: "binary record data",
			cmd: AppendCommand{
				StreamName: "stream",
				Records:    [][]byte{{0x00, 0x01, 0x02, 0xFF}},
			},
			expectedOutput: "*4\r\n$6\r\nAPPEND\r\n$6\r\nstream\r\n*0\r\n*1\r\n$4\r\n\x00\x01\x02\xFF\r\n",
		},
		{
			name: "stream name with special characters",
			cmd: AppendCommand{
				StreamName: "my-stream_123",
				Records:    [][]byte{[]byte("test")},
			},
			expectedOutput: "*4\r\n$6\r\nAPPEND\r\n$13\r\nmy-stream_123\r\n*0\r\n*1\r\n$4\r\ntest\r\n",
		},
		{
			name: "empty record",
			cmd: AppendCommand{
				StreamName: "stream",
				Records:    [][]byte{{}},
			},
			expectedOutput: "*4\r\n$6\r\nAPPEND\r\n$6\r\nstream\r\n*0\r\n*1\r\n$0\r\n\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)

			err := encoder.EncodeAppendCommand(writer, test.cmd)
			require.NoError(err)

			err = writer.Flush()
			require.NoError(err)

			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestEncodeAppendCommand_WriterError(t *testing.T) {
	require := require.New(t)
	encoder := NewCommandEncoder()

	failingWriter := &failingWriter{failAfter: 0}
	writer := bufio.NewWriterSize(failingWriter, 1)

	cmd := AppendCommand{
		StreamName: "mystream",
		Records:    [][]byte{[]byte("record")},
	}

	err := encoder.EncodeAppendCommand(writer, cmd)
	require.Error(err)
	require.Equal(io.ErrShortWrite, err)
}

func TestEncodeReadCommand(t *testing.T) {
	encoder := NewCommandEncoder()

	tests := []struct {
		name           string
		cmd            ReadCommand
		expectedOutput string
	}{
		{
			name: "no options (all defaults)",
			cmd: ReadCommand{
				StreamName: "stream1",
				Options: ReadCommandOptions{
					Count: 0,
					Block: 0,
					MinID: "",
				},
			},
			expectedOutput: "*3\r\n$4\r\nREAD\r\n$7\r\nstream1\r\n*0\r\n",
		},
		{
			name: "with COUNT option",
			cmd: ReadCommand{
				StreamName: "stream1",
				Options: ReadCommandOptions{
					Count: 10,
					Block: 0,
					MinID: "",
				},
			},
			expectedOutput: "*3\r\n$4\r\nREAD\r\n$7\r\nstream1\r\n*2\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n",
		},
		{
			name: "with BLOCK option",
			cmd: ReadCommand{
				StreamName: "stream1",
				Options: ReadCommandOptions{
					Count: 0,
					Block: 5000 * time.Millisecond,
					MinID: "",
				},
			},
			expectedOutput: "*3\r\n$4\r\nREAD\r\n$7\r\nstream1\r\n*2\r\n$5\r\nBLOCK\r\n$4\r\n5000\r\n",
		},
		{
			name: "with MIN_ID option",
			cmd: ReadCommand{
				StreamName: "stream1",
				Options: ReadCommandOptions{
					Count: 0,
					Block: 0,
					MinID: "1234567890-5",
				},
			},
			expectedOutput: "*3\r\n$4\r\nREAD\r\n$7\r\nstream1\r\n*2\r\n$6\r\nMIN_ID\r\n$12\r\n1234567890-5\r\n",
		},
		{
			name: "with all options",
			cmd: ReadCommand{
				StreamName: "mystream",
				Options: ReadCommandOptions{
					Count: 25,
					Block: 10000 * time.Millisecond,
					MinID: "9999999999-10",
				},
			},
			expectedOutput: "*3\r\n$4\r\nREAD\r\n$8\r\nmystream\r\n*6\r\n$5\r\nCOUNT\r\n$2\r\n25\r\n$5\r\nBLOCK\r\n$5\r\n10000\r\n$6\r\nMIN_ID\r\n$13\r\n9999999999-10\r\n",
		},
		{
			name: "with COUNT and BLOCK",
			cmd: ReadCommand{
				StreamName: "test",
				Options: ReadCommandOptions{
					Count: 5,
					Block: 1000 * time.Millisecond,
					MinID: "",
				},
			},
			expectedOutput: "*3\r\n$4\r\nREAD\r\n$4\r\ntest\r\n*4\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n$5\r\nBLOCK\r\n$4\r\n1000\r\n",
		},
		{
			name: "with COUNT and MIN_ID",
			cmd: ReadCommand{
				StreamName: "test",
				Options: ReadCommandOptions{
					Count: 100,
					Block: 0,
					MinID: "1000-0",
				},
			},
			expectedOutput: "*3\r\n$4\r\nREAD\r\n$4\r\ntest\r\n*4\r\n$5\r\nCOUNT\r\n$3\r\n100\r\n$6\r\nMIN_ID\r\n$6\r\n1000-0\r\n",
		},
		{
			name: "with BLOCK and MIN_ID",
			cmd: ReadCommand{
				StreamName: "test",
				Options: ReadCommandOptions{
					Count: 0,
					Block: 500 * time.Millisecond,
					MinID: "2000-0",
				},
			},
			expectedOutput: "*3\r\n$4\r\nREAD\r\n$4\r\ntest\r\n*4\r\n$5\r\nBLOCK\r\n$3\r\n500\r\n$6\r\nMIN_ID\r\n$6\r\n2000-0\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)

			err := encoder.EncodeReadCommand(writer, test.cmd)
			require.NoError(err)

			err = writer.Flush()
			require.NoError(err)

			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestEncodeReadCommand_WriterError(t *testing.T) {
	require := require.New(t)
	encoder := NewCommandEncoder()

	failingWriter := &failingWriter{failAfter: 0}
	writer := bufio.NewWriterSize(failingWriter, 1)

	cmd := ReadCommand{
		StreamName: "mystream",
	}

	err := encoder.EncodeReadCommand(writer, cmd)
	require.Error(err)
	require.Equal(io.ErrShortWrite, err)
}

func TestEncodeTrimCommand(t *testing.T) {
	encoder := NewCommandEncoder()

	tests := []struct {
		name           string
		cmd            TrimCommand
		expectedOutput string
	}{
		{
			name: "basic trim with MIN_ID",
			cmd: TrimCommand{
				StreamName: "stream1",
				Options: TrimCommandOptions{
					MinID: "1234567890-5",
				},
			},
			expectedOutput: "*3\r\n$4\r\nTRIM\r\n$7\r\nstream1\r\n*2\r\n$6\r\nMIN_ID\r\n$12\r\n1234567890-5\r\n",
		},
		{
			name: "trim with simple MIN_ID",
			cmd: TrimCommand{
				StreamName: "mystream",
				Options: TrimCommandOptions{
					MinID: "1000-0",
				},
			},
			expectedOutput: "*3\r\n$4\r\nTRIM\r\n$8\r\nmystream\r\n*2\r\n$6\r\nMIN_ID\r\n$6\r\n1000-0\r\n",
		},
		{
			name: "trim with large MIN_ID",
			cmd: TrimCommand{
				StreamName: "test",
				Options: TrimCommandOptions{
					MinID: "9999999999999999-999",
				},
			},
			expectedOutput: "*3\r\n$4\r\nTRIM\r\n$4\r\ntest\r\n*2\r\n$6\r\nMIN_ID\r\n$20\r\n9999999999999999-999\r\n",
		},
		{
			name: "trim with stream name containing special characters",
			cmd: TrimCommand{
				StreamName: "my-stream_123",
				Options: TrimCommandOptions{
					MinID: "5000-10",
				},
			},
			expectedOutput: "*3\r\n$4\r\nTRIM\r\n$13\r\nmy-stream_123\r\n*2\r\n$6\r\nMIN_ID\r\n$7\r\n5000-10\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)

			err := encoder.EncodeTrimCommand(writer, test.cmd)
			require.NoError(err)

			err = writer.Flush()
			require.NoError(err)

			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestEncodeTrimCommand_WriterError(t *testing.T) {
	require := require.New(t)
	encoder := NewCommandEncoder()

	failingWriter := &failingWriter{failAfter: 0}
	writer := bufio.NewWriterSize(failingWriter, 1)

	cmd := TrimCommand{
		StreamName: "mystream",
		Options: TrimCommandOptions{
			MinID: "1000-0",
		},
	}

	err := encoder.EncodeTrimCommand(writer, cmd)
	require.Error(err)
	require.Equal(io.ErrShortWrite, err)
}

func TestEncodeDeleteCommand(t *testing.T) {
	encoder := NewCommandEncoder()

	tests := []struct {
		name           string
		cmd            DeleteCommand
		expectedOutput string
	}{
		{
			name: "simple stream name",
			cmd: DeleteCommand{
				StreamName: "stream1",
			},
			expectedOutput: "*3\r\n$6\r\nDELETE\r\n$7\r\nstream1\r\n*0\r\n",
		},
		{
			name: "single character stream name",
			cmd: DeleteCommand{
				StreamName: "s",
			},
			expectedOutput: "*3\r\n$6\r\nDELETE\r\n$1\r\ns\r\n*0\r\n",
		},
		{
			name: "stream name with spaces",
			cmd: DeleteCommand{
				StreamName: "my stream",
			},
			expectedOutput: "*3\r\n$6\r\nDELETE\r\n$9\r\nmy stream\r\n*0\r\n",
		},
		{
			name: "stream name with special characters",
			cmd: DeleteCommand{
				StreamName: "my-stream_123",
			},
			expectedOutput: "*3\r\n$6\r\nDELETE\r\n$13\r\nmy-stream_123\r\n*0\r\n",
		},
		{
			name: "stream name with unicode",
			cmd: DeleteCommand{
				StreamName: "流stream",
			},
			expectedOutput: "*3\r\n$6\r\nDELETE\r\n$9\r\n流stream\r\n*0\r\n",
		},
		{
			name: "long stream name",
			cmd: DeleteCommand{
				StreamName: "verylongstreamnamewithlotsofcharactersbutwithinlimits",
			},
			expectedOutput: "*3\r\n$6\r\nDELETE\r\n$53\r\nverylongstreamnamewithlotsofcharactersbutwithinlimits\r\n*0\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)

			err := encoder.EncodeDeleteCommand(writer, test.cmd)
			require.NoError(err)

			err = writer.Flush()
			require.NoError(err)

			require.Equal(test.expectedOutput, buf.String())
		})
	}
}

func TestEncodeDeleteCommand_WriterError(t *testing.T) {
	require := require.New(t)
	encoder := NewCommandEncoder()

	failingWriter := &failingWriter{failAfter: 0}
	writer := bufio.NewWriterSize(failingWriter, 1)

	cmd := DeleteCommand{
		StreamName: "mystream",
	}

	err := encoder.EncodeDeleteCommand(writer, cmd)
	require.Error(err)
	require.Equal(io.ErrShortWrite, err)
}
