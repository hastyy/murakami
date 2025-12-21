package integration

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hastyy/murakami/internal/controller"
	"github.com/hastyy/murakami/internal/protocol"
	"github.com/hastyy/murakami/internal/store"
	"github.com/hastyy/murakami/internal/tcp"
	"github.com/stretchr/testify/require"
)

// TestServerIntegration spawns a real server with all dependencies and tests
// the full request-response cycle for CREATE, APPEND, READ, and TRIM commands.
func TestServerIntegration(t *testing.T) {
	require := require.New(t)

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Setup store with key generator
	keyGen := store.NewTimestampKeyGenerator()
	store := store.NewInMemoryStreamStore(keyGen)

	// Setup protocol decoder and encoder
	bufPool := protocol.NewBufferPool()
	decoder := protocol.NewCommandDecoder(bufPool)
	encoder := protocol.NewReplyEncoder()

	// Setup controller (handler)
	handler := controller.New(store, decoder, encoder, logger.With("component", "controller"))

	// Setup TCP server with a specific test port
	connPool := tcp.NewConnectionPool()
	listen := func(address string) (net.Listener, error) {
		return net.Listen("tcp", address)
	}
	testServer := tcp.NewServer(connPool, listen, time.Sleep, tcp.ServerConfig{
		Address: "127.0.0.1:17500",
	})

	// Start server in background
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- testServer.Start(handler)
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Defer server shutdown
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := testServer.Stop(ctx)
		require.NoError(err)
	}()

	// Connect to the server
	conn, err := net.Dial("tcp", "127.0.0.1:17500")
	require.NoError(err)
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Test 1: CREATE stream with name "pizza"
	t.Log("Test 1: CREATE stream 'pizza'")
	err = sendCommand(writer, "*3\r\n$6\r\nCREATE\r\n$5\r\npizza\r\n*0\r\n")
	require.NoError(err)

	reply := readSimpleString(reader)
	require.Equal("+OK\r\n", reply)

	// Test 2: APPEND "pepperoni"
	t.Log("Test 2: APPEND 'pepperoni'")
	err = sendCommand(writer, "*4\r\n$6\r\nAPPEND\r\n$5\r\npizza\r\n*0\r\n*1\r\n$9\r\npepperoni\r\n")
	require.NoError(err)

	id1 := readBulkString(reader)
	require.True(protocol.IsValidID(id1), "Expected valid ID, got: %s", id1)
	t.Logf("  First record ID: %s", id1)

	// Test 3: APPEND "margarita", "carbonara"
	t.Log("Test 3: APPEND 'margarita', 'carbonara'")
	err = sendCommand(writer, "*4\r\n$6\r\nAPPEND\r\n$5\r\npizza\r\n*0\r\n*2\r\n$9\r\nmargarita\r\n$9\r\ncarbonara\r\n")
	require.NoError(err)

	id2 := readBulkString(reader)
	require.True(protocol.IsValidID(id2), "Expected valid ID, got: %s", id2)
	t.Logf("  Last record ID: %s", id2)

	// Test 4: READ whole stream
	t.Log("Test 4: READ whole stream")
	err = sendCommand(writer, "*3\r\n$4\r\nREAD\r\n$5\r\npizza\r\n*2\r\n$6\r\nMIN_ID\r\n$3\r\n0-0\r\n")
	require.NoError(err)

	records := readRecordArray(reader)
	require.Len(records, 3, "Expected 3 records")
	require.Equal("pepperoni", string(records[0].Value))
	require.Equal("margarita", string(records[1].Value))
	require.Equal("carbonara", string(records[2].Value))
	t.Logf("  Read %d records", len(records))
	for i, rec := range records {
		t.Logf("    [%d] ID=%s, Value=%s", i, rec.ID, string(rec.Value))
	}

	// Test 5: TRIM 1st entry (using the first record's ID)
	t.Log("Test 5: TRIM first entry")
	// To trim the first entry, we need to use the second record's ID
	// because TRIM deletes everything strictly less than the given ID
	secondRecordID := records[1].ID
	trimCmd := buildTrimCommand("pizza", secondRecordID)
	err = sendCommand(writer, trimCmd)
	require.NoError(err)

	reply = readSimpleString(reader)
	require.Equal("+OK\r\n", reply)

	// Test 6: READ whole stream again (should only have 2 records now)
	t.Log("Test 6: READ whole stream after TRIM")
	err = sendCommand(writer, "*3\r\n$4\r\nREAD\r\n$5\r\npizza\r\n*2\r\n$6\r\nMIN_ID\r\n$3\r\n0-0\r\n")
	require.NoError(err)

	records = readRecordArray(reader)
	require.Len(records, 2, "Expected 2 records after trim")
	require.Equal("margarita", string(records[0].Value))
	require.Equal("carbonara", string(records[1].Value))
	t.Logf("  Read %d records after trim", len(records))
	for i, rec := range records {
		t.Logf("    [%d] ID=%s, Value=%s", i, rec.ID, string(rec.Value))
	}
}

// Helper functions for reading protocol responses

func sendCommand(w *bufio.Writer, cmd string) error {
	_, err := w.WriteString(cmd)
	if err != nil {
		return err
	}
	return w.Flush()
}

func readSimpleString(r *bufio.Reader) string {
	line, err := r.ReadString('\n')
	if err != nil {
		panic(err)
	}
	return line
}

func readBulkString(r *bufio.Reader) string {
	// Read '$'
	b, err := r.ReadByte()
	if err != nil {
		panic(err)
	}
	if b != '$' {
		panic("expected bulk string")
	}

	// Read length line
	lengthLine, err := r.ReadString('\n')
	if err != nil {
		panic(err)
	}
	lengthLine = strings.TrimSpace(lengthLine)

	var length int
	_, err = io.ReadAtLeast(r, make([]byte, 0), 0) // no-op to check error
	if err == nil {
		// Parse length
		n := 0
		for _, ch := range lengthLine {
			if ch >= '0' && ch <= '9' {
				n = n*10 + int(ch-'0')
			}
		}
		length = n
	}

	// Read the actual string content
	buf := make([]byte, length)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		panic(err)
	}

	// Read trailing CRLF
	r.ReadByte() // \r
	r.ReadByte() // \n

	return string(buf)
}

type Record struct {
	ID    string
	Value []byte
}

func readRecordArray(r *bufio.Reader) []Record {
	// Read '*'
	b, err := r.ReadByte()
	if err != nil {
		panic(err)
	}
	if b != '*' {
		panic("expected array")
	}

	// Read array length
	lengthLine, err := r.ReadString('\n')
	if err != nil {
		panic(err)
	}
	lengthLine = strings.TrimSpace(lengthLine)

	arrayLength := 0
	for _, ch := range lengthLine {
		if ch >= '0' && ch <= '9' {
			arrayLength = arrayLength*10 + int(ch-'0')
		}
	}

	// Array contains pairs of [id, value], so number of records is arrayLength/2
	numRecords := arrayLength / 2
	records := make([]Record, numRecords)

	for i := 0; i < numRecords; i++ {
		// Read ID (bulk string)
		id := readBulkString(r)
		// Read Value (bulk string)
		value := readBulkString(r)

		records[i] = Record{
			ID:    id,
			Value: []byte(value),
		}
	}

	return records
}

func buildTrimCommand(streamName, minID string) string {
	var sb strings.Builder

	// Array with 3 elements
	sb.WriteString("*3\r\n")

	// Command: TRIM
	sb.WriteString("$4\r\n")
	sb.WriteString("TRIM\r\n")

	// Stream name
	sb.WriteString("$")
	sb.WriteString(itoa(len(streamName)))
	sb.WriteString("\r\n")
	sb.WriteString(streamName)
	sb.WriteString("\r\n")

	// Options array (2 elements: MIN_ID key and value)
	sb.WriteString("*2\r\n")
	sb.WriteString("$6\r\n")
	sb.WriteString("MIN_ID\r\n")
	sb.WriteString("$")
	sb.WriteString(itoa(len(minID)))
	sb.WriteString("\r\n")
	sb.WriteString(minID)
	sb.WriteString("\r\n")

	return sb.String()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte
	i := len(buf) - 1
	for n > 0 {
		buf[i] = byte('0' + n%10)
		n /= 10
		i--
	}

	return string(buf[i+1:])
}
