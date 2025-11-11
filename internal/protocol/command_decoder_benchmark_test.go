package protocol

import (
	"strings"
	"testing"
)

func BenchmarkDecodeNextCommand(b *testing.B) {
	pool := &benchmarkBufferPool{}
	d := NewCommandDecoder(Config{
		BufPool: pool,
	})

	// Keep the command string constant
	command := "*3\r\n$6\r\nCREATE\r\n$6\r\nstream\r\n*0\r\n"
	reader := readerFrom(command)

	b.ResetTimer()

	for b.Loop() {
		// Reset the reader to the beginning for each iteration - adds some overhead and memory allocations
		reader.Reset(strings.NewReader(command))

		_, err := d.DecodeNextCommand(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeCreateCommand(b *testing.B) {
	pool := &benchmarkBufferPool{}
	d := NewCommandDecoder(Config{
		BufPool: pool,
	})

	// CREATE command with TIMESTAMP_STRATEGY option
	command := "$6\r\nstream\r\n*2\r\n$18\r\nTIMESTAMP_STRATEGY\r\n$6\r\nclient\r\n"
	reader := readerFrom(command)

	b.ResetTimer()

	for b.Loop() {
		// Reset the reader to the beginning for each iteration - adds some overhead and memory allocations
		reader.Reset(strings.NewReader(command))

		_, err := d.DecodeCreateCommand(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeAppendCommand(b *testing.B) {
	pool := &benchmarkBufferPool{}
	d := NewCommandDecoder(Config{
		BufPool: pool,
	})

	// APPEND command with TIMESTAMP option and 8 records of 64 bytes each (512 bytes total)
	// Build the command string
	var sb strings.Builder

	// Stream name
	sb.WriteString("$6\r\nstream\r\n")

	// Options array with TIMESTAMP
	sb.WriteString("*2\r\n$9\r\nTIMESTAMP\r\n$15\r\n1700000001234-0\r\n")

	// Records array with 8 records
	sb.WriteString("*8\r\n")
	record := strings.Repeat("x", 64) // 64-byte record
	for range 8 {
		sb.WriteString("$64\r\n")
		sb.WriteString(record)
		sb.WriteString("\r\n")
	}

	command := sb.String()
	reader := readerFrom(command)

	b.ResetTimer()

	for b.Loop() {
		// Reset the reader to the beginning for each iteration - adds some overhead and memory allocations
		reader.Reset(strings.NewReader(command))

		_, err := d.DecodeAppendCommand(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeReadCommand(b *testing.B) {
	pool := &benchmarkBufferPool{}
	d := NewCommandDecoder(Config{
		BufPool: pool,
	})

	// READ command with all options: COUNT, BLOCK, MIN_TIMESTAMP
	var sb strings.Builder

	// Stream name
	sb.WriteString("$6\r\nstream\r\n")

	// Options array with all available options
	sb.WriteString("*6\r\n")
	sb.WriteString("$5\r\nCOUNT\r\n$3\r\n100\r\n")
	sb.WriteString("$5\r\nBLOCK\r\n$4\r\n5000\r\n")
	sb.WriteString("$13\r\nMIN_TIMESTAMP\r\n$15\r\n1700000001234-0\r\n")

	command := sb.String()
	reader := readerFrom(command)

	b.ResetTimer()

	for b.Loop() {
		// Reset the reader to the beginning for each iteration - adds some overhead and memory allocations
		reader.Reset(strings.NewReader(command))

		_, err := d.DecodeReadCommand(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeTrimCommand(b *testing.B) {
	pool := &benchmarkBufferPool{}
	d := NewCommandDecoder(Config{
		BufPool: pool,
	})

	// TRIM command with UNTIL option
	var sb strings.Builder

	// Stream name
	sb.WriteString("$6\r\nstream\r\n")

	// Options array with UNTIL
	sb.WriteString("*2\r\n$5\r\nUNTIL\r\n$15\r\n1700000001234-0\r\n")

	command := sb.String()
	reader := readerFrom(command)

	b.ResetTimer()

	for b.Loop() {
		// Reset the reader to the beginning for each iteration - adds some overhead and memory allocations
		reader.Reset(strings.NewReader(command))

		_, err := d.DecodeTrimCommand(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeDeleteCommand(b *testing.B) {
	pool := &benchmarkBufferPool{}
	d := NewCommandDecoder(Config{
		BufPool: pool,
	})

	// DELETE command with empty options array
	command := "$6\r\nstream\r\n*0\r\n"
	reader := readerFrom(command)

	b.ResetTimer()

	for b.Loop() {
		// Reset the reader to the beginning for each iteration - adds some overhead and memory allocations
		reader.Reset(strings.NewReader(command))

		_, err := d.DecodeDeleteCommand(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

var buf = make([]byte, 1024)

type benchmarkBufferPool struct{}

func (b *benchmarkBufferPool) Get() []byte {
	return buf
}

func (b *benchmarkBufferPool) Put(buf []byte) {
	// do nothing
}
