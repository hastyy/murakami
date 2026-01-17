package log_old

import (
	"crypto/rand"
	"testing"

	"github.com/hastyy/murakami/internal/unit"
)

func BenchmarkAppend(b *testing.B) {
	segment := NewTestActiveSegment("/tmp/murakami-test", 1_000)

	data := make([]byte, 512*unit.Byte)
	_, err := rand.Read(data)
	if err != nil {
		b.Fatal(err)
	}

	// buf := make([]byte, 2 * unit.KiB)

	b.ResetTimer()
	for b.Loop() {
		_, err := segment.Append(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogAppend(b *testing.B) {
	log, err := NewLog("/tmp/murakami-test", "test")
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 512*unit.Byte)
	_, err = rand.Read(data)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for b.Loop() {
		_, err := log.Append(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
