package logtree

import (
	"strconv"
	"testing"
)

func BenchmarkAppend(b *testing.B) {
	// Create a new LogTree with TimestampKeyGenerator
	tree := New[int]()
	keyGenerator := &mockKeyGenerator{ts: 1762862293315}
	records := make([]int, 100)

	// Reset the benchmark timer before the loop
	b.ResetTimer()

	// Run the benchmark
	for b.Loop() {
		tree.Append(keyGenerator.GetNext(), records)
	}
}

type mockKeyGenerator struct {
	ts int64
}

func (g *mockKeyGenerator) GetNext() Key {
	g.ts++
	return Key(strconv.FormatInt(g.ts, 10))
}
