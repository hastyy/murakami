package logtree

import (
	"testing"
)

const baseKey = "00000000000000000000"

var insertValue = []int{10}

func BenchmarkInsert(b *testing.B) {
	tree := New[int]()
	keyBuf := []byte(baseKey)

	for i := 0; b.Loop(); i++ {
		// Convert iteration number to string in-place (right to left)
		// This is faster than fmt.Sprintf as it avoids allocations
		num := i
		pos := len(keyBuf) - 1
		for num > 0 && pos >= 0 {
			keyBuf[pos] = byte('0' + (num % 10))
			num /= 10
			pos--
		}
		// Reset any remaining positions to '0' (shouldn't be needed if i < 10^20)
		for pos >= 0 {
			keyBuf[pos] = '0'
			pos--
		}
		tree.Append(Key(keyBuf), insertValue)
	}
}
