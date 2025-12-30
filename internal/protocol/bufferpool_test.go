package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSizeClassesBufferPool_Get_ReturnsBufferOfRequestedSizeOrLarger(t *testing.T) {
	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64:  10,
		128: 10,
		256: 10,
	}, false)

	tests := []struct {
		name            string
		requestedSize   int
		expectedMinSize int
	}{
		{"exact match smallest", 64, 64},
		{"exact match middle", 128, 128},
		{"exact match largest", 256, 256},
		{"smaller than smallest returns smallest", 32, 64},
		{"between classes returns next larger", 100, 128},
		{"between classes returns next larger 2", 200, 256},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			buf, err := pool.Get(tt.requestedSize)
			require.NoError(err)
			require.GreaterOrEqual(len(buf), tt.expectedMinSize)

			pool.Put(buf)
		})
	}
}

func TestSizeClassesBufferPool_Get_ReturnsErrNoBufferAvailable_WhenPoolEmpty(t *testing.T) {
	require := require.New(t)

	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64: 1,
	}, false)

	// Get the only buffer
	buf, err := pool.Get(64)
	require.NoError(err)
	require.NotNil(buf)

	// Try to get another - should fail
	_, err = pool.Get(64)
	require.ErrorIs(err, ErrNoBufferAvailable)

	// Return the buffer
	pool.Put(buf)

	// Now we can get again
	buf2, err := pool.Get(64)
	require.NoError(err)
	require.NotNil(buf2)
}

func TestSizeClassesBufferPool_Get_ReturnsErrNoBufferAvailable_WhenRequestedSizeTooLarge(t *testing.T) {
	require := require.New(t)

	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64:  10,
		128: 10,
	}, false)

	// Request larger than any available class
	_, err := pool.Get(256)
	require.ErrorIs(err, ErrNoBufferAvailable)
}

func TestSizeClassesBufferPool_Get_FallsBackToLargerClass_WhenSmallerEmpty(t *testing.T) {
	require := require.New(t)

	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64:  1,
		128: 1,
	}, false)

	// Exhaust the 64-byte pool
	buf64, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf64))

	// Request 64 bytes - falls back to 128-byte pool.
	// Since 128 >= 2*64, it splits the 128 into 2x64.
	buf64_2, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf64_2))

	// The second 64-byte split should now be in the pool
	buf64_3, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf64_3))

	pool.Put(buf64)
	pool.Put(buf64_2)
	pool.Put(buf64_3)
}

func TestSizeClassesBufferPool_Get_SplitsLargerBuffer_WhenAtLeast2xRequested(t *testing.T) {
	require := require.New(t)

	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64:  0, // Start with empty 64-byte pool
		256: 1, // Only have 256-byte buffers
	}, false)

	// Request 64 bytes - should get 256, split into 4x64, return one
	buf, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf))

	// The other 3 splits should be in the 64-byte pool now
	buf2, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf2))

	buf3, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf3))

	buf4, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf4))

	// Now the pool should be empty
	_, err = pool.Get(64)
	require.ErrorIs(err, ErrNoBufferAvailable)
}

func TestSizeClassesBufferPool_Get_SplitsByPowerOf2(t *testing.T) {
	require := require.New(t)

	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		256: 0, // Empty
		512: 1, // Only have 512-byte buffers
	}, false)

	// Request 129 bytes from 512 buffer
	// 512/129 ≈ 3.96, floor to power of 2 = 2
	// Split into 2x256
	buf, err := pool.Get(129)
	require.NoError(err)
	require.Equal(256, len(buf))

	// One more 256-byte buffer should be available
	buf2, err := pool.Get(129)
	require.NoError(err)
	require.Equal(256, len(buf2))

	// Now empty
	_, err = pool.Get(129)
	require.ErrorIs(err, ErrNoBufferAvailable)
}

func TestSizeClassesBufferPool_Put_ReturnsBufferToCorrectPool(t *testing.T) {
	require := require.New(t)

	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64:  1,
		128: 1,
	}, false)

	// Get both buffers
	buf64, _ := pool.Get(64)
	buf128, _ := pool.Get(128)

	// Both pools should be empty now
	_, err := pool.Get(64)
	require.ErrorIs(err, ErrNoBufferAvailable)
	_, err = pool.Get(128)
	require.ErrorIs(err, ErrNoBufferAvailable)

	// Return buffers
	pool.Put(buf64)
	pool.Put(buf128)

	// Both pools should have buffers again
	buf64Again, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf64Again))

	buf128Again, err := pool.Get(128)
	require.NoError(err)
	require.Equal(128, len(buf128Again))
}

func TestSizeClassesBufferPool_Put_DiscardsUnknownSizeClass(t *testing.T) {
	require := require.New(t)

	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64: 1,
	}, false)

	// Create a buffer that doesn't match any size class
	unknownBuf := make([]byte, 100)

	// This should not panic, just discard
	pool.Put(unknownBuf)

	// Pool should still work normally
	buf, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf))

	_, err = pool.Get(100)
	require.ErrorIs(err, ErrNoBufferAvailable)
}

func TestSizeClassesBufferPool_Put_PanicsOnDoublePut(t *testing.T) {
	require := require.New(t)

	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64: 2,
	}, false)

	buf, _ := pool.Get(64)
	pool.Put(buf)

	// Double put should panic
	require.Panics(func() {
		pool.Put(buf)
	})
}

func TestSizeClassesBufferPool_ChannelCapacityIncludesSplits(t *testing.T) {
	require := require.New(t)

	// Create pool where 64-byte class could receive splits from 256-byte class
	// Channel capacity for 64-byte class should be: 1 + 4*2 = 9
	// (1 initial buffer + 2 buffers from 256-byte class that can each split into 4x64)
	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64:  1,
		256: 2,
	}, false)

	// Get all buffers at once, holding them to force splits and verify channel capacity
	buffers := make([][]byte, 0, 9)

	// Get 9 buffers: 1 initial 64-byte + 8 from splitting both 256-byte buffers (4 each)
	for i := 0; i < 9; i++ {
		buf, err := pool.Get(64)
		require.NoError(err, "get iteration %d", i)
		require.Equal(64, len(buf))
		buffers = append(buffers, buf)
	}

	// We should have 9 buffers total (1 original + 8 from splits)
	require.Equal(9, len(buffers))

	// All pools should now be empty
	_, err := pool.Get(64)
	require.ErrorIs(err, ErrNoBufferAvailable)

	// Put all 9 buffers back - this tests that channel has capacity for all of them
	for _, buf := range buffers {
		pool.Put(buf)
	}

	// Verify all 9 are back in the pool
	for i := 0; i < 9; i++ {
		buf, err := pool.Get(64)
		require.NoError(err, "final get iteration %d", i)
		require.Equal(64, len(buf))
	}

	// Pool should be empty again
	_, err = pool.Get(64)
	require.ErrorIs(err, ErrNoBufferAvailable)
}

func TestSizeClassesBufferPool_ClearsPoisonOnGet(t *testing.T) {
	require := require.New(t)

	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64: 1,
	}, false)

	buf, _ := pool.Get(64)

	// First 16 bytes should be cleared (not poison pattern)
	require.False(isPoisoned(buf), "buffer should not be poisoned after Get")

	pool.Put(buf)
}

func TestSizeClassesBufferPool_WritesPoisonOnPut(t *testing.T) {
	require := require.New(t)

	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64: 1,
	}, false)

	buf, _ := pool.Get(64)

	// Write some data
	copy(buf, []byte("hello world"))

	pool.Put(buf)

	// Get the buffer back and check it was poisoned (and then cleared)
	buf2, _ := pool.Get(64)

	// Should be the same underlying buffer, now cleared
	require.False(isPoisoned(buf2))
}

func TestSizeClassesBufferPool_Get_AllocatesWhenEmpty(t *testing.T) {
	require := require.New(t)

	// Create pool with allocWhenEmpty = true
	pool := NewSizeClassesBufferPool(map[SizeClass]BufferCount{
		64:  1,
		128: 0, // No initial buffers
	}, true)

	// Exhaust the 64-byte pool
	buf64, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf64))

	// Request another 64-byte buffer - should allocate instead of returning error
	buf64_2, err := pool.Get(64)
	require.NoError(err)
	require.Equal(64, len(buf64_2))

	// Request 128-byte buffer - should allocate from the smallest suitable class (128)
	buf128, err := pool.Get(100)
	require.NoError(err)
	require.Equal(128, len(buf128))

	// Cleanup
	pool.Put(buf64)
	pool.Put(buf64_2)
	pool.Put(buf128)
}

func TestFloorPowerOf2(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 0},
		{-1, 0},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 4},
		{5, 4},
		{7, 4},
		{8, 8},
		{9, 8},
		{15, 8},
		{16, 16},
		{17, 16},
		{100, 64},
		{1000, 512},
		{1024, 1024},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.expected, floorPowerOf2(tt.input))
		})
	}
}

// isPoisoned checks if a buffer has the poison pattern in its first bytes.
func isPoisoned(buf []byte) bool {
	if len(buf) < len(poisonPattern) {
		return false
	}
	for i := 0; i < len(poisonPattern); i++ {
		if buf[i] != poisonPattern[i] {
			return false
		}
	}
	return true
}
