package protocol

import (
	"bytes"
	"errors"
	"slices"

	"github.com/hastyy/murakami/internal/assert"
	"github.com/hastyy/murakami/internal/unit"
)

// SizeClass represents a buffer size in bytes.
type SizeClass = int

// BufferCount represents the number of buffers to pre-allocate.
type BufferCount = int

var (
	// ErrNoBufferAvailable is returned when no buffer is available in any pool.
	ErrNoBufferAvailable = errors.New("no buffer available")

	// poisonPattern is written to the first bytes of a buffer when it's in the pool.
	// Used to detect double-Put errors.
	poisonPattern = []byte{
		0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
		0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
	}
)

// DefaultSizeClassConfig is the default size class configuration for SizeClassesBufferPool.
// Total memory budget: ~4 GiB for buffers, plus ~733 MiB for channel overhead.
// Each larger class has double the buffer count of the previous smaller class.
// This "inverted" distribution favors larger buffers, which is intentional:
// larger buffers can be split into smaller ones to replenish depleted smaller classes,
// but smaller buffers cannot be combined into larger ones.
//
// Derivation: With sizes 256, 512, 1K, 2K, 4K and counts x, 2x, 4x, 8x, 16x,
// total memory = 256x + 1024x + 4096x + 16384x + 65536x = 87,296x = 4 GiB, so x ≈ 49,200.
//
// Pool channel capacities are sized to hold the initial buffers plus all possible
// buffers that could be created by splitting larger classes. For example, the 256-byte
// pool can hold its initial 49,200 buffers plus up to 16,728,000 buffers from splits.
var DefaultSizeClassConfig = map[SizeClass]BufferCount{
	256 * unit.Byte: 49_200,
	512 * unit.Byte: 98_400,
	1 * unit.KiB:    196_800,
	2 * unit.KiB:    393_600,
	4 * unit.KiB:    787_200,
}

// BufferPool is a simple single-class buffer pool that wraps SizeClassesBufferPool.
// It uses a fixed 4 KiB buffer size, which is suitable for most workloads.
// Total memory budget: ~4 GiB (1,048,576 buffers × 4 KiB).
type BufferPool struct {
	pool *SizeClassesBufferPool
}

// NewBufferPool creates a new BufferPool with a single 4 KiB size class.
// Pre-allocates 1,048,576 buffers for a total of ~4 GiB memory.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: NewSizeClassesBufferPool(map[SizeClass]BufferCount{
			4 * unit.KiB: 1_048_576,
		}, false),
	}
}

// Get retrieves a buffer from the pool with at least the requested size.
// Returns ErrNoBufferAvailable if no buffer is available.
func (p *BufferPool) Get(requestedSize int) ([]byte, error) {
	return p.pool.Get(requestedSize)
}

// Put returns a buffer to the pool for reuse.
func (p *BufferPool) Put(buf []byte) {
	p.pool.Put(buf)
}

// SizeClassesBufferPool is a multi-class buffer pool for efficient reuse of byte slices.
// It maintains separate pools for different buffer size classes, allowing callers
// to request buffers of specific sizes and receive appropriately-sized buffers.
//
// Each size class has its own channel-based pool. Get() attempts non-blocking
// receives starting from the smallest suitable class; if empty, it tries larger
// classes and splits buffers down. Put() uses a poison pattern to detect double-Put
// errors and discards buffers when a pool is full.
type SizeClassesBufferPool struct {
	sizeClasses    []SizeClass               // sorted sizes for lookup
	pools          map[SizeClass]chan []byte // size -> pool channel
	allocWhenEmpty bool                      // allocate new buffer when pool is empty
}

// NewSizeClassesBufferPool creates a new SizeClassesBufferPool with the specified size class configuration.
// The config maps buffer sizes to the number of buffers to pre-allocate for each class.
// Classes are sorted internally; no particular order is expected in the input.
//
// If allocWhenEmpty is true, Get() allocates a new buffer of the closest size class when
// all pools are empty, instead of returning ErrNoBufferAvailable.
//
// Pool channel capacities are automatically sized to accommodate both the initial
// buffers and any additional buffers that may be created by splitting larger classes.
func NewSizeClassesBufferPool(sizeClassConfig map[SizeClass]BufferCount, allocWhenEmpty bool) *SizeClassesBufferPool {
	// Extract and sort size classes
	sizeClasses := make([]SizeClass, 0, len(sizeClassConfig))
	for sizeClass := range sizeClassConfig {
		sizeClasses = append(sizeClasses, sizeClass)
	}
	slices.Sort(sizeClasses)

	// Create and populate pools
	pools := make(map[SizeClass]chan []byte, len(sizeClasses))
	for i, sizeClass := range sizeClasses {
		bufferCount := sizeClassConfig[sizeClass]

		// Calculate channel capacity: initial count + potential splits from larger classes
		channelCapacity := bufferCount
		for j := i + 1; j < len(sizeClasses); j++ {
			largerClass := sizeClasses[j]
			largerCount := sizeClassConfig[largerClass]
			splitFactor := largerClass / sizeClass
			channelCapacity += splitFactor * largerCount
		}

		pool := make(chan []byte, channelCapacity)
		for range bufferCount {
			buf := make([]byte, sizeClass)
			// Mark as poisoned (in pool)
			if len(buf) >= len(poisonPattern) {
				copy(buf[:len(poisonPattern)], poisonPattern)
			}
			pool <- buf
		}
		pools[sizeClass] = pool
	}

	return &SizeClassesBufferPool{
		sizeClasses:    sizeClasses,
		pools:          pools,
		allocWhenEmpty: allocWhenEmpty,
	}
}

// Get retrieves a buffer from the pool with at least the requested size.
// It finds the smallest size class that can accommodate requestedSize and attempts
// a non-blocking receive. If that class is empty, it tries larger classes.
//
// When a buffer from a larger class is obtained and is at least 2x the requested size,
// it is split based on power-of-2 division: one part is returned, the rest are Put
// back to the appropriate pool based on their size.
//
// Returns ErrNoBufferAvailable if all suitable pools are empty (unless allocWhenEmpty is true).
func (p *SizeClassesBufferPool) Get(requestedSize int) ([]byte, error) {
	// Track smallest suitable class for potential allocation
	var smallestSuitableClass SizeClass = -1

	// Linear search through sorted classes to find smallest class >= requestedSize.
	// Intentionally simple - we expect a small number of classes (5-10).
	for classIdx, classSize := range p.sizeClasses {
		if classSize < requestedSize {
			continue
		}

		// Remember the first (smallest) suitable class
		if smallestSuitableClass == -1 {
			smallestSuitableClass = classSize
		}

		select {
		case buf := <-p.pools[classSize]:
			// Clear poison pattern
			if len(buf) >= len(poisonPattern) {
				clear(buf[:len(poisonPattern)])
			}

			// If not at first class and buffer is at least 2x requested, split
			if classIdx > 0 && len(buf) >= 2*requestedSize {
				return p.splitAndReturn(buf, requestedSize), nil
			}
			return buf, nil
		default:
			// Pool empty, try next larger class
			continue
		}
	}

	// All pools empty - allocate if configured to do so
	if p.allocWhenEmpty && smallestSuitableClass > 0 {
		return make([]byte, smallestSuitableClass), nil
	}

	return nil, ErrNoBufferAvailable
}

// splitAndReturn splits a buffer based on power-of-2 division of buffer length
// by requested size. Returns the first split and Puts the rest back to the pool.
//
// Example: requestedSize=129, buf length=512
// - 512/129 ≈ 3.96, floor to power of 2 = 2
// - Split into 2 parts of 256 bytes each
// - Return first 256-byte part, Put second 256-byte part to 256 pool
func (p *SizeClassesBufferPool) splitAndReturn(buf []byte, requestedSize int) []byte {
	bufLen := len(buf)

	// Determine split count as largest power of 2 <= bufLen/requestedSize
	splitCount := floorPowerOf2(bufLen / requestedSize)
	splitSize := bufLen / splitCount

	// Return first split
	result := buf[:splitSize]

	// Put remaining splits (they'll go to matching pool or be discarded)
	for i := 1; i < splitCount; i++ {
		start := i * splitSize
		split := buf[start : start+splitSize]
		p.Put(split)
	}

	return result
}

// floorPowerOf2 returns the largest power of 2 less than or equal to n.
// Returns 0 if n <= 0.
func floorPowerOf2(n int) int {
	if n <= 0 {
		return 0
	}
	power := 1
	for power*2 <= n {
		power *= 2
	}
	return power
}

// Put returns a buffer to the appropriate pool based on its length.
// Uses a poison pattern to detect double-Put errors.
// If the pool is full or the buffer's length doesn't match any size class,
// the buffer is discarded.
//
// Callers should return buffers with their original length intact.
func (p *SizeClassesBufferPool) Put(buf []byte) {
	size := len(buf)
	pool, ok := p.pools[size]
	if !ok {
		// Unknown size class - discard
		return
	}

	// Check for double-Put using poison pattern
	assert.OK(len(buf) >= len(poisonPattern), "buffer too small for poison pattern check")
	assert.OK(!bytes.Equal(buf[:len(poisonPattern)], poisonPattern), "bufferpool: double Put detected - buffer already in pool")

	// Write poison pattern
	copy(buf[:len(poisonPattern)], poisonPattern)

	select {
	case pool <- buf:
	default:
		// TODO: Add warning log here - pool is full, discarding buffer.
		// This should be rare now that channel capacity accounts for splits,
		// but could indicate a buffer leak (Get without Put).
	}
}
