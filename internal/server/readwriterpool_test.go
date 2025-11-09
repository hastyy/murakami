package server

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hastyy/murakami/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewConcurrentConnectionReadWriterPool_PanicsOnInvalidPoolSize(t *testing.T) {
	testutil.AssertPanics(t, func() {
		NewConcurrentConnectionReadWriterPool(0, 1024)
	})

	testutil.AssertPanics(t, func() {
		NewConcurrentConnectionReadWriterPool(-1, 1024)
	})
}

func TestNewConcurrentConnectionReadWriterPool_PanicsOnInvalidBufferSize(t *testing.T) {
	testutil.AssertPanics(t, func() {
		NewConcurrentConnectionReadWriterPool(10, 0)
	})

	testutil.AssertPanics(t, func() {
		NewConcurrentConnectionReadWriterPool(10, -1)
	})
}

func TestNewConcurrentConnectionReadWriterPool_CreatesPoolWithCorrectSize(t *testing.T) {
	poolSize := 5
	bufferSize := 1024

	pool := NewConcurrentConnectionReadWriterPool(poolSize, bufferSize)
	require.NotNil(t, pool)
	require.NotNil(t, pool.pool)

	// Verify we can get exactly poolSize items without blocking
	items := make([]*ConnectionReadWriter, poolSize)
	for i := range poolSize {
		items[i] = pool.Get()
		require.NotNil(t, items[i])
	}

	// Put them all back
	for i := range poolSize {
		pool.Put(items[i])
	}
}

func TestConcurrentConnectionReadWriterPool_Get_ReturnsValidConnectionReadWriter(t *testing.T) {
	pool := NewConcurrentConnectionReadWriterPool(1, 1024)

	rw := pool.Get()
	require.NotNil(t, rw)

	// Test that the returned ConnectionReadWriter is functional
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	rw.Attach(serverConn)
	rw.ResetLimits()

	// Write data from client
	testData := []byte("test")
	go func() {
		_, _ = clientConn.Write(testData)
	}()

	// Read using the pooled ConnectionReadWriter
	buf := make([]byte, len(testData))
	n, err := rw.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)
	require.Equal(t, testData, buf)
}

func TestConcurrentConnectionReadWriterPool_Get_BlocksWhenPoolEmpty(t *testing.T) {
	pool := NewConcurrentConnectionReadWriterPool(1, 1024)

	// Get the only item in the pool
	rw := pool.Get()
	require.NotNil(t, rw)

	// Try to get another item - should block
	getCompleted := make(chan struct{})
	var rw2 *ConnectionReadWriter
	go func() {
		rw2 = pool.Get()
		close(getCompleted)
	}()

	// Give it a moment to ensure Get() is actually blocking
	select {
	case <-getCompleted:
		t.Fatal("Get() should have blocked but returned immediately")
	case <-time.After(50 * time.Millisecond):
		// Good, it's blocking as expected
	}

	// Put the first item back
	pool.Put(rw)

	// Now the Get() should complete
	select {
	case <-getCompleted:
		require.NotNil(t, rw2)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Get() should have completed after Put() but timed out")
	}
}

func TestConcurrentConnectionReadWriterPool_Put_AllowsReuse(t *testing.T) {
	pool := NewConcurrentConnectionReadWriterPool(2, 1024)

	// Get two items
	rw1 := pool.Get()
	rw2 := pool.Get()

	require.NotNil(t, rw1)
	require.NotNil(t, rw2)

	// Put them back
	pool.Put(rw1)
	pool.Put(rw2)

	// Get them again (should get the same instances)
	rw3 := pool.Get()
	rw4 := pool.Get()

	require.NotNil(t, rw3)
	require.NotNil(t, rw4)

	// At least one should be reused (might be either one due to channel ordering)
	require.True(t, rw3 == rw1 || rw3 == rw2 || rw4 == rw1 || rw4 == rw2,
		"Expected at least one ConnectionReadWriter to be reused from pool")
}

func TestConcurrentConnectionReadWriterPool_ConcurrentAccess(t *testing.T) {
	poolSize := 10
	numGoroutines := 50
	iterationsPerGoroutine := 100

	pool := NewConcurrentConnectionReadWriterPool(poolSize, 1024)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch multiple goroutines that Get/Put from the pool
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range iterationsPerGoroutine {
				rw := pool.Get()
				require.NotNil(t, rw)
				// Simulate some work
				time.Sleep(1 * time.Microsecond)
				pool.Put(rw)
			}
		}()
	}

	// Wait for all goroutines to complete
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("Concurrent access test timed out")
	}
}

func TestConcurrentConnectionReadWriterPool_PutDoesNotBlockWhenPoolNotFull(t *testing.T) {
	pool := NewConcurrentConnectionReadWriterPool(2, 1024)

	rw := pool.Get()
	require.NotNil(t, rw)

	// Put should not block since pool is not full
	done := make(chan struct{})
	go func() {
		pool.Put(rw)
		close(done)
	}()

	select {
	case <-done:
		// Success - Put completed without blocking
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Put() should not block when pool is not full")
	}
}

func TestConcurrentConnectionReadWriterPool_MultipleGetPutCycles(t *testing.T) {
	poolSize := 3
	pool := NewConcurrentConnectionReadWriterPool(poolSize, 512)

	// Perform multiple get/put cycles
	for cycle := 0; cycle < 10; cycle++ {
		items := make([]*ConnectionReadWriter, poolSize)

		// Get all items
		for i := 0; i < poolSize; i++ {
			items[i] = pool.Get()
			require.NotNil(t, items[i])
		}

		// Put all items back
		for i := 0; i < poolSize; i++ {
			pool.Put(items[i])
		}
	}

	// Verify pool is still functional
	rw := pool.Get()
	require.NotNil(t, rw)
	pool.Put(rw)
}
