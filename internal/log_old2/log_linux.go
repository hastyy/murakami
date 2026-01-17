//go:build linux

package log_old2

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func preallocateDiskSpacePlatform(file *os.File, size int64) error {
	// Use Linux fallocate for true preallocation
	return unix.Fallocate(int(file.Fd()), 0, 0, size)
}

// adviseMemoryAccessPatterns provides OS hints about memory access patterns for better performance.
// This is a Linux-specific optimization using madvise.
func adviseMemoryAccessPatterns(logData, indexData []byte) {
	if logData != nil {
		// Log file: sequential reads (streaming)
		_ = syscall.Madvise(logData, syscall.MADV_SEQUENTIAL)
	}

	if indexData != nil {
		// Index file: random access (binary search)
		_ = syscall.Madvise(indexData, syscall.MADV_RANDOM)
	}
}
