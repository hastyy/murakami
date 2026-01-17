//go:build !linux

package log_old2

import "os"

func preallocateDiskSpacePlatform(_ *os.File, _ int64) error {
	// Not supported on this platform, return error to trigger fallback
	return os.ErrNotExist
}

// adviseMemoryAccessPatterns is a no-op on non-Linux platforms.
func adviseMemoryAccessPatterns(_, _ []byte) {
	// madvise is Linux-specific, so this is a no-op on other platforms
}
