package log_old2

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew_CreatesLogFromScratch(t *testing.T) {
	// Create a temporary directory for the test
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	logName := "testlog"

	// Create a new log from scratch (no existing segments)
	log, err := New(dataDir, logName)

	// Verify no error occurred
	require.NoError(t, err, "New() should succeed when creating log from scratch")
	require.NotNil(t, log, "New() should return a non-nil log")

	// Verify log structure
	require.NotNil(t, log.activeSegment, "log should have an active segment")
	require.NotNil(t, log.segments, "log should have a segments slice")

	// When creating from scratch, we should have:
	// - No sealed segments (empty directory)
	// - One active segment (the newly created one)
	require.Len(t, log.segments, 1, "log should have exactly one segment (the active segment)")

	// Verify the active segment is in the segments slice
	require.Equal(t, log.activeSegment, log.segments[0], "active segment should be the first (and only) segment")

	// Verify the active segment has base offset 0 (since no existing segments)
	require.Equal(t, Offset(0), log.activeSegment.BaseOffset(), "active segment should have base offset 0 when creating from scratch")

	// Verify the log directory was created
	logDir := filepath.Join(dataDir, logName)
	require.DirExists(t, logDir, "log directory should be created")

	// Verify directory permissions (should be 0700)
	info, err := os.Stat(logDir)
	require.NoError(t, err, "should be able to stat log directory")
	require.Equal(t, os.FileMode(directoryPermissions), info.Mode().Perm(), "log directory should have correct permissions")
}
