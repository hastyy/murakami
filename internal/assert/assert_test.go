package assert

import (
	"testing"

	"github.com/hastyy/murakami/internal/testutil"
)

func TestOK_Panics(t *testing.T) {
	// Test that it panics when condition is false
	testutil.AssertPanics(t, func() {
		OK(false, "error: %s", "test")
	})

	// Test that it doesn't panic when condition is true
	testutil.AssertNotPanics(t, func() {
		OK(true, "this should not panic")
	})
}

func TestNonNil_Panics(t *testing.T) {
	// Test that it panics when value is nil
	testutil.AssertPanics(t, func() {
		NonNil(nil, "error: %s", "test")
	})

	//
	var i any
	testutil.AssertPanics(t, func() {
		NonNil(i, "error: %s", "test")
	})

	type T any
	var x T
	testutil.AssertPanics(t, func() {
		NonNil(x, "error: %s", "test")
	})

	// Test that it doesn't panic when value is not nil
	testutil.AssertNotPanics(t, func() {
		NonNil(struct{}{}, "this should not panic")
	})
}
