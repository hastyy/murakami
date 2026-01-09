package assert

import (
	"testing"
)

func TestOK_Panics(t *testing.T) {
	// Test that it panics when condition is false
	assertPanics(t, func() {
		OK(false, "this should panic")
	})

	// Test that it doesn't panic when condition is true
	assertNotPanics(t, func() {
		OK(true, "this should not panic")
	})
}

func TestNonNil_Panics(t *testing.T) {
	// Test that it panics when value is nil
	assertPanics(t, func() {
		NonNil(nil, "this should panic")
	})

	// nil interface because of no value and no type
	var i any
	assertPanics(t, func() {
		NonNil(i, "this should panic")
	})

	// non-nil interface because it has type, although it has no value
	// so should still panic because we are only interested in the value
	// meaning that if we were to directly check y != nil, it would be true.
	var x *struct{}
	var y any = x
	assertPanics(t, func() {
		NonNil(y, "this should panic")
	})

	// Test that it doesn't panic when value is not nil
	assertNotPanics(t, func() {
		NonNil(struct{}{}, "this should not panic")
	})
}

func TestNonZero_Panics(t *testing.T) {
	// Test that it panics when value is zero for int
	assertPanics(t, func() {
		var zero int
		NonZero(zero, "this should panic")
	})

	// Test that it panics when value is zero for string
	assertPanics(t, func() {
		var zero string
		NonZero(zero, "this should panic")
	})

	// Test that it panics when value is zero for bool
	assertPanics(t, func() {
		var zero bool
		NonZero(zero, "this should panic")
	})

	// Test that it doesn't panic when value is non-zero for int
	assertNotPanics(t, func() {
		NonZero(42, "this should not panic")
	})

	// Test that it doesn't panic when value is non-zero for string
	assertNotPanics(t, func() {
		NonZero("hello", "this should not panic")
	})

	// Test that it doesn't panic when value is non-zero for bool
	assertNotPanics(t, func() {
		NonZero(true, "this should not panic")
	})
}

func TestNonEmpty_Panics(t *testing.T) {
	// Test that it panics when slice is nil
	assertPanics(t, func() {
		NonEmpty[int](nil, "this should panic")
	})

	// Test that it panics when slice is empty for int slice
	assertPanics(t, func() {
		var empty []int
		NonEmpty(empty, "this should panic")
	})

	// Test that it panics when slice is empty for string slice
	assertPanics(t, func() {
		var empty []string
		NonEmpty(empty, "this should panic")
	})

	// Test that it panics when slice is explicitly empty
	assertPanics(t, func() {
		NonEmpty([]int{}, "this should panic")
	})

	// Test that it doesn't panic when slice is not empty for int slice
	assertNotPanics(t, func() {
		NonEmpty([]int{1, 2, 3}, "this should not panic")
	})

	// Test that it doesn't panic when slice is not empty for string slice
	assertNotPanics(t, func() {
		NonEmpty([]string{"hello", "world"}, "this should not panic")
	})

	// Test that it doesn't panic when slice has one element
	assertNotPanics(t, func() {
		NonEmpty([]int{42}, "this should not panic")
	})
}

func assertPanics(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic but code did not panic")
		}
	}()
	f()
}

func assertNotPanics(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Expected no panic but code panicked: %v", r)
		}
	}()
	f()
}
