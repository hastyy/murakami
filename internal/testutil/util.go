package testutil

import "testing"

func AssertPanics(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic but code did not panic")
		}
	}()
	f()
}

func AssertNotPanics(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Expected no panic but code panicked: %v", r)
		}
	}()
	f()
}
