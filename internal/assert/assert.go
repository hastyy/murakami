// Package assert provides runtime tripwires to validate state invariants.
// It is a fail-fast mechanism that allows us to immediately crash processes that
// go into a bad state.
// This can also allow us to catch bugs faster.
// This practice has a great synergy with deterministic simulation testing to find bugs even faster.
package assert

import (
	"fmt"
	"reflect"
)

func OK(cond bool, format string, args ...any) {
	if !cond {
		panic(failedMsgf(format, args...))
	}
}

func NonNil(v any, format string, args ...any) {
	if isNil(v) {
		panic(failedMsgf(format, args...))
	}
}

func isNil(i any) bool {
	if i == nil {
		return true
	}
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

func failedMsgf(format string, args ...any) string {
	return fmt.Sprintln("assertion failed:", fmt.Sprintf(format, args...))
}
