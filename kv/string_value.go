//go:build test

package kv

// NewStringValue creates a new instance of Value.
// It is only used for tests.
func NewStringValue(value string) Value {
	return NewValue([]byte(value))
}
