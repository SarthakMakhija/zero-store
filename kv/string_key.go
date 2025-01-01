//go:build test

package kv

// NewStringKeyWithTimestamp creates a new instance of Key.
// It is only used for tests.
func NewStringKeyWithTimestamp(key string, timestamp uint64) Key {
	return NewKey([]byte(key), timestamp)
}
