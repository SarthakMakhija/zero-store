//go:build test

package kv

// NewStringKey creates a new instance of Key.
// It is only used for tests.
func NewStringKey(key string) Key {
	return NewStringKeyWithTimestamp(key, 0)
}

// NewStringKeyWithTimestamp creates a new instance of Key.
// It is only used for tests.
func NewStringKeyWithTimestamp(key string, timestamp uint64) Key {
	return NewKey([]byte(key), timestamp)
}
