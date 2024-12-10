//go:build test

package kv

// NewStringKey creates a new instance of Key.
// It is only used for tests.
func NewStringKey(key string) Key {
	return Key{key: []byte(key)}
}
