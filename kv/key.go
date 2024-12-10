package kv

type Key struct {
	key []byte
}

// NewKey creates a new instance of the Key.
func NewKey(key []byte) Key {
	return Key{
		key: key,
	}
}

// RawString returns the string representation of raw key.
func (key Key) RawString() string {
	return string(key.key)
}
