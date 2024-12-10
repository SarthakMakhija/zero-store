package kv

import (
	"bytes"
)

var EmptyKey = Key{key: nil}

type Key struct {
	key []byte
}

// NewKey creates a new instance of the Key.
func NewKey(key []byte) Key {
	return Key{
		key: key,
	}
}

// DecodeKeyFrom decodes the key from the given byte slice.
func DecodeKeyFrom(buffer []byte) Key {
	//TODO: handle timestamp
	return Key{
		key: buffer[:],
	}
}

// EncodedBytes returns the encoded format of the Key.
func (key Key) EncodedBytes() []byte {
	if key.IsRawKeyEmpty() {
		return nil
	}
	buffer := make([]byte, key.EncodedSizeInBytes())

	copy(buffer, key.key)
	//TODO: handle timestamp

	return buffer
}

// EncodedSizeInBytes returns the length of the encoded key.
func (key Key) EncodedSizeInBytes() int {
	if key.IsRawKeyEmpty() {
		return 0
	}
	return len(key.key) //TODO: handle timestamp
}

// CompareKeys compares the user provided key and the instance of the Key existing in the system.
// It is mainly called from external.SkipList.
func CompareKeys(userKey, systemKey Key) int {
	//TODO: compare using timestamp.
	return bytes.Compare(userKey.key, systemKey.key)
}

// IsRawKeyEqualTo returns true if the raw key two keys is the same.
func (key Key) IsRawKeyEqualTo(other Key) bool {
	return bytes.Compare(key.key, other.key) == 0
}

// RawString returns the string representation of raw key.
func (key Key) RawString() string {
	return string(key.RawBytes())
}

// RawBytes returns the raw key.
func (key Key) RawBytes() []byte {
	return key.key
}

// IsRawKeyEmpty returns true if the raw key is empty.
func (key Key) IsRawKeyEmpty() bool {
	return key.RawSizeInBytes() == 0
}

// RawSizeInBytes returns the size of the raw key.
func (key Key) RawSizeInBytes() int {
	return len(key.RawBytes())
}
