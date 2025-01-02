package kv

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

const TimestampSize = int(unsafe.Sizeof(uint64(0)))

var EmptyKey = Key{key: nil}

type Key struct {
	key       []byte
	timestamp uint64
}

// NewKey creates a new instance of the Key.
func NewKey(key []byte, timestamp uint64) Key {
	return Key{
		key:       key,
		timestamp: timestamp,
	}
}

// DecodeKeyFrom decodes the key from the given byte slice.
func DecodeKeyFrom(buffer []byte) Key {
	if len(buffer) < TimestampSize {
		panic("buffer too small to decode the key from")
	}

	length := len(buffer)
	return Key{
		key:       buffer[:length-TimestampSize],
		timestamp: binary.LittleEndian.Uint64(buffer[length-TimestampSize:]),
	}
}

// EncodedBytes returns the encoded format of the Key.
// The encoded format of Key includes:
//
// | Raw Key| timestamp |
func (key Key) EncodedBytes() []byte {
	if key.IsRawKeyEmpty() {
		return nil
	}
	buffer := make([]byte, key.EncodedSizeInBytes())

	numberOfBytesWritten := copy(buffer, key.key)
	binary.LittleEndian.PutUint64(buffer[numberOfBytesWritten:], key.timestamp)

	return buffer
}

// EncodedSizeInBytes returns the length of the encoded key.
func (key Key) EncodedSizeInBytes() int {
	if key.IsRawKeyEmpty() {
		return 0
	}
	return len(key.key) + TimestampSize
}

// CompareKeysWithDescendingTimestamp compares the two keys.
// It compares the raw keys ([]byte).
// If the comparison result is not zero, it is returned.
// Else, the timestamps of the two keys are compared:
// 1) It returns 0, if the timestamps of the two keys are same.
// 2) It returns -1, if the timestamp of key is greater than the timestamp of the other key.
// 3) It returns 1, if the timestamp of key is less than the timestamp of the other key.
// Timestamp plays an important role in ordering of keys.
// Consider a key "consensus" with timestamps 15 and 13 in a sorted segment,
// and user wants to perform a scan between "consensus" to "decimal" with timestamp as 16. This means we would like to return
// all the keys falling between the two, such that: timestamp of the keys in system <= 16.
// However, "consensus" is present with 15 and 13 timestamps. We would only return "consensus" with timestamp 15. If the key
// "consensus" with timestamp 15 is placed before the key "consensus" with timestamp 13, range iteration becomes easier because
// the first key will always have the latest timestamp.
func (key Key) CompareKeysWithDescendingTimestamp(other Key) int {
	comparison := bytes.Compare(key.key, other.key)
	if comparison != 0 {
		return comparison
	}
	if key.timestamp == other.timestamp {
		return 0
	}
	if key.timestamp > other.timestamp {
		return -1
	}
	return 1
}

// CompareKeys compares the user provided key and the instance of the Key existing in the system.
// It is mainly called from external.SkipList.
func CompareKeys(userKey, systemKey Key) int {
	return userKey.CompareKeysWithDescendingTimestamp(systemKey)
}

// IsEqualTo returns true if the Key is equal to the other Key.
func (key Key) IsEqualTo(other Key) bool {
	return bytes.Equal(key.key, other.key) && key.timestamp == other.timestamp
}

// IsRawKeyEqualTo returns true if the raw key two keys is the same.
func (key Key) IsRawKeyEqualTo(other Key) bool {
	return bytes.Equal(key.key, other.key)
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

// IsRawKeyGreaterThan returns true if the raw key of the key is greater than the raw key of the other.
func (key Key) IsRawKeyGreaterThan(other Key) bool {
	return bytes.Compare(key.key, other.key) > 0
}

// IsRawKeyLesserThan returns true if the raw key of key is lesser than the raw key of the other.
func (key Key) IsRawKeyLesserThan(other Key) bool {
	return bytes.Compare(key.key, other.key) < 0
}

// RawSizeInBytes returns the size of the raw key.
func (key Key) RawSizeInBytes() int {
	return len(key.RawBytes())
}

// Timestamp returns the timestamp of the key.
func (key Key) Timestamp() uint64 {
	return key.timestamp
}
