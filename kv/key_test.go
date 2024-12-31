package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRawKeyIsEqualTo(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.True(t, key.IsRawKeyEqualTo(NewStringKeyWithTimestamp("consensus", 20)))
}

func TestRawKeyIsNotEqualTo(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.False(t, key.IsRawKeyEqualTo(NewStringKeyWithTimestamp("raft", 10)))
}

func TestEncodedKeySize(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, 17, key.EncodedSizeInBytes())
}

func TestKeyComparisonLessThan(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, -1, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("distributed", 10)))
}

func TestKeyComparisonLessThanBasedOnTimestamp(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 15)
	assert.Equal(t, -1, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("consensus", 10)))
}

func TestKeyComparisonEqualTo(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, 0, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("consensus", 10)))
}

func TestKeyComparisonEqualToBasedOnTimestamp(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, 0, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("consensus", 10)))
}

func TestKeyComparisonGreaterThan(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, 1, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("accurate", 10)))
}

func TestKeyComparisonGreaterThanBasedOnTimestamp(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, 1, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("consensus", 20)))
}

func TestKeyIsEqualToOther(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	otherKey := NewStringKeyWithTimestamp("consensus", 10)
	assert.True(t, key.IsEqualTo(otherKey))
}

func TestKeyIsNotEqualToOtherBasedOnTimestamp(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	otherKey := NewStringKeyWithTimestamp("consensus", 11)
	assert.False(t, key.IsEqualTo(otherKey))
}

func TestKeyIsNotEqualToOtherBasedOnRawKey(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	otherKey := NewStringKeyWithTimestamp("raft", 10)
	assert.False(t, key.IsEqualTo(otherKey))
}

func TestRawStringFromAStringKey(t *testing.T) {
	key := NewStringKey("store-type")
	assert.Equal(t, "store-type", key.RawString())
}

func TestRawStringFromKey(t *testing.T) {
	key := NewKey([]byte("store-type"), 0)
	assert.Equal(t, "store-type", key.RawString())
}

func TestRawKeyIsEmpty(t *testing.T) {
	key := NewKey(nil, 0)
	assert.True(t, key.IsRawKeyEmpty())
}

func TestRawSizeInBytes(t *testing.T) {
	key := NewKey([]byte("store-type"), 0)
	assert.Equal(t, 10, key.RawSizeInBytes())
}

func TestEncodedBytes(t *testing.T) {
	key := NewKey([]byte("store-type"), 10)
	decodedKey := DecodeKeyFrom(key.EncodedBytes())

	assert.Equal(t, "store-type", decodedKey.RawString())
	assert.Equal(t, uint64(10), decodedKey.timestamp)
}

func TestEncodedSizeInBytesForAnEmptyKey(t *testing.T) {
	key := NewKey(nil, 0)
	assert.Equal(t, 0, key.EncodedSizeInBytes())
}
