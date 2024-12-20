package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRawStringFromAStringKey(t *testing.T) {
	key := NewStringKey("store-type")
	assert.Equal(t, "store-type", key.RawString())
}

func TestRawStringFromKey(t *testing.T) {
	key := NewKey([]byte("store-type"))
	assert.Equal(t, "store-type", key.RawString())
}

func TestRawKeyIsEmpty(t *testing.T) {
	key := NewKey(nil)
	assert.True(t, key.IsRawKeyEmpty())
}

func TestRawKeyIsEqualTo(t *testing.T) {
	key := NewStringKey("consensus")
	assert.True(t, key.IsRawKeyEqualTo(NewStringKey("consensus")))
}

func TestRawKeyIsNotEqualTo(t *testing.T) {
	key := NewStringKey("consensus")
	assert.False(t, key.IsRawKeyEqualTo(NewStringKey("raft")))
}

func TestRawSizeInBytes(t *testing.T) {
	key := NewKey([]byte("store-type"))
	assert.Equal(t, 10, key.RawSizeInBytes())
}

func TestEncodedBytes(t *testing.T) {
	key := NewKey([]byte("store-type"))
	decodedKey := DecodeKeyFrom(key.EncodedBytes())

	assert.Equal(t, "store-type", decodedKey.RawString())
}

func TestEncodedSizeInBytes(t *testing.T) {
	key := NewKey([]byte("store-type"))
	assert.Equal(t, 10, key.EncodedSizeInBytes())
}

func TestEncodedSizeInBytesForAnEmptyKey(t *testing.T) {
	key := NewKey(nil)
	assert.Equal(t, 0, key.EncodedSizeInBytes())
}

func TestKeyIsEqualToOther(t *testing.T) {
	key := NewKey([]byte("store-type"))
	assert.True(t, key.IsEqualTo(NewStringKey("store-type")))
}

func TestKeyIsNotEqualToOther(t *testing.T) {
	key := NewKey([]byte("store-type"))
	assert.False(t, key.IsEqualTo(NewStringKey("zero-store")))
}
