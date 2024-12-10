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
