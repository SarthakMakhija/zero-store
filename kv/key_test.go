package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRawStringFromKey(t *testing.T) {
	key := NewStringKey("store-type")
	assert.Equal(t, "store-type", key.RawString())
}
