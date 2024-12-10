package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmptyValue(t *testing.T) {
	value := NewValue(nil)
	assert.True(t, value.IsEmpty())
}

func TestRawStringFromValue(t *testing.T) {
	value := NewStringValue("zero disk architecture")
	assert.Equal(t, "zero disk architecture", value.String())
}

func TestSizeInBytesOfValue(t *testing.T) {
	value := NewStringValue("zero disk architecture")
	assert.Equal(t, 22, value.SizeInBytes())
}

func TestValueIsEmpty(t *testing.T) {
	value := NewStringValue("")
	assert.True(t, value.IsEmpty())
}

func TestEncodeValue(t *testing.T) {
	value := NewStringValue("zero disk architecture")
	buffer := make([]byte, len(value.String()))

	value.EncodeTo(buffer)
	decodedValue := DecodeValueFrom(buffer)

	assert.Equal(t, "zero disk architecture", decodedValue.String())
}
