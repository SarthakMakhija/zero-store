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
	expectedSize := 22 + deletedByteSize
	assert.Equal(t, expectedSize, value.SizeInBytes())
}

func TestSizeAsUint32OfValue(t *testing.T) {
	value := NewStringValue("zero disk architecture")
	expectedSize := 22 + deletedByteSize
	assert.Equal(t, uint32(expectedSize), value.SizeAsUint32())
}

func TestValueIsEmpty(t *testing.T) {
	value := NewStringValue("")
	assert.True(t, value.IsEmpty())
}

func TestEncodeValue1(t *testing.T) {
	value := NewStringValue("zero disk architecture")
	buffer := make([]byte, value.SizeAsUint32())

	value.EncodeTo(buffer)
	decodedValue := DecodeValueFrom(buffer)

	assert.Equal(t, "zero disk architecture", decodedValue.String())
}

func TestEncodeValue2(t *testing.T) {
	value := NewStringValue("zero disk architecture")
	decodedValue := DecodeValueFrom(value.EncodedBytes())

	assert.Equal(t, "zero disk architecture", decodedValue.String())
}

func TestEncodeADeletedValue1(t *testing.T) {
	value := NewDeletedValue()
	buffer := make([]byte, value.SizeAsUint32())

	value.EncodeTo(buffer)
	decodedValue := DecodeValueFrom(buffer)

	assert.Equal(t, "", decodedValue.String())
	assert.True(t, value.IsDeleted())
}

func TestEncodeADeletedValue2(t *testing.T) {
	value := NewDeletedValue()
	decodedValue := DecodeValueFrom(value.EncodedBytes())

	assert.Equal(t, "", decodedValue.String())
	assert.True(t, value.IsDeleted())
}
