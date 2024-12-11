package kv

import "unsafe"

var EmptyValue = Value{value: nil}

const (
	deletedMarker    byte = 0x01
	nonDeletedMarker byte = 0x00
)

const deletedByteSize = int(unsafe.Sizeof(uint8(0)))

// Value is a tiny wrapper over raw []byte slice.
type Value struct {
	value   []byte
	deleted byte
}

// DecodeValueFrom sets the provided byte slice as its value.
// It is mainly called from external.SkipList.
func DecodeValueFrom(buffer []byte) Value {
	length := len(buffer)
	return Value{
		value:   buffer[:length-1],
		deleted: buffer[length-1],
	}
}

// NewValue creates a new instance of Value.
func NewValue(value []byte) Value {
	return Value{
		value:   value,
		deleted: nonDeletedMarker,
	}
}

// NewDeletedValue creates a new instance of deleted Value.
func NewDeletedValue() Value {
	return Value{
		value:   nil,
		deleted: deletedMarker,
	}
}

// EncodeTo writes the raw byte slice to the provided buffer.
// It is mainly called from external.SkipList.
func (value Value) EncodeTo(buffer []byte) {
	if len(buffer) < value.SizeInBytes() {
		panic("buffer too small to encode value")
	}
	numberOfBytesCopied := copy(buffer[:], value.value)
	buffer[numberOfBytesCopied] = value.deleted
}

// IsEmpty returns true if the Value is empty.
func (value Value) IsEmpty() bool {
	return len(value.Bytes()) == 0
}

// IsDeleted returns true if the value is deleted.
func (value Value) IsDeleted() bool {
	return value.deleted&deletedMarker == deletedMarker
}

// SizeInBytes returns the length of the raw byte slice.
func (value Value) SizeInBytes() int {
	return len(value.Bytes()) + deletedByteSize
}

// SizeAsUint32 returns the length of the raw byte slice as uint32.
func (value Value) SizeAsUint32() uint32 {
	return uint32(value.SizeInBytes())
}

// Bytes returns the raw value.
func (value Value) Bytes() []byte {
	return value.value
}

// String returns the string representation of Value.
func (value Value) String() string {
	return string(value.Bytes())
}
