package kv

// Value is a tiny wrapper over raw []byte slice.
type Value struct {
	value []byte
}

// NewValue creates a new instance of Value
func NewValue(value []byte) Value {
	return Value{value: value}
}

// IsEmpty returns true if the Value is empty.
func (value Value) IsEmpty() bool {
	return len(value.value) == 0
}

// String returns the string representation of Value.
func (value Value) String() string {
	return string(value.value)
}

// SizeInBytes returns the length of the raw byte slice.
func (value Value) SizeInBytes() int {
	return len(value.value)
}

// Bytes returns the raw value.
func (value Value) Bytes() []byte {
	return value.value
}
