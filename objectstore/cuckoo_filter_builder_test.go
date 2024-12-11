package objectstore

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAddASingleKeyToCuckooFilter(t *testing.T) {
	builder := NewCuckooFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))

	filter := builder.Build()
	assert.True(t, filter.Lookup(kv.NewStringKey("consensus").RawBytes()))
}

func TestAddAFewKeysToCuckooFilter(t *testing.T) {
	builder := NewCuckooFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))
	builder.Add(kv.NewStringKey("storage"))
	builder.Add(kv.NewStringKey("zero disk"))

	filter := builder.Build()
	assert.True(t, filter.Lookup(kv.NewStringKey("consensus").RawBytes()))
	assert.True(t, filter.Lookup(kv.NewStringKey("storage").RawBytes()))
	assert.True(t, filter.Lookup(kv.NewStringKey("zero disk").RawBytes()))
}

func TestNonExistingKeysInCuckooFilter(t *testing.T) {
	builder := NewCuckooFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))
	builder.Add(kv.NewStringKey("storage"))
	builder.Add(kv.NewStringKey("zero disk"))

	filter := builder.Build()
	assert.False(t, filter.Lookup(kv.NewStringKey("disk").RawBytes()))
	assert.False(t, filter.Lookup(kv.NewStringKey("cloud").RawBytes()))
	assert.False(t, filter.Lookup(kv.NewStringKey("raw").RawBytes()))
}
