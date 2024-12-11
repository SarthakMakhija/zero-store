package objectstore

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAddASingleKeyToBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))

	filter := builder.Build()
	assert.True(t, filter.mayBePresent(kv.NewStringKey("consensus")))
}

func TestAddAFewKeysToBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))
	builder.Add(kv.NewStringKey("storage"))
	builder.Add(kv.NewStringKey("zero disk"))

	filter := builder.Build()
	assert.True(t, filter.mayBePresent(kv.NewStringKey("consensus")))
	assert.True(t, filter.mayBePresent(kv.NewStringKey("storage")))
	assert.True(t, filter.mayBePresent(kv.NewStringKey("zero disk")))
}

func TestNonExistingKeysInBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))
	builder.Add(kv.NewStringKey("storage"))
	builder.Add(kv.NewStringKey("zero disk"))

	filter := builder.Build()
	assert.False(t, filter.mayBePresent(kv.NewStringKey("disk")))
	assert.False(t, filter.mayBePresent(kv.NewStringKey("cloud")))
	assert.False(t, filter.mayBePresent(kv.NewStringKey("raw")))
}
