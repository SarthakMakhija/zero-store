package filter

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAddASingleKeyToBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))

	filter := builder.Build()
	assert.True(t, filter.MayContain(kv.NewStringKey("consensus")))
}

func TestAddAFewKeysToBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))
	builder.Add(kv.NewStringKey("storage"))
	builder.Add(kv.NewStringKey("zero disk"))

	filter := builder.Build()
	assert.True(t, filter.MayContain(kv.NewStringKey("consensus")))
	assert.True(t, filter.MayContain(kv.NewStringKey("storage")))
	assert.True(t, filter.MayContain(kv.NewStringKey("zero disk")))
}

func TestNonExistingKeysInBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))
	builder.Add(kv.NewStringKey("storage"))
	builder.Add(kv.NewStringKey("zero disk"))

	filter := builder.Build()
	assert.False(t, filter.MayContain(kv.NewStringKey("disk")))
	assert.False(t, filter.MayContain(kv.NewStringKey("cloud")))
	assert.False(t, filter.MayContain(kv.NewStringKey("raw")))
}

func TestEncodeAndDecodeBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))
	builder.Add(kv.NewStringKey("storage"))
	builder.Add(kv.NewStringKey("zero disk"))

	buffer, err := builder.Build().Encode()
	assert.NoError(t, err)

	filter, err := DecodeToBloomFilter(buffer)
	assert.NoError(t, err)

	assert.True(t, filter.MayContain(kv.NewStringKey("consensus")))
	assert.True(t, filter.MayContain(kv.NewStringKey("storage")))
	assert.True(t, filter.MayContain(kv.NewStringKey("zero disk")))
}
