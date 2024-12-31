package filter

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAddASingleKeyToBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKeyWithTimestamp("consensus", 5))

	filter := builder.Build()
	assert.True(t, filter.MayContain(kv.NewStringKeyWithTimestamp("consensus", 5)))
}

func TestAddAFewKeysToBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKeyWithTimestamp("consensus", 2))
	builder.Add(kv.NewStringKeyWithTimestamp("storage", 5))
	builder.Add(kv.NewStringKeyWithTimestamp("zero disk", 6))

	filter := builder.Build()
	assert.True(t, filter.MayContain(kv.NewStringKeyWithTimestamp("consensus", 5)))
	assert.True(t, filter.MayContain(kv.NewStringKeyWithTimestamp("storage", 5)))
	assert.True(t, filter.MayContain(kv.NewStringKeyWithTimestamp("zero disk", 5)))
}

func TestNonExistingKeysInBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKeyWithTimestamp("consensus", 2))
	builder.Add(kv.NewStringKeyWithTimestamp("storage", 3))
	builder.Add(kv.NewStringKeyWithTimestamp("zero disk", 4))

	filter := builder.Build()
	assert.False(t, filter.MayContain(kv.NewStringKeyWithTimestamp("disk", 2)))
	assert.False(t, filter.MayContain(kv.NewStringKeyWithTimestamp("cloud", 2)))
	assert.False(t, filter.MayContain(kv.NewStringKeyWithTimestamp("raw", 2)))
}

func TestEncodeAndDecodeBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKeyWithTimestamp("consensus", 3))
	builder.Add(kv.NewStringKeyWithTimestamp("storage", 5))
	builder.Add(kv.NewStringKeyWithTimestamp("zero disk", 8))

	buffer, err := builder.Build().Encode()
	assert.NoError(t, err)

	filter, err := DecodeToBloomFilter(buffer)
	assert.NoError(t, err)

	assert.True(t, filter.MayContain(kv.NewStringKeyWithTimestamp("consensus", 5)))
	assert.True(t, filter.MayContain(kv.NewStringKeyWithTimestamp("storage", 5)))
	assert.True(t, filter.MayContain(kv.NewStringKeyWithTimestamp("zero disk", 5)))
}
