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
	assert.True(t, filter.mayHave(kv.NewStringKey("consensus")))
}

func TestAddAFewKeysToBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))
	builder.Add(kv.NewStringKey("storage"))
	builder.Add(kv.NewStringKey("zero disk"))

	filter := builder.Build()
	assert.True(t, filter.mayHave(kv.NewStringKey("consensus")))
	assert.True(t, filter.mayHave(kv.NewStringKey("storage")))
	assert.True(t, filter.mayHave(kv.NewStringKey("zero disk")))
}

func TestNonExistingKeysInBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))
	builder.Add(kv.NewStringKey("storage"))
	builder.Add(kv.NewStringKey("zero disk"))

	filter := builder.Build()
	assert.False(t, filter.mayHave(kv.NewStringKey("disk")))
	assert.False(t, filter.mayHave(kv.NewStringKey("cloud")))
	assert.False(t, filter.mayHave(kv.NewStringKey("raw")))
}

func TestEncodeAndDecodeBloomFilter(t *testing.T) {
	builder := NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))
	builder.Add(kv.NewStringKey("storage"))
	builder.Add(kv.NewStringKey("zero disk"))

	buffer, err := builder.Build().Encode()
	assert.Nil(t, err)

	filter, err := decodeBloomFilter(buffer)
	assert.Nil(t, err)

	assert.True(t, filter.mayHave(kv.NewStringKey("consensus")))
	assert.True(t, filter.mayHave(kv.NewStringKey("storage")))
	assert.True(t, filter.mayHave(kv.NewStringKey("zero disk")))
}
