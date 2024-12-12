package filter

import (
	"fmt"
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

	buffer, err := builder.Build().asBytes()
	assert.Nil(t, err)

	filter, err := decodeBloomFilter(buffer)
	assert.Nil(t, err)

	assert.True(t, filter.mayHave(kv.NewStringKey("consensus")))
	assert.True(t, filter.mayHave(kv.NewStringKey("storage")))
	assert.True(t, filter.mayHave(kv.NewStringKey("zero disk")))
}

func TestS(t *testing.T) {
	builder := NewBloomFilterBuilder()
	for count := 0; count < 10_000; count++ {
		builder.Add(kv.NewStringKey(fmt.Sprintf("consensus%d", count)))
	}

	buffer, err := builder.Build().asBytes()
	assert.Nil(t, err)

	//12008
	println(len(buffer))
}
