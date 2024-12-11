package filter

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/bits-and-blooms/bloom/v3"
)

// BloomFilterBuilder represents a bloom filter builder.
type BloomFilterBuilder struct {
	keys []kv.Key
}

// NewBloomFilterBuilder creates a new instance of bloom filter builder.
func NewBloomFilterBuilder() *BloomFilterBuilder {
	return &BloomFilterBuilder{}
}

// Add adds the given key to the collection of keys in BloomFilterBuilder.
func (builder *BloomFilterBuilder) Add(key kv.Key) {
	builder.keys = append(builder.keys, key)
}

// Build creates a new instance of BloomFilter.
func (builder *BloomFilterBuilder) Build() *BloomFilter {
	filter := newBloomFilter(bloom.NewWithEstimates(uint(len(builder.keys)), 0.01))
	for _, key := range builder.keys {
		filter.add(key)
	}
	return filter
}
