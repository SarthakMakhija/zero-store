package objectstore

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/bits-and-blooms/bloom/v3"
)

// FilterBuilder represents a cuckoo filter builder.
type FilterBuilder struct {
	keys []kv.Key
}

// NewBloomFilterBuilder creates a new instance of bloom filter builder.
func NewBloomFilterBuilder() *FilterBuilder {
	return &FilterBuilder{}
}

// Add adds the given key to the collection of keys in FilterBuilder.
func (builder *FilterBuilder) Add(key kv.Key) {
	builder.keys = append(builder.keys, key)
}

// Build creates a new instance of BloomFilter.
func (builder *FilterBuilder) Build() *BloomFilter {
	filter := newBloomFilter(bloom.NewWithEstimates(uint(len(builder.keys)), 0.01))
	for _, key := range builder.keys {
		filter.add(key)
	}
	return filter
}
