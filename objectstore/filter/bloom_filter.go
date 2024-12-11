package filter

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/bits-and-blooms/bloom/v3"
)

// BloomFilter is a wrapper over filter.BloomFilter.
type BloomFilter struct {
	filter *bloom.BloomFilter
}

// newBloomFilter creates a new instance of BloomFilter.
func newBloomFilter(filter *bloom.BloomFilter) *BloomFilter {
	return &BloomFilter{
		filter: filter,
	}
}

// Add adds the given key in the bloom filter.
func (filter *BloomFilter) add(key kv.Key) {
	filter.filter.Add(key.RawBytes())
}

// mayBePresent returns true if the given key may be present in the bloom filter, false otherwise.
func (filter *BloomFilter) mayBePresent(key kv.Key) bool {
	return filter.filter.Test(key.RawBytes())
}
