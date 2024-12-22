package cache

import "github.com/SarthakMakhija/zero-store/objectstore/filter"

type BloomFilterCache struct {
	comparableKeyCache[uint64, filter.BloomFilter]
}

func NewBloomFilterCache(options ComparableKeyCacheOptions[uint64, filter.BloomFilter]) (BloomFilterCache, error) {
	cache, err := newComparableKeyCache[uint64, filter.BloomFilter](options)
	if err != nil {
		return BloomFilterCache{}, err
	}
	return BloomFilterCache{
		cache,
	}, nil
}
