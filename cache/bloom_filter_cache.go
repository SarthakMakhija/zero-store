package cache

import (
	"github.com/SarthakMakhija/zero-store/objectstore/filter"
	"github.com/maypok86/otter"
	"unsafe"
)

const (
	cacheKeySize   = unsafe.Sizeof(uint64(0))
	cacheValueSize = unsafe.Sizeof(filter.BloomFilter{})
)

type BloomFilterCache struct {
	cache otter.Cache[uint64, filter.BloomFilter]
}

func NewBloomFilterCache(options BloomFilterCacheOptions) (BloomFilterCache, error) {
	cache, err := otter.MustBuilder[uint64, filter.BloomFilter](int(options.sizeInBytes)).
		Cost(func(key uint64, value filter.BloomFilter) uint32 {
			return uint32(cacheKeySize + cacheValueSize)
		}).
		WithTTL(options.entryTTL).
		Build()

	if err != nil {
		return BloomFilterCache{}, err
	}
	return BloomFilterCache{
		cache: cache,
	}, nil
}

func (cache BloomFilterCache) Set(key uint64, filter filter.BloomFilter) bool {
	return cache.cache.Set(key, filter)
}

func (cache BloomFilterCache) Get(key uint64) (filter.BloomFilter, bool) {
	return cache.cache.Get(key)
}
