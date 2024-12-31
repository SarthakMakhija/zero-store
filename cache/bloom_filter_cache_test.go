package cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore/filter"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"unsafe"
)

func TestBloomFilterCacheSetAndGetASingleKeyAndBloomFilter(t *testing.T) {
	builder := filter.NewBloomFilterBuilder()
	builder.Add(kv.NewStringKeyWithTimestamp("consensus", 5))

	cacheOptions := NewComparableKeyCacheOptions[uint64, filter.BloomFilter](
		200,
		5*time.Minute,
		func(key uint64, value filter.BloomFilter) uint32 {
			return uint32(unsafe.Sizeof(key) + unsafe.Sizeof(value))
		},
	)
	cache, err := NewBloomFilterCache(cacheOptions)
	assert.NoError(t, err)

	assert.True(t, cache.Set(10, builder.Build()))

	cachedFilter, ok := cache.Get(10)
	assert.True(t, ok)
	assert.True(t, cachedFilter.MayContain(kv.NewStringKeyWithTimestamp("consensus", 5)))
}

func TestBloomFilterCacheSetAndGetACoupleOfKeyAndBloomFilters(t *testing.T) {
	builder := filter.NewBloomFilterBuilder()
	builder.Add(kv.NewStringKeyWithTimestamp("consensus", 7))

	cacheOptions := NewComparableKeyCacheOptions[uint64, filter.BloomFilter](
		200,
		5*time.Minute,
		func(key uint64, value filter.BloomFilter) uint32 {
			return uint32(unsafe.Sizeof(key) + unsafe.Sizeof(value))
		},
	)
	cache, err := NewBloomFilterCache(cacheOptions)
	assert.NoError(t, err)
	assert.True(t, cache.Set(10, builder.Build()))

	builder = filter.NewBloomFilterBuilder()
	builder.Add(kv.NewStringKeyWithTimestamp("raft", 8))
	assert.True(t, cache.Set(20, builder.Build()))

	cachedFilter, ok := cache.Get(10)
	assert.True(t, ok)
	assert.True(t, cachedFilter.MayContain(kv.NewStringKeyWithTimestamp("consensus", 9)))

	cachedFilter, ok = cache.Get(20)
	assert.True(t, ok)
	assert.True(t, cachedFilter.MayContain(kv.NewStringKeyWithTimestamp("raft", 10)))
}

func TestBloomFilterCacheWithFewElementsUpToTheSize(t *testing.T) {
	builder := filter.NewBloomFilterBuilder()
	builder.Add(kv.NewStringKeyWithTimestamp("consensus", 8))

	cacheOptions := NewComparableKeyCacheOptions[uint64, filter.BloomFilter](
		200,
		5*time.Minute,
		func(key uint64, value filter.BloomFilter) uint32 {
			return uint32(unsafe.Sizeof(key) + unsafe.Sizeof(value))
		},
	)
	cache, err := NewBloomFilterCache(cacheOptions)
	assert.NoError(t, err)
	assert.True(t, cache.Set(10, builder.Build()))

	builder = filter.NewBloomFilterBuilder()
	builder.Add(kv.NewStringKeyWithTimestamp("raft", 10))
	bloomFilter := builder.Build()

	//size of an entry is 16 bytes, 12 entries means 192 bytes
	for key := 1; key <= 12; key++ {
		assert.True(t, cache.Set(uint64(key), bloomFilter))
	}
	for key := 1; key <= 12; key++ {
		_, ok := cache.Get(uint64(key))
		assert.True(t, ok)
	}
}
