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
	builder.Add(kv.NewStringKey("consensus"))

	cacheOptions := NewComparableKeyCacheOptions[uint64, filter.BloomFilter](
		200,
		5*time.Minute,
		func(key uint64, value filter.BloomFilter) uint32 {
			return uint32(unsafe.Sizeof(key) + unsafe.Sizeof(value))
		},
	)
	cache, err := NewComparableKeyCache[uint64, filter.BloomFilter](cacheOptions)
	assert.NoError(t, err)

	assert.True(t, cache.Set(10, builder.Build()))

	cachedFilter, ok := cache.Get(10)
	assert.True(t, ok)
	assert.True(t, cachedFilter.MayContain(kv.NewStringKey("consensus")))
}

func TestBloomFilterCacheSetAndGetACoupleOfKeyAndBloomFilters(t *testing.T) {
	builder := filter.NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))

	cacheOptions := NewComparableKeyCacheOptions[uint64, filter.BloomFilter](
		200,
		5*time.Minute,
		func(key uint64, value filter.BloomFilter) uint32 {
			return uint32(unsafe.Sizeof(key) + unsafe.Sizeof(value))
		},
	)
	cache, err := NewComparableKeyCache[uint64, filter.BloomFilter](cacheOptions)
	assert.NoError(t, err)
	assert.True(t, cache.Set(10, builder.Build()))

	builder = filter.NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("raft"))
	assert.True(t, cache.Set(20, builder.Build()))

	cachedFilter, ok := cache.Get(10)
	assert.True(t, ok)
	assert.True(t, cachedFilter.MayContain(kv.NewStringKey("consensus")))

	cachedFilter, ok = cache.Get(20)
	assert.True(t, ok)
	assert.True(t, cachedFilter.MayContain(kv.NewStringKey("raft")))
}

func TestBloomFilterCacheWithFewElementsUpToTheSize(t *testing.T) {
	builder := filter.NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("consensus"))

	cacheOptions := NewComparableKeyCacheOptions[uint64, filter.BloomFilter](
		200,
		5*time.Minute,
		func(key uint64, value filter.BloomFilter) uint32 {
			return uint32(unsafe.Sizeof(key) + unsafe.Sizeof(value))
		},
	)
	cache, err := NewComparableKeyCache[uint64, filter.BloomFilter](cacheOptions)
	assert.NoError(t, err)
	assert.True(t, cache.Set(10, builder.Build()))

	builder = filter.NewBloomFilterBuilder()
	builder.Add(kv.NewStringKey("raft"))
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
