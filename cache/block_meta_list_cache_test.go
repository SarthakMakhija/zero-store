package cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"unsafe"
)

func TestBlockMetaListCacheSetAndGetASingleKeyAndMetaList(t *testing.T) {
	blockMetaList := block.NewBlockMetaList(false)
	blockMetaList.Add(block.Meta{
		BlockBeginOffset: 0,
		StartingKey:      kv.NewStringKey("accurate"),
		EndingKey:        kv.NewStringKey("consensus"),
	})

	cacheOptions := NewComparableKeyCacheOptions[uint64, *block.MetaList](
		200,
		5*time.Minute,
		func(key uint64, value *block.MetaList) uint32 {
			return uint32(unsafe.Sizeof(key) + unsafe.Sizeof(value))
		},
	)
	cache, err := NewBlockMetaListCache(cacheOptions)
	assert.NoError(t, err)

	assert.True(t, cache.Set(10, blockMetaList))

	cachedBlockMetaList, ok := cache.Get(10)
	assert.True(t, ok)
	meta, _ := cachedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
}

func TestBlockMetaListCacheSetAndGetACoupleOfKeyAndMetaList(t *testing.T) {
	blockMetaList := block.NewBlockMetaList(false)
	blockMetaList.Add(block.Meta{
		BlockBeginOffset: 0,
		StartingKey:      kv.NewStringKey("accurate"),
		EndingKey:        kv.NewStringKey("consensus"),
	})

	cacheOptions := NewComparableKeyCacheOptions[uint64, *block.MetaList](
		200,
		5*time.Minute,
		func(key uint64, value *block.MetaList) uint32 {
			return uint32(unsafe.Sizeof(key) + unsafe.Sizeof(value))
		},
	)
	cache, err := NewBlockMetaListCache(cacheOptions)
	assert.NoError(t, err)
	assert.True(t, cache.Set(10, blockMetaList))

	blockMetaList = block.NewBlockMetaList(false)
	blockMetaList.Add(block.Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKey("distributed"),
		EndingKey:        kv.NewStringKey("foundationDb"),
	})
	assert.True(t, cache.Set(20, blockMetaList))

	cachedBlockMetaList, ok := cache.Get(10)
	assert.True(t, ok)
	meta, _ := cachedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())

	cachedBlockMetaList, ok = cache.Get(20)
	assert.True(t, ok)
	meta, _ = cachedBlockMetaList.GetAt(0)
	assert.Equal(t, "distributed", meta.StartingKey.RawString())
}