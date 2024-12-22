package cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"unsafe"
)

func TestBlockCacheSetAndGetASingleKeyAndBlock(t *testing.T) {
	blockBuilder := block.NewBlockBuilderWithDefaultBlockSize()
	assert.True(t, blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft")))

	cacheOptions := NewComparableKeyCacheOptions[BlockId, block.Block](
		1024,
		5*time.Minute,
		func(key BlockId, value block.Block) uint32 {
			return uint32(unsafe.Sizeof(key) + unsafe.Sizeof(value))
		},
	)
	cache, err := NewBlockCache(cacheOptions)
	assert.NoError(t, err)
	assert.True(t, cache.Set(NewBlockId(10, 0), blockBuilder.Build()))

	cachedBlock, ok := cache.Get(NewBlockId(10, 0))
	assert.True(t, ok)

	iterator := cachedBlock.SeekToFirst()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "raft", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockCacheSetAndGetACoupleOfKeyAndBlocks(t *testing.T) {
	blockBuilder := block.NewBlockBuilderWithDefaultBlockSize()
	assert.True(t, blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft")))

	cacheOptions := NewComparableKeyCacheOptions[BlockId, block.Block](
		1024,
		5*time.Minute,
		func(key BlockId, value block.Block) uint32 {
			return uint32(unsafe.Sizeof(key) + unsafe.Sizeof(value))
		},
	)
	cache, err := NewBlockCache(cacheOptions)
	assert.NoError(t, err)
	assert.True(t, cache.Set(NewBlockId(10, 0), blockBuilder.Build()))

	blockBuilder = block.NewBlockBuilderWithDefaultBlockSize()
	assert.True(t, blockBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("etcd")))
	assert.True(t, cache.Set(NewBlockId(10, 1), blockBuilder.Build()))

	cachedBlock, ok := cache.Get(NewBlockId(10, 0))
	assert.True(t, ok)

	iterator := cachedBlock.SeekToFirst()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "raft", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())

	cachedBlock, ok = cache.Get(NewBlockId(10, 1))
	assert.True(t, ok)

	iterator = cachedBlock.SeekToFirst()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, "distributed", iterator.Key().RawString())
	assert.Equal(t, "etcd", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
