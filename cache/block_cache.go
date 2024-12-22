package cache

import (
	"github.com/SarthakMakhija/zero-store/objectstore/block"
)

type BlockId struct {
	segmentId  uint64
	blockIndex int
}

func NewBlockId(segmentId uint64, blockIndex int) BlockId {
	return BlockId{
		segmentId:  segmentId,
		blockIndex: blockIndex,
	}
}

type BlockCache struct {
	comparableKeyCache[BlockId, block.Block]
}

func NewBlockCache(options ComparableKeyCacheOptions[BlockId, block.Block]) (BlockCache, error) {
	cache, err := newComparableKeyCache[BlockId, block.Block](options)
	if err != nil {
		return BlockCache{}, err
	}
	return BlockCache{
		cache,
	}, nil
}
