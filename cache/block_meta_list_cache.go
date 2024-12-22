package cache

import (
	"github.com/SarthakMakhija/zero-store/objectstore/block"
)

type BlockMetaListCache struct {
	comparableKeyCache[uint64, *block.MetaList]
}

func NewBlockMetaListCache(options ComparableKeyCacheOptions[uint64, *block.MetaList]) (BlockMetaListCache, error) {
	cache, err := newComparableKeyCache[uint64, *block.MetaList](options)
	if err != nil {
		return BlockMetaListCache{}, err
	}
	return BlockMetaListCache{
		cache,
	}, nil
}
