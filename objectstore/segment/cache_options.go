package segment

import (
	"github.com/SarthakMakhija/zero-store/cache"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/SarthakMakhija/zero-store/objectstore/filter"
)

type SortedSegmentCacheOptions struct {
	bloomFilterCacheOptions   cache.ComparableKeyCacheOptions[uint64, filter.BloomFilter]
	blockMetaListCacheOptions cache.ComparableKeyCacheOptions[uint64, *block.MetaList]
}

func NewSortedSegmentCacheOptions(
	bloomFilterCacheOptions cache.ComparableKeyCacheOptions[uint64, filter.BloomFilter],
	blockMetaListCacheOptions cache.ComparableKeyCacheOptions[uint64, *block.MetaList]) SortedSegmentCacheOptions {
	return SortedSegmentCacheOptions{
		bloomFilterCacheOptions:   bloomFilterCacheOptions,
		blockMetaListCacheOptions: blockMetaListCacheOptions,
	}
}
