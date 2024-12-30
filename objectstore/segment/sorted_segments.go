package segment

import (
	"errors"
	"github.com/SarthakMakhija/zero-store/cache"
	"github.com/SarthakMakhija/zero-store/iterator"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/SarthakMakhija/zero-store/objectstore/filter"
)

var ErrNoSegmentForTheSegmentId = errors.New("no segment for this id")

type SortedSegments struct {
	persistentSegments map[uint64]*SortedSegment
	store              objectstore.Store
	bloomFilterCache   cache.BloomFilterCache
	blockMetaListCache cache.BlockMetaListCache
	enableCompression  bool
}

func NewSortedSegments(store objectstore.Store, options SortedSegmentCacheOptions, enableCompression bool) (*SortedSegments, error) {
	bloomFilterCache, err := cache.NewBloomFilterCache(options.bloomFilterCacheOptions)
	if err != nil {
		return nil, err
	}
	blockMetaListCache, err := cache.NewBlockMetaListCache(options.blockMetaListCacheOptions)
	if err != nil {
		return nil, err
	}
	return &SortedSegments{
		persistentSegments: make(map[uint64]*SortedSegment),
		store:              store,
		bloomFilterCache:   bloomFilterCache,
		blockMetaListCache: blockMetaListCache,
		enableCompression:  enableCompression,
	}, nil
}

func (sortedSegments *SortedSegments) BuildAndWritePersistentSortedSegment(iterator iterator.Iterator, segmentId uint64) (*SortedSegment, error) {
	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(sortedSegments.store, sortedSegments.enableCompression)
	for iterator.IsValid() {
		sortedSegmentBuilder.add(iterator.Key(), iterator.Value())
		if err := iterator.Next(); err != nil {
			return nil, err
		}
	}
	persistentSortedSegment, blockMetaList, bloomFilter, err := sortedSegmentBuilder.build(segmentId)
	if err != nil {
		return nil, err
	}
	sortedSegments.updateState(segmentId, persistentSortedSegment, bloomFilter, blockMetaList)
	return persistentSortedSegment, nil
}

func (sortedSegments *SortedSegments) Load(segmentId uint64, blockSize uint, enableCompression bool) (*SortedSegment, error) {
	sortedSegment, ok := sortedSegments.persistentSegments[segmentId]
	if ok {
		return sortedSegment, nil
	}
	sortedSegment, blockMetaList, bloomFilter, err := load(segmentId, blockSize, enableCompression, sortedSegments.store)
	if err != nil {
		return nil, err
	}
	sortedSegments.updateState(segmentId, sortedSegment, bloomFilter, blockMetaList)
	return sortedSegment, nil
}

func (sortedSegments *SortedSegments) SeekToFirst(segmentId uint64) (*Iterator, error) {
	sortedSegment, ok := sortedSegments.persistentSegments[segmentId]
	if !ok {
		return nil, ErrNoSegmentForTheSegmentId
	}
	blockMetaList, err := sortedSegments.getOrFetchBlockMetaList(sortedSegment)
	if err != nil {
		return nil, err
	}
	return sortedSegment.seekToFirst(blockMetaList)
}

func (sortedSegments *SortedSegments) SeekToKey(key kv.Key, segmentId uint64) (*Iterator, error) {
	sortedSegment, ok := sortedSegments.persistentSegments[segmentId]
	if !ok {
		return nil, ErrNoSegmentForTheSegmentId
	}
	blockMetaList, err := sortedSegments.getOrFetchBlockMetaList(sortedSegment)
	if err != nil {
		return nil, err
	}
	return sortedSegment.seekToKey(key, blockMetaList)
}

func (sortedSegments *SortedSegments) MayContain(key kv.Key, segmentId uint64) (bool, error) {
	sortedSegment, ok := sortedSegments.persistentSegments[segmentId]
	if !ok {
		return false, ErrNoSegmentForTheSegmentId
	}
	bloomFilter, err := sortedSegments.getOrFetchBloomFilter(sortedSegment)
	if err != nil {
		return false, err
	}
	return sortedSegment.mayContain(key, bloomFilter), nil
}

func (sortedSegments *SortedSegments) getOrFetchBlockMetaList(sortedSegment *SortedSegment) (*block.MetaList, error) {
	blockMetaList, ok := sortedSegments.blockMetaListCache.Get(sortedSegment.id)
	if !ok {
		blockMetaList, err := loadBlockMetaList(sortedSegment.id, sortedSegment.footerBlock, sortedSegments.enableCompression, sortedSegments.store)
		if err != nil {
			return nil, err
		}
		sortedSegments.blockMetaListCache.Set(sortedSegment.id, blockMetaList)
		return blockMetaList, nil
	}
	return blockMetaList, nil
}

func (sortedSegments *SortedSegments) getOrFetchBloomFilter(sortedSegment *SortedSegment) (filter.BloomFilter, error) {
	bloomFilter, ok := sortedSegments.bloomFilterCache.Get(sortedSegment.id)
	if !ok {
		bloomFilter, err := loadBloomFilter(sortedSegment.id, sortedSegment.footerBlock, sortedSegments.store)
		if err != nil {
			return filter.BloomFilter{}, err
		}
		sortedSegments.bloomFilterCache.Set(sortedSegment.id, bloomFilter)
		return bloomFilter, nil
	}
	return bloomFilter, nil
}

func (sortedSegments *SortedSegments) updateState(segmentId uint64, persistentSortedSegment *SortedSegment, bloomFilter filter.BloomFilter, blockMetaList *block.MetaList) {
	sortedSegments.persistentSegments[segmentId] = persistentSortedSegment
	sortedSegments.bloomFilterCache.Set(segmentId, bloomFilter)
	sortedSegments.blockMetaListCache.Set(segmentId, blockMetaList)
}
