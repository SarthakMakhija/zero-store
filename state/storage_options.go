package state

import (
	"github.com/SarthakMakhija/zero-store/cache"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/SarthakMakhija/zero-store/objectstore/filter"
	"time"
	"unsafe"
)

const (
	bloomFilterCacheSizeInBytes = 8 * 1024 * 1024
	bloomFilterCacheEntryTTL    = 5 * time.Minute

	blockMetaListCacheSizeInBytes = 16 * 1024 * 1024
	blockMetaListCacheEntryTTL    = 5 * time.Minute
)

type StorageOptions struct {
	sortedSegmentSizeInBytes      int64
	storeType                     objectstore.StoreType
	rootDirectory                 string
	sortedSegmentBlockCompression bool
	inMemoryMode                  bool
	flushInactiveSegmentDuration  time.Duration
	bloomFilterCacheOptions       cache.ComparableKeyCacheOptions[uint64, filter.BloomFilter]
	blockMetaListCacheOptions     cache.ComparableKeyCacheOptions[uint64, *block.MetaList]
}

type StorageOptionsBuilder struct {
	sortedSegmentSizeInBytes      int64
	storeType                     objectstore.StoreType
	rootDirectory                 string
	sortedSegmentBlockCompression bool
	inMemoryMode                  bool
	flushInactiveSegmentDuration  time.Duration
	bloomFilterCacheOptions       cache.ComparableKeyCacheOptions[uint64, filter.BloomFilter]
	blockMetaListCacheOptions     cache.ComparableKeyCacheOptions[uint64, *block.MetaList]
}

func NewStorageOptionsBuilder() *StorageOptionsBuilder {
	return &StorageOptionsBuilder{
		sortedSegmentSizeInBytes:      1 << 20,
		sortedSegmentBlockCompression: false,
		inMemoryMode:                  false,
		flushInactiveSegmentDuration:  60 * time.Second,
		bloomFilterCacheOptions: cache.NewComparableKeyCacheOptions[uint64, filter.BloomFilter](
			bloomFilterCacheSizeInBytes,
			bloomFilterCacheEntryTTL,
			func(id uint64, value filter.BloomFilter) uint32 {
				return uint32(unsafe.Sizeof(id) + unsafe.Sizeof(value))
			},
		),
		blockMetaListCacheOptions: cache.NewComparableKeyCacheOptions[uint64, *block.MetaList](
			blockMetaListCacheSizeInBytes,
			blockMetaListCacheEntryTTL,
			func(id uint64, value *block.MetaList) uint32 {
				return uint32(unsafe.Sizeof(id) + unsafe.Sizeof(value))
			},
		),
	}
}

func (builder *StorageOptionsBuilder) WithSortedSegmentSizeInBytes(size int64) *StorageOptionsBuilder {
	if size < 0 {
		panic("sorted segment size must be greater than 0")
	}
	//TODO: what if size is too less like 10 bytes?
	builder.sortedSegmentSizeInBytes = size
	return builder
}

func (builder *StorageOptionsBuilder) WithFileSystemStoreType(rootDirectory string) *StorageOptionsBuilder {
	builder.storeType = objectstore.FileSystemStore
	builder.rootDirectory = rootDirectory
	return builder
}

func (builder *StorageOptionsBuilder) EnableSortedSegmentBlockCompression() *StorageOptionsBuilder {
	builder.sortedSegmentBlockCompression = true
	return builder
}

func (builder *StorageOptionsBuilder) EnableInMemoryMode() *StorageOptionsBuilder {
	builder.inMemoryMode = true
	return builder
}

func (builder *StorageOptionsBuilder) WithFlushInactiveSegmentDuration(duration time.Duration) *StorageOptionsBuilder {
	builder.flushInactiveSegmentDuration = duration
	return builder
}

func (builder *StorageOptionsBuilder) WithBloomFilterCacheOptions(options cache.ComparableKeyCacheOptions[uint64, filter.BloomFilter]) *StorageOptionsBuilder {
	builder.bloomFilterCacheOptions = options
	return builder
}

func (builder *StorageOptionsBuilder) WithBlockMetaListCacheOptions(options cache.ComparableKeyCacheOptions[uint64, *block.MetaList]) *StorageOptionsBuilder {
	builder.blockMetaListCacheOptions = options
	return builder
}

func (builder *StorageOptionsBuilder) Build() StorageOptions {
	if !builder.storeType.IsValid() {
		panic("invalid store type")
	}
	if len(builder.rootDirectory) == 0 {
		panic("root directory must be specified")
	}
	return StorageOptions{
		sortedSegmentSizeInBytes:      builder.sortedSegmentSizeInBytes,
		storeType:                     builder.storeType,
		rootDirectory:                 builder.rootDirectory,
		sortedSegmentBlockCompression: builder.sortedSegmentBlockCompression,
		inMemoryMode:                  builder.inMemoryMode,
		flushInactiveSegmentDuration:  builder.flushInactiveSegmentDuration,
		bloomFilterCacheOptions:       builder.bloomFilterCacheOptions,
		blockMetaListCacheOptions:     builder.blockMetaListCacheOptions,
	}
}
