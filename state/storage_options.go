package state

import (
	"github.com/SarthakMakhija/zero-store/objectstore"
	"time"
)

type StorageOptions struct {
	sortedSegmentSizeInBytes      int64
	storeType                     objectstore.StoreType
	rootDirectory                 string
	sortedSegmentBlockCompression bool
	inMemoryMode                  bool
	flushInactiveSegmentDuration  time.Duration
}

type StorageOptionsBuilder struct {
	sortedSegmentSizeInBytes      int64
	storeType                     objectstore.StoreType
	rootDirectory                 string
	sortedSegmentBlockCompression bool
	inMemoryMode                  bool
	flushInactiveSegmentDuration  time.Duration
}

func NewStorageOptionsBuilder() *StorageOptionsBuilder {
	return &StorageOptionsBuilder{
		sortedSegmentSizeInBytes:      1 << 20,
		sortedSegmentBlockCompression: false,
		inMemoryMode:                  false,
		flushInactiveSegmentDuration:  60 * time.Second,
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
	}
}
