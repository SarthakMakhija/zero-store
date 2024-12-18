package state

import "github.com/SarthakMakhija/zero-store/objectstore"

type StorageOptions struct {
	sortedSegmentSizeInBytes      int64
	maximumInactiveSegments       uint
	storeType                     objectstore.StoreType
	rootDirectory                 string
	sortedSegmentBlockCompression bool
}

type StorageOptionsBuilder struct {
	sortedSegmentSizeInBytes      int64
	maximumInactiveSegments       uint
	storeType                     objectstore.StoreType
	rootDirectory                 string
	sortedSegmentBlockCompression bool
}

func NewStorageOptionsBuilder() *StorageOptionsBuilder {
	return &StorageOptionsBuilder{
		sortedSegmentSizeInBytes:      1 << 20,
		maximumInactiveSegments:       8,
		sortedSegmentBlockCompression: false,
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

func (builder *StorageOptionsBuilder) WithMaximumInactiveSegments(inactiveSegments uint) *StorageOptionsBuilder {
	builder.maximumInactiveSegments = inactiveSegments
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

func (builder *StorageOptionsBuilder) Build() StorageOptions {
	if !builder.storeType.IsValid() {
		panic("invalid store type")
	}
	if len(builder.rootDirectory) == 0 {
		panic("root directory must be specified")
	}
	return StorageOptions{
		sortedSegmentSizeInBytes:      builder.sortedSegmentSizeInBytes,
		maximumInactiveSegments:       builder.maximumInactiveSegments,
		storeType:                     builder.storeType,
		rootDirectory:                 builder.rootDirectory,
		sortedSegmentBlockCompression: builder.sortedSegmentBlockCompression,
	}
}
