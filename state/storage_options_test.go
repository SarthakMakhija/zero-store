package state

import (
	"github.com/SarthakMakhija/zero-store/cache"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/SarthakMakhija/zero-store/objectstore/filter"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStorageOptionsWithSortedSegmentSize(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).WithFileSystemStoreType(".").Build()
	assert.Equal(t, int64(2<<10), storageOptions.sortedSegmentSizeInBytes)
}

func TestStorageOptionsWithoutStoreType(t *testing.T) {
	assert.Panics(t, func() {
		NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).Build()
	})
}

func TestStorageOptionsWithStoreTypeButWithEmptyRootDirectory(t *testing.T) {
	assert.Panics(t, func() {
		NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).WithFileSystemStoreType("").Build()
	})
}

func TestStorageOptionsWithFileSystemStoreType(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).WithFileSystemStoreType(".").Build()
	assert.Equal(t, objectstore.FileSystemStore, storageOptions.storeType)
}

func TestStorageOptionsWithStoreTypeAndRootDirectory(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).WithFileSystemStoreType(".").Build()
	assert.Equal(t, ".", storageOptions.rootDirectory)
}

func TestStorageOptionsWithSortedSegmentBlockCompressionEnabled(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).WithFileSystemStoreType(".").EnableSortedSegmentBlockCompression().Build()
	assert.True(t, storageOptions.sortedSegmentBlockCompression)
}

func TestStorageOptionsWithSortedSegmentBlockCompressionNotEnabled(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).WithFileSystemStoreType(".").Build()
	assert.False(t, storageOptions.sortedSegmentBlockCompression)
}

func TestStorageOptionsFlushInactiveSegmentDuration(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().
		WithSortedSegmentSizeInBytes(2 << 10).
		WithFileSystemStoreType(".").
		WithFlushInactiveSegmentDuration(4 * time.Second).
		Build()
	assert.Equal(t, 4*time.Second, storageOptions.flushInactiveSegmentDuration)
}

func TestStorageOptionsBloomFilterCacheOptions(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().
		WithSortedSegmentSizeInBytes(2 << 10).
		WithFileSystemStoreType(".").
		WithBloomFilterCacheOptions(cache.NewComparableKeyCacheOptions[uint64, filter.BloomFilter](200, 5*time.Minute, nil)).
		Build()
	assert.Equal(t, uint(200), storageOptions.bloomFilterCacheOptions.SizeInBytes())
	assert.Equal(t, 5*time.Minute, storageOptions.bloomFilterCacheOptions.EntryTTL())
}

func TestStorageOptionsBlockMetaListCacheOptions(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().
		WithSortedSegmentSizeInBytes(2 << 10).
		WithFileSystemStoreType(".").
		WithBlockMetaListCacheOptions(cache.NewComparableKeyCacheOptions[uint64, *block.MetaList](150, 3*time.Minute, nil)).
		Build()
	assert.Equal(t, uint(150), storageOptions.blockMetaListCacheOptions.SizeInBytes())
	assert.Equal(t, 3*time.Minute, storageOptions.blockMetaListCacheOptions.EntryTTL())
}
