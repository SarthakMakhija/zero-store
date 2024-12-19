package state

import (
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStorageOptionsWithSortedSegmentSize(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).WithFileSystemStoreType(".").Build()
	assert.Equal(t, int64(2<<10), storageOptions.sortedSegmentSizeInBytes)
}

func TestStorageOptionsWithMaximumInactiveSegments(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().WithMaximumInactiveSegments(4).WithFileSystemStoreType(".").Build()
	assert.Equal(t, uint(4), storageOptions.maximumInactiveSegments)
}

func TestStorageOptionsWithoutStoreType(t *testing.T) {
	assert.Panics(t, func() {
		NewStorageOptionsBuilder().WithMaximumInactiveSegments(4).Build()
	})
}

func TestStorageOptionsWithStoreTypeButWithEmptyRootDirectory(t *testing.T) {
	assert.Panics(t, func() {
		NewStorageOptionsBuilder().WithMaximumInactiveSegments(4).WithFileSystemStoreType("").Build()
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
