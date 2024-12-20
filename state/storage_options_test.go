package state

import (
	"github.com/SarthakMakhija/zero-store/objectstore"
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

func TestStorageOptionsWithInMemoryModeEnabled(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).WithFileSystemStoreType(".").EnableInMemoryMode().Build()
	assert.True(t, storageOptions.inMemoryMode)
}

func TestStorageOptionsWithInMemoryModeNotEnabled(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).WithFileSystemStoreType(".").Build()
	assert.False(t, storageOptions.inMemoryMode)
}

func TestStorageOptionsFlushInactiveSegmentDuration(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().
		WithSortedSegmentSizeInBytes(2 << 10).
		WithFileSystemStoreType(".").
		WithFlushInactiveSegmentDuration(4 * time.Second).
		Build()
	assert.Equal(t, 4*time.Second, storageOptions.flushInactiveSegmentDuration)
}
