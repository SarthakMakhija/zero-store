package segment

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestIterateOverASortedSegmentWithASingleBlockContainingSingleKeyValue(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	segment, err := sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.SeekToFirst()
	defer iterator.Close()

	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverASortedSegmentWithASingleBlockContainingMultipleKeyValues(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("bbolt"))

	segment, err := sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.SeekToFirst()
	defer iterator.Close()

	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverASortedSegmentWithTwoBlocks(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilder(store, 30, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	segment, err := sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.SeekToFirst()
	defer iterator.Close()

	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverASortedSegmentWithASingleBlockContainingSingleKeyValueUsingSeekToKeyEqualToTheGivenKey(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	segment, err := sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.SeekToKey(kv.NewStringKey("consensus"))
	defer iterator.Close()

	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverAnSortedSegmentWithASingleBlockContainingMultipleKeyValuesUsingSeekToKey(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("bbolt"))

	segment, err := sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.SeekToKey(kv.NewStringKey("contribute"))
	defer iterator.Close()

	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverASortedSegmentWithASingleBlockContainingMultipleKeyValuesUsingSeekToKeyContainingTheKey(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("bbolt"))

	segment, err := sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.SeekToKey(kv.NewStringKey("consensus"))
	defer iterator.Close()

	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverASortedSegmentWithTwoBlocksUsingSeekToKey(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("cart"), kv.NewStringValue("draft"))
	sortedSegmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	segment, err := sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.SeekToKey(kv.NewStringKey("consensus"))
	defer iterator.Close()

	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverASortedSegmentWithTwoBlocksUsingSeekToKeyWithTheKeyLessThanTheFirstKeyOfTheFirstBlock(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilder(store, 30, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("cart"), kv.NewStringValue("draft"))
	sortedSegmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	segment, err := sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.SeekToKey(kv.NewStringKey("bolt"))
	defer iterator.Close()

	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("draft"), iterator.Value())

	_ = iterator.Next()
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
