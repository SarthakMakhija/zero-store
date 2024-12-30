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

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToFirst(blockMetaList)
	assert.NoError(t, err)

	defer iterator.Close()

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

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.add(kv.NewStringKey("etcd"), kv.NewStringValue("bbolt"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToFirst(blockMetaList)
	assert.NoError(t, err)

	defer iterator.Close()

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

	sortedSegmentBuilder := newSortedSegmentBuilder(store, 30, false)
	sortedSegmentBuilder.add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToFirst(blockMetaList)
	assert.NoError(t, err)

	defer iterator.Close()

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

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToKey(kv.NewStringKey("consensus"), blockMetaList)
	assert.NoError(t, err)

	defer iterator.Close()

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

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.add(kv.NewStringKey("etcd"), kv.NewStringValue("bbolt"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToKey(kv.NewStringKey("contribute"), blockMetaList)
	assert.NoError(t, err)

	defer iterator.Close()

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

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.add(kv.NewStringKey("etcd"), kv.NewStringValue("bbolt"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToKey(kv.NewStringKey("consensus"), blockMetaList)
	assert.NoError(t, err)

	defer iterator.Close()

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

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKey("cart"), kv.NewStringValue("draft"))
	sortedSegmentBuilder.add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToKey(kv.NewStringKey("consensus"), blockMetaList)
	assert.NoError(t, err)

	defer iterator.Close()

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

	sortedSegmentBuilder := newSortedSegmentBuilder(store, 30, false)
	sortedSegmentBuilder.add(kv.NewStringKey("cart"), kv.NewStringValue("draft"))
	sortedSegmentBuilder.add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToKey(kv.NewStringKey("bolt"), blockMetaList)
	assert.NoError(t, err)

	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("draft"), iterator.Value())

	_ = iterator.Next()
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
