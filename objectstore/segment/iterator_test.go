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
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))

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
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 5), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("etcd", 5), kv.NewStringValue("bbolt"))

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

	sortedSegmentBuilder := newSortedSegmentBuilder(store, 50, false)
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 8), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 9), kv.NewStringValue("TiKV"))

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
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToKey(kv.NewStringKeyWithTimestamp("consensus", 6), blockMetaList)
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
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToKey(kv.NewStringKeyWithTimestamp("contribute", 9), blockMetaList)
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
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToKey(kv.NewStringKeyWithTimestamp("consensus", 6), blockMetaList)
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
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("cart", 5), kv.NewStringValue("draft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToKey(kv.NewStringKeyWithTimestamp("consensus", 10), blockMetaList)
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

	sortedSegmentBuilder := newSortedSegmentBuilder(store, 50, false)
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("cart", 9), kv.NewStringValue("draft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 10), kv.NewStringValue("TiKV"))

	segment, blockMetaList, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	iterator, err := segment.seekToKey(kv.NewStringKeyWithTimestamp("bolt", 11), blockMetaList)
	assert.NoError(t, err)

	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("draft"), iterator.Value())

	_ = iterator.Next()
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
