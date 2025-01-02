package segment

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestSortedSegmentWithASingleBlockContainingSingleKeyValue(t *testing.T) {
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

	readBlock, err := segment.readBlock(0, blockMetaList)
	assert.NoError(t, err)

	blockIterator := readBlock.SeekToFirst()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), blockIterator.Value())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestSortedSegmentWithATwoBlocks(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := newSortedSegmentBuilder(store, 50, false)
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 20), kv.NewStringValue("TiKV"))

	segment, _, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	assert.Equal(t, 2, segment.noOfBlocks())
}

func TestLoadSortedSegmentWithSingleBlockContainingMultipleKeyValuePairs(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	segmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 4), kv.NewStringValue("TiKV"))
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("etcd", 4), kv.NewStringValue("bbolt"))

	_, _, _, err = segmentBuilder.build(segmentId)
	assert.NoError(t, err)

	segment, blockMetaList, _, err := load(segmentId, block.DefaultBlockSize, false, store)
	assert.NoError(t, err)

	iterator, err := segment.seekToFirst(blockMetaList)
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

func TestLoadSortedSegmentWithSingleBlockContainingMultipleKeyValuePairsWithValidationOfStartingAndEndingKey(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	segmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 20), kv.NewStringValue("TiKV"))
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("etcd", 30), kv.NewStringValue("bbolt"))

	_, _, _, err = segmentBuilder.build(segmentId)
	assert.NoError(t, err)

	segment, _, _, err := load(1, block.DefaultBlockSize, false, store)
	assert.NoError(t, err)
	assert.Equal(t, "consensus", segment.startingKey.RawString())
	assert.Equal(t, "etcd", segment.endingKey.RawString())
}

func TestLoadASortedSegmentWithTwoBlocks(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := newSortedSegmentBuilder(store, 50, false)
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 30), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 40), kv.NewStringValue("TiKV"))

	_, _, _, err = sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	segment, blockMetaList, _, err := load(1, 50, false, store)
	assert.NoError(t, err)

	iterator, err := segment.seekToFirst(blockMetaList)
	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestLoadASortedSegmentWithTwoBlocksWithValidationOfStartingAndEndingKey(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := newSortedSegmentBuilder(store, 50, false)
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 30), kv.NewStringValue("TiKV"))

	_, _, _, err = sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	segment, _, _, err := load(1, 50, false, store)
	assert.NoError(t, err)
	assert.Equal(t, "consensus", segment.startingKey.RawString())
	assert.Equal(t, "distributed", segment.endingKey.RawString())
}

func TestSortedSegmentContainsTheKeyInItsRange(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("etcd", 30), kv.NewStringValue("TiKV"))

	segment, _, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	assert.True(t, segment.containsInItsRange(kv.NewStringKeyWithTimestamp("distributed", 32)))
}

func TestSortedSegmentDoesNotContainTheKeyInItsRangeGivenTheKeyIsSmallerThanTheStartingKeyOfTheSegment(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("etcd", 30), kv.NewStringValue("TiKV"))

	segment, _, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	assert.False(t, segment.containsInItsRange(kv.NewStringKeyWithTimestamp("alphabet", 32)))
}

func TestSortedSegmentDoesNotContainTheKeyInItsRangeGivenTheKeyIsGreaterThanTheEndingKeyOfTheSegment(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKeyWithTimestamp("etcd", 30), kv.NewStringValue("TiKV"))

	segment, _, _, err := sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	assert.False(t, segment.containsInItsRange(kv.NewStringKeyWithTimestamp("foundation", 32)))
}
