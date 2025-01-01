package segment

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestBuildASortedSegmentWithASingleBlockContainingSingleKeyValue(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	segmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	segment, blockMetaList, _, err := segmentBuilder.build(segmentId)
	assert.NoError(t, err)

	readBlock, err := segment.readBlock(0, blockMetaList)
	assert.NoError(t, err)

	blockIterator := readBlock.SeekToFirst()
	defer blockIterator.Close()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, "consensus", blockIterator.Key().RawString())
	assert.Equal(t, "raft", blockIterator.Value().String())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestBuildASortedSegmentWithASingleBlockWithStartingAndEndingKey(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	segmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("badgerDB", 5), kv.NewStringValue("LSM"))
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("raft"))

	segment, _, _, err := segmentBuilder.build(segmentId)
	assert.NoError(t, err)

	assert.Equal(t, kv.NewStringKeyWithTimestamp("badgerDB", 5), segment.startingKey)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 6), segment.endingKey)
}

func TestBuildASortedSegmentWithASingleBlockContainingMultipleKeyValues(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	segmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("badgerDB", 5), kv.NewStringValue("LSM"))
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("raft"))
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("etcd"))

	segment, blockMetaList, _, err := segmentBuilder.build(segmentId)
	assert.NoError(t, err)

	readBlock, err := segment.readBlock(0, blockMetaList)
	assert.NoError(t, err)

	blockIterator := readBlock.SeekToFirst()
	defer blockIterator.Close()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, "badgerDB", blockIterator.Key().RawString())
	assert.Equal(t, "LSM", blockIterator.Value().String())

	_ = blockIterator.Next()
	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, "consensus", blockIterator.Key().RawString())
	assert.Equal(t, "raft", blockIterator.Value().String())

	_ = blockIterator.Next()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, "distributed", blockIterator.Key().RawString())
	assert.Equal(t, "etcd", blockIterator.Value().String())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestBuildASortedSegmentWithTwoBlocks(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()
	segmentBuilder := newSortedSegmentBuilder(store, 50, false)
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	segmentBuilder.add(kv.NewStringKeyWithTimestamp("distributed", 10), kv.NewStringValue("TiKV"))

	segment, blockMetaList, _, err := segmentBuilder.build(segmentId)
	assert.NoError(t, err)

	assertBlockWithASingleKeyValue := func(blockIndex int, value kv.Value) {
		readBlock, err := segment.readBlock(blockIndex, blockMetaList)
		assert.NoError(t, err)

		blockIterator := readBlock.SeekToFirst()
		defer blockIterator.Close()

		assert.True(t, blockIterator.IsValid())
		assert.Equal(t, value, blockIterator.Value())

		_ = blockIterator.Next()
		assert.False(t, blockIterator.IsValid())
	}

	assertBlockWithASingleKeyValue(0, kv.NewStringValue("raft"))
	assertBlockWithASingleKeyValue(1, kv.NewStringValue("TiKV"))
}
