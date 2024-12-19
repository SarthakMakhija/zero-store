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
	assert.Nil(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	segment, err := sortedSegmentBuilder.Build(segmentId, store)
	assert.Nil(t, err)

	block, err := segment.readBlock(0)
	assert.Nil(t, err)

	blockIterator := block.SeekToFirst()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), blockIterator.Value())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestSortedSegmentWithATwoBlocks(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.Nil(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilder(store, 30, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	segment, err := sortedSegmentBuilder.Build(segmentId, store)
	assert.Nil(t, err)

	assert.Equal(t, 2, segment.noOfBlocks())
}

func TestLoadSortedSegmentWithSingleBlockContainingMultipleKeyValuePairs(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.Nil(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix(segmentId))
	}()

	segmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	segmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	segmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))
	segmentBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("bbolt"))

	_, err = segmentBuilder.Build(segmentId, store)
	assert.Nil(t, err)

	segment, err := Load(segmentId, block.DefaultBlockSize, false, store)
	assert.Nil(t, err)

	iterator, err := segment.SeekToFirst()
	assert.Nil(t, err)

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
	assert.Nil(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix(segmentId))
	}()

	segmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	segmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	segmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))
	segmentBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("bbolt"))

	_, err = segmentBuilder.Build(segmentId, store)
	assert.Nil(t, err)

	segment, err := Load(1, block.DefaultBlockSize, false, store)
	assert.Nil(t, err)
	assert.Equal(t, "consensus", segment.startingKey.RawString())
	assert.Equal(t, "etcd", segment.endingKey.RawString())
}

func TestLoadASortedSegmentWithTwoBlocks(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.Nil(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilder(store, 30, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	_, err = sortedSegmentBuilder.Build(segmentId, store)
	assert.Nil(t, err)

	segment, err := Load(1, 30, false, store)
	assert.Nil(t, err)

	iterator, err := segment.SeekToFirst()
	assert.Nil(t, err)

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
	assert.Nil(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix(segmentId))
	}()

	sortedSegmentBuilder := NewSortedSegmentBuilder(store, 30, false)
	sortedSegmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	_, err = sortedSegmentBuilder.Build(segmentId, store)
	assert.Nil(t, err)

	segment, err := Load(1, 30, false, store)
	assert.Nil(t, err)
	assert.Equal(t, "consensus", segment.startingKey.RawString())
	assert.Equal(t, "distributed", segment.endingKey.RawString())
}
