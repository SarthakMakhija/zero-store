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
	assert.Nil(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix(segmentId))
	}()

	segmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	segmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	segment, err := segmentBuilder.Build(segmentId, store)
	assert.NoError(t, err)

	readBlock, err := segment.readBlock(0)
	assert.NoError(t, err)

	blockIterator := readBlock.SeekToFirst()
	defer blockIterator.Close()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, "consensus", blockIterator.Key().RawString())
	assert.Equal(t, "raft", blockIterator.Value().String())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestBuildASortedSegmentWithASingleBlockContainingMultipleKeyValues(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.Nil(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix(segmentId))
	}()

	segmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(store, false)
	segmentBuilder.Add(kv.NewStringKey("badgerDB"), kv.NewStringValue("LSM"))
	segmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	segmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("etcd"))

	segment, err := segmentBuilder.Build(segmentId, store)
	assert.NoError(t, err)

	readBlock, err := segment.readBlock(0)
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
	assert.Nil(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix(segmentId))
	}()
	segmentBuilder := NewSortedSegmentBuilder(store, 30, false)
	segmentBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	segmentBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	segment, err := segmentBuilder.Build(1, store)
	assert.Nil(t, err)

	assertBlockWithASingleKeyValue := func(blockIndex int, value kv.Value) {
		readBlock, err := segment.readBlock(blockIndex)
		assert.Nil(t, err)

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