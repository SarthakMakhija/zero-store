package segment

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
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

	ssTable, err := sortedSegmentBuilder.Build(segmentId, store)
	assert.Nil(t, err)

	assert.Equal(t, 2, ssTable.noOfBlocks())
}
