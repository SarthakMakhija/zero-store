package segment

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestLoadSortedSegmentWithSingleBlockAndCheckKeysForExistenceUsingBloom(t *testing.T) {
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

	_, err = sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	segment, err := Load(1, block.DefaultBlockSize, false, store)

	assert.NoError(t, err)
	assert.True(t, segment.MayContain(kv.NewStringKey("consensus")))
	assert.True(t, segment.MayContain(kv.NewStringKey("distributed")))
	assert.True(t, segment.MayContain(kv.NewStringKey("etcd")))
}

func TestLoadSortedSegmentWithSingleBlockAndCheckKeysForNonExistenceUsingBloom(t *testing.T) {
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

	_, err = sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	segment, err := Load(1, block.DefaultBlockSize, false, store)

	assert.NoError(t, err)
	assert.False(t, segment.MayContain(kv.NewStringKey("paxos")))
	assert.False(t, segment.MayContain(kv.NewStringKey("bolt")))
}

func TestLoadASortedSegmentWithTwoBlocksAndCheckKeysForExistenceUsingBloom(t *testing.T) {
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

	_, err = sortedSegmentBuilder.Build(segmentId)
	assert.NoError(t, err)

	segment, err := Load(1, 30, false, store)

	assert.NoError(t, err)
	assert.Equal(t, 2, segment.noOfBlocks())
	assert.True(t, segment.MayContain(kv.NewStringKey("consensus")))
	assert.True(t, segment.MayContain(kv.NewStringKey("distributed")))
	assert.False(t, segment.MayContain(kv.NewStringKey("etcd")))
}
