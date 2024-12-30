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

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.add(kv.NewStringKey("etcd"), kv.NewStringValue("bbolt"))

	_, _, _, err = sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	segment, _, bloomFilter, err := load(1, block.DefaultBlockSize, false, store)

	assert.NoError(t, err)
	assert.True(t, segment.mayContain(kv.NewStringKey("consensus"), bloomFilter))
	assert.True(t, segment.mayContain(kv.NewStringKey("distributed"), bloomFilter))
	assert.True(t, segment.mayContain(kv.NewStringKey("etcd"), bloomFilter))
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

	sortedSegmentBuilder := newSortedSegmentBuilderWithDefaultBlockSize(store, false)
	sortedSegmentBuilder.add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))
	sortedSegmentBuilder.add(kv.NewStringKey("etcd"), kv.NewStringValue("bbolt"))

	_, _, _, err = sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	segment, _, bloomFilter, err := load(1, block.DefaultBlockSize, false, store)

	assert.NoError(t, err)
	assert.False(t, segment.mayContain(kv.NewStringKey("paxos"), bloomFilter))
	assert.False(t, segment.mayContain(kv.NewStringKey("bolt"), bloomFilter))
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

	sortedSegmentBuilder := newSortedSegmentBuilder(store, 30, false)
	sortedSegmentBuilder.add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegmentBuilder.add(kv.NewStringKey("distributed"), kv.NewStringValue("TiKV"))

	_, _, _, err = sortedSegmentBuilder.build(segmentId)
	assert.NoError(t, err)

	segment, _, bloomFilter, err := load(1, 30, false, store)

	assert.NoError(t, err)
	assert.Equal(t, 2, segment.noOfBlocks())
	assert.True(t, segment.mayContain(kv.NewStringKey("consensus"), bloomFilter))
	assert.True(t, segment.mayContain(kv.NewStringKey("distributed"), bloomFilter))
	assert.False(t, segment.mayContain(kv.NewStringKey("etcd"), bloomFilter))
}
