package segment

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestSortedSegmentsWithSingleSegmentAndCheckKeysForExistence(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	segments, err := testInstantiateSortedSegments(store)
	assert.NoError(t, err)

	_, err = segments.BuildAndWritePersistentSortedSegment(
		&testKeyValueIterator{
			keys:   []kv.Key{kv.NewStringKeyWithTimestamp("algorithm", 10), kv.NewStringKeyWithTimestamp("distributed", 10), kv.NewStringKeyWithTimestamp("etcd", 10)},
			values: []kv.Value{kv.NewStringValue("graph"), kv.NewStringValue("foundation"), kv.NewStringValue("key-value")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	contain, _ := segments.MayContain(kv.NewStringKeyWithTimestamp("algorithm", 11), segmentId)
	assert.True(t, contain)

	contain, _ = segments.MayContain(kv.NewStringKeyWithTimestamp("distributed", 11), segmentId)
	assert.True(t, contain)

	contain, _ = segments.MayContain(kv.NewStringKeyWithTimestamp("etcd", 11), segmentId)
	assert.True(t, contain)
}

func TestLoadSortedSegmentsWithSingleSegmentCheckKeysForNonExistence(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(PathSuffixForSegment(segmentId))
	}()

	segments, err := testInstantiateSortedSegments(store)
	assert.NoError(t, err)

	_, err = segments.BuildAndWritePersistentSortedSegment(
		&testKeyValueIterator{
			keys:   []kv.Key{kv.NewStringKeyWithTimestamp("algorithm", 10), kv.NewStringKeyWithTimestamp("distributed", 10), kv.NewStringKeyWithTimestamp("etcd", 10)},
			values: []kv.Value{kv.NewStringValue("graph"), kv.NewStringValue("foundation"), kv.NewStringValue("key-value")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	_, err = segments.Load(segmentId, block.DefaultBlockSize, false)
	assert.NoError(t, err)

	contain, _ := segments.MayContain(kv.NewStringKeyWithTimestamp("algorithm", 10), segmentId)
	assert.True(t, contain)

	contain, _ = segments.MayContain(kv.NewStringKeyWithTimestamp("paxos", 10), segmentId)
	assert.False(t, contain)
}
