package get_strategies

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/segment"
	"github.com/stretchr/testify/assert"
	"os"
	"slices"
	"testing"
)

func TestNonDurableAlsoGetFromActiveSegmentAndPersistentSegmentForAnExistingKey(t *testing.T) {
	activeSegment := memory.NewSortedSegment(1, 1<<10)
	activeSegment.Set(kv.NewStringKeyWithTimestamp("raft", 14), kv.NewStringValue("consensus"))

	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(segment.PathSuffixForSegment(segmentId))
	}()

	segments, err := testInstantiateSortedSegments(store)
	assert.NoError(t, err)

	persistentSegment, err := segments.BuildAndWritePersistentSortedSegment(
		&testKeyValueIterator{
			keys:   []kv.Key{kv.NewStringKeyWithTimestamp("raft", 10)},
			values: []kv.Value{kv.NewStringValue("another consensus")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	getOperation := NewNonDurableAlsoGet(
		NewNonDurableOnlyGet(activeSegment, nil),
		NewDurableOnlyGet(segments, slices.Backward([]segment.SortedSegment{persistentSegment})),
	)

	getResponse := getOperation.Get(kv.NewStringKeyWithTimestamp("raft", 15))
	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("consensus"), getResponse.Value())

	getResponse = getOperation.Get(kv.NewStringKeyWithTimestamp("raft", 11))
	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("another consensus"), getResponse.Value())
}

func TestNonDurableAlsoGetFromActiveSegmentAndPersistentSegmentForANonExistingKey(t *testing.T) {
	activeSegment := memory.NewSortedSegment(1, 1<<10)
	activeSegment.Set(kv.NewStringKeyWithTimestamp("raft", 14), kv.NewStringValue("consensus"))

	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	segmentId := uint64(1)

	defer func() {
		store.Close()
		_ = os.Remove(segment.PathSuffixForSegment(segmentId))
	}()

	segments, err := testInstantiateSortedSegments(store)
	assert.NoError(t, err)

	persistentSegment, err := segments.BuildAndWritePersistentSortedSegment(
		&testKeyValueIterator{
			keys:   []kv.Key{kv.NewStringKeyWithTimestamp("raft", 10)},
			values: []kv.Value{kv.NewStringValue("another consensus")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	getOperation := NewNonDurableAlsoGet(
		NewNonDurableOnlyGet(activeSegment, nil),
		NewDurableOnlyGet(segments, slices.Backward([]segment.SortedSegment{persistentSegment})),
	)

	getResponse := getOperation.Get(kv.NewStringKeyWithTimestamp("non-existing", 15))
	assert.False(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.EmptyValue, getResponse.Value())
}
