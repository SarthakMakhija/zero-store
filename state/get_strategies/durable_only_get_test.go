package get_strategies

import (
	"github.com/SarthakMakhija/zero-store/cache"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/SarthakMakhija/zero-store/objectstore/filter"
	"github.com/SarthakMakhija/zero-store/objectstore/segment"
	"github.com/stretchr/testify/assert"
	"os"
	"slices"
	"testing"
	"time"
	"unsafe"
)

type testKeyValueIterator struct {
	keys   []kv.Key
	values []kv.Value
	index  int
}

func (iterator *testKeyValueIterator) Key() kv.Key {
	return iterator.keys[iterator.index]
}

func (iterator *testKeyValueIterator) Value() kv.Value {
	return iterator.values[iterator.index]
}

func (iterator *testKeyValueIterator) Next() error {
	iterator.index += 1
	return nil
}

func (iterator *testKeyValueIterator) IsValid() bool {
	return iterator.index < len(iterator.keys)
}

func (iterator *testKeyValueIterator) Close() {
}

func TestDurableOnlyGetWithASingleSegmentContainingTheKey(t *testing.T) {
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

	aSegment, err := segments.BuildAndWritePersistentSortedSegment(
		&testKeyValueIterator{
			keys:   []kv.Key{kv.NewStringKeyWithTimestamp("raft", 10)},
			values: []kv.Value{kv.NewStringValue("consensus")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	getOperation := newDurableOnlyGet(segments, slices.Backward([]*segment.SortedSegment{aSegment}))
	getResponse := getOperation.get(kv.NewStringKeyWithTimestamp("raft", 11))

	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("consensus"), getResponse.Value())
}

func TestDurableOnlyGetWithASingleSegmentNotContainingTheKey(t *testing.T) {
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

	aSegment, err := segments.BuildAndWritePersistentSortedSegment(
		&testKeyValueIterator{
			keys:   []kv.Key{kv.NewStringKeyWithTimestamp("raft", 10)},
			values: []kv.Value{kv.NewStringValue("consensus")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	getOperation := newDurableOnlyGet(segments, slices.Backward([]*segment.SortedSegment{aSegment}))
	getResponse := getOperation.get(kv.NewStringKeyWithTimestamp("paxos", 11))

	assert.False(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.EmptyValue, getResponse.Value())
}

func TestDurableOnlyGetWithMultipleSegmentsOneOfWhichContainsTheKey(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	aSegmentId := uint64(1)
	anotherSegmentId := uint64(2)

	defer func() {
		store.Close()
		_ = os.Remove(segment.PathSuffixForSegment(aSegmentId))
		_ = os.Remove(segment.PathSuffixForSegment(anotherSegmentId))
	}()

	segments, err := testInstantiateSortedSegments(store)
	assert.NoError(t, err)

	aSegment, err := segments.BuildAndWritePersistentSortedSegment(
		&testKeyValueIterator{
			keys:   []kv.Key{kv.NewStringKeyWithTimestamp("raft", 10)},
			values: []kv.Value{kv.NewStringValue("consensus")},
		},
		aSegmentId,
	)
	assert.NoError(t, err)

	anotherSegment, err := segments.BuildAndWritePersistentSortedSegment(
		&testKeyValueIterator{
			keys:   []kv.Key{kv.NewStringKeyWithTimestamp("paxos", 15)},
			values: []kv.Value{kv.NewStringValue("another consensus")},
		},
		anotherSegmentId,
	)
	assert.NoError(t, err)

	getOperation := newDurableOnlyGet(segments, slices.Backward([]*segment.SortedSegment{aSegment, anotherSegment}))
	getResponse := getOperation.get(kv.NewStringKeyWithTimestamp("paxos", 18))

	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("another consensus"), getResponse.Value())
}

func TestDurableOnlyGetWithMultipleSegmentsManyOfWhichContainTheKey(t *testing.T) {
	storeDefinition, err := objectstore.NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := objectstore.NewStore(".", storeDefinition)
	aSegmentId := uint64(1)
	anotherSegmentId := uint64(2)

	defer func() {
		store.Close()
		_ = os.Remove(segment.PathSuffixForSegment(aSegmentId))
		_ = os.Remove(segment.PathSuffixForSegment(anotherSegmentId))
	}()

	segments, err := testInstantiateSortedSegments(store)
	assert.NoError(t, err)

	aSegment, err := segments.BuildAndWritePersistentSortedSegment(
		&testKeyValueIterator{
			keys:   []kv.Key{kv.NewStringKeyWithTimestamp("raft", 10)},
			values: []kv.Value{kv.NewStringValue("consensus")},
		},
		aSegmentId,
	)
	assert.NoError(t, err)

	anotherSegment, err := segments.BuildAndWritePersistentSortedSegment(
		&testKeyValueIterator{
			keys:   []kv.Key{kv.NewStringKeyWithTimestamp("raft", 15)},
			values: []kv.Value{kv.NewStringValue("another consensus")},
		},
		anotherSegmentId,
	)
	assert.NoError(t, err)

	getOperation := newDurableOnlyGet(segments, slices.Backward([]*segment.SortedSegment{anotherSegment, aSegment}))

	getResponse := getOperation.get(kv.NewStringKeyWithTimestamp("raft", 16))
	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("another consensus"), getResponse.Value())

	getResponse = getOperation.get(kv.NewStringKeyWithTimestamp("raft", 14))
	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("consensus"), getResponse.Value())
}

func testInstantiateSortedSegments(store objectstore.Store) (*segment.SortedSegments, error) {
	return segment.NewSortedSegments(store,
		segment.NewSortedSegmentCacheOptions(
			cache.NewComparableKeyCacheOptions[uint64, filter.BloomFilter](
				1000,
				5*time.Minute,
				func(id uint64, value filter.BloomFilter) uint32 {
					return uint32(unsafe.Sizeof(id) + unsafe.Sizeof(value))
				}),
			cache.NewComparableKeyCacheOptions[uint64, *block.MetaList](
				1000,
				5*time.Minute,
				func(id uint64, value *block.MetaList) uint32 {
					return uint32(unsafe.Sizeof(id) + unsafe.Sizeof(value))
				},
			)), false,
	)
}
