package segment

import (
	"github.com/SarthakMakhija/zero-store/cache"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/SarthakMakhija/zero-store/objectstore/filter"
	"github.com/stretchr/testify/assert"
	"os"
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

func TestSortedSegmentsWithASingleKeyValue(t *testing.T) {
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
			keys:   []kv.Key{kv.NewStringKey("raft")},
			values: []kv.Value{kv.NewStringValue("consensus")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	iterator, err := segments.SeekToFirst(segmentId)
	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("consensus"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestSortedSegmentsWithMultipleKeyValues(t *testing.T) {
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
			keys:   []kv.Key{kv.NewStringKey("algorithm"), kv.NewStringKey("distributed"), kv.NewStringKey("etcd")},
			values: []kv.Value{kv.NewStringValue("graph"), kv.NewStringValue("foundation"), kv.NewStringValue("key-value")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	iterator, err := segments.SeekToFirst(segmentId)
	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("graph"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("foundation"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("key-value"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestSortedSegmentsSeekToKeyWithMultipleKeyValues(t *testing.T) {
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
			keys:   []kv.Key{kv.NewStringKey("algorithm"), kv.NewStringKey("distributed"), kv.NewStringKey("etcd")},
			values: []kv.Value{kv.NewStringValue("graph"), kv.NewStringValue("foundation"), kv.NewStringValue("key-value")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	iterator, err := segments.SeekToKey(kv.NewStringKey("distributed"), segmentId)
	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("foundation"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("key-value"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestLoadASortedSegmentWithMultipleKeyValues(t *testing.T) {
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
			keys:   []kv.Key{kv.NewStringKey("algorithm"), kv.NewStringKey("distributed"), kv.NewStringKey("etcd")},
			values: []kv.Value{kv.NewStringValue("graph"), kv.NewStringValue("foundation"), kv.NewStringValue("key-value")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	_, err = segments.Load(segmentId, block.DefaultBlockSize, false)
	assert.NoError(t, err)

	iterator, err := segments.SeekToFirst(segmentId)
	assert.NoError(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("graph"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("foundation"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("key-value"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestSortedSegmentWhichMayContainAKey(t *testing.T) {
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
			keys:   []kv.Key{kv.NewStringKey("algorithm"), kv.NewStringKey("distributed"), kv.NewStringKey("etcd")},
			values: []kv.Value{kv.NewStringValue("graph"), kv.NewStringValue("foundation"), kv.NewStringValue("key-value")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	contains, err := segments.MayContain(kv.NewStringKey("algorithm"), segmentId)
	assert.NoError(t, err)
	assert.True(t, contains)

	contains, err = segments.MayContain(kv.NewStringKey("distributed"), segmentId)
	assert.NoError(t, err)
	assert.True(t, contains)

	contains, err = segments.MayContain(kv.NewStringKey("etcd"), segmentId)
	assert.NoError(t, err)
	assert.True(t, contains)
}

func TestSortedSegmentWhichMustNotContainAKey(t *testing.T) {
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
			keys:   []kv.Key{kv.NewStringKey("algorithm"), kv.NewStringKey("distributed"), kv.NewStringKey("etcd")},
			values: []kv.Value{kv.NewStringValue("graph"), kv.NewStringValue("foundation"), kv.NewStringValue("key-value")},
		},
		segmentId,
	)
	assert.NoError(t, err)

	contains, err := segments.MayContain(kv.NewStringKey("hard-disk"), segmentId)
	assert.NoError(t, err)
	assert.False(t, contains)
}

func testInstantiateSortedSegments(store objectstore.Store) (*SortedSegments, error) {
	return NewSortedSegments(store,
		NewSortedSegmentCacheOptions(
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
