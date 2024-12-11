package memory

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

const testSortedSegmentSize = 1 << 10

func TestEmptySortedSegment(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSize)
	assert.True(t, sortedSegment.IsEmpty())
}

func TestSortedSegmentWithASingleKey(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSize)
	sortedSegment.Set(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	value, ok := sortedSegment.Get(kv.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestSortedSegmentWithANonExistentKey(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSize)

	value, ok := sortedSegment.Get(kv.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestSortedSegmentWithMultipleKeys(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSize)
	sortedSegment.Set(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegment.Set(kv.NewStringKey("storage"), kv.NewStringValue("NVMe"))

	value, ok := sortedSegment.Get(kv.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = sortedSegment.Get(kv.NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("NVMe"), value)
}

func TestSortedSegmentWithADelete(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSize)

	sortedSegment.Set(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegment.Delete(kv.NewStringKey("consensus"))

	value, ok := sortedSegment.Get(kv.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestSortedSegmentAllEntries(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSize)
	sortedSegment.Set(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	sortedSegment.Set(kv.NewStringKey("bolt"), kv.NewStringValue("kv"))
	sortedSegment.Set(kv.NewStringKey("etcd"), kv.NewStringValue("distributed"))

	var keys []kv.Key
	var values []kv.Value
	sortedSegment.AllEntries(func(key kv.Key, value kv.Value) {
		keys = append(keys, key)
		values = append(values, value)
	})

	assert.Equal(t, []kv.Key{
		kv.NewStringKey("bolt"),
		kv.NewStringKey("consensus"),
		kv.NewStringKey("etcd"),
	}, keys)

	assert.Equal(t, []kv.Value{
		kv.NewStringValue("kv"),
		kv.NewStringValue("raft"),
		kv.NewStringValue("distributed"),
	}, values)
}

//TODO: add tests for keys with timestamps
