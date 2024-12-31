package memory

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

const testSortedSegmentSizeInBytes = 1 << 10

func TestEmptySortedSegment(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSizeInBytes)
	assert.True(t, sortedSegment.IsEmpty())
}

func TestSortedSegmentWithASingleKey(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSizeInBytes)
	sortedSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	value, ok := sortedSegment.Get(kv.NewStringKeyWithTimestamp("consensus", 5))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestSortedSegmentWithASingleKeyIncludingTimestampWhichReturnsTheValueOfTheKeyWithTimestampLessThanOrEqualToTheGiven(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSizeInBytes)
	sortedSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))

	value, ok := sortedSegment.Get(kv.NewStringKeyWithTimestamp("consensus", 5))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestSortedSegmentWithASingleKeyIncludingTimestampDoesNotReturnTheValueOfTheKeyWithTimestampLessThanOrEqualToTheGiven(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSizeInBytes)
	sortedSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))

	_, ok := sortedSegment.Get(kv.NewStringKeyWithTimestamp("consensus", 2))
	assert.False(t, ok)
}

func TestSortedSegmentWithANonExistentKey(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSizeInBytes)

	value, ok := sortedSegment.Get(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestSortedSegmentWithMultipleKeys(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSizeInBytes)
	sortedSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	sortedSegment.Set(kv.NewStringKeyWithTimestamp("storage", 5), kv.NewStringValue("NVMe"))

	value, ok := sortedSegment.Get(kv.NewStringKeyWithTimestamp("consensus", 5))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = sortedSegment.Get(kv.NewStringKeyWithTimestamp("storage", 5))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("NVMe"), value)
}

func TestSortedSegmentWithADelete(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSizeInBytes)

	sortedSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	sortedSegment.Delete(kv.NewStringKeyWithTimestamp("consensus", 6))

	value, ok := sortedSegment.Get(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestSortedSegmentWithADeleteAndAGetWithTimestampHigherThanThatOfTheKeyInMemtable(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSizeInBytes)
	sortedSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	sortedSegment.Delete(kv.NewStringKeyWithTimestamp("consensus", 6))

	value, ok := sortedSegment.Get(kv.NewStringKeyWithTimestamp("consensus", 7))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestSortedSegmentHasEnoughSpaceToFitTheRequiredSize(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSizeInBytes)
	assert.True(t, sortedSegment.CanFit(500))
}

func TestSortedSegmentDoesNotHaveEnoughSpaceToFitTheRequiredSize(t *testing.T) {
	sortedSegmentSizeInBytes := int64(200)
	sortedSegment := NewSortedSegment(1, sortedSegmentSizeInBytes)

	sortedSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	assert.False(t, sortedSegment.CanFit(20))
}

func TestSortedSegmentAllEntriesIterator(t *testing.T) {
	sortedSegment := NewSortedSegment(1, testSortedSegmentSizeInBytes)
	sortedSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 1), kv.NewStringValue("raft"))
	sortedSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 2), kv.NewStringValue("paxos"))
	sortedSegment.Set(kv.NewStringKeyWithTimestamp("bolt", 3), kv.NewStringValue("kv"))
	sortedSegment.Set(kv.NewStringKeyWithTimestamp("etcd", 4), kv.NewStringValue("distributed"))

	iterator := NewAllEntriesSortedSegmentIterator(sortedSegment)
	assert.Equal(t, "bolt", iterator.Key().RawString())
	assert.Equal(t, "kv", iterator.Value().String())

	assert.NoError(t, iterator.Next())

	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "paxos", iterator.Value().String())

	assert.NoError(t, iterator.Next())

	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "raft", iterator.Value().String())

	assert.NoError(t, iterator.Next())

	assert.Equal(t, "etcd", iterator.Key().RawString())
	assert.Equal(t, "distributed", iterator.Value().String())

	assert.NoError(t, iterator.Next())
	assert.False(t, iterator.IsValid())
}
