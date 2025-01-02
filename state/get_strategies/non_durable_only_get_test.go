package get_strategies

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory"
	"github.com/stretchr/testify/assert"
	"slices"
	"testing"
)

func TestNonDurableOnlyGetFromActiveSegment(t *testing.T) {
	activeSegment := memory.NewSortedSegment(1, 1<<10)
	activeSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 1), kv.NewStringValue("raft"))

	getOperation := newNonDurableOnlyGet(activeSegment, nil)
	value, ok := getOperation.get(kv.NewStringKeyWithTimestamp("consensus", 2))

	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestNonDurableOnlyGetForANonExistingKeyFromActiveSegment(t *testing.T) {
	activeSegment := memory.NewSortedSegment(1, 1<<10)
	activeSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 1), kv.NewStringValue("raft"))

	getOperation := newNonDurableOnlyGet(activeSegment, nil)
	value, ok := getOperation.get(kv.NewStringKeyWithTimestamp("non-existing", 2))

	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestNonDurableOnlyGetFromActiveAndSingleInactiveSegment(t *testing.T) {
	activeSegment := memory.NewSortedSegment(1, 1<<10)
	activeSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 9), kv.NewStringValue("raft"))

	inactiveSegment := memory.NewSortedSegment(2, 1<<10)
	inactiveSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("raft"))
	inactiveSegment.Set(kv.NewStringKeyWithTimestamp("distributed", 5), kv.NewStringValue("etcd"))

	inactiveSegments := []*memory.SortedSegment{inactiveSegment}
	getOperation := newNonDurableOnlyGet(activeSegment, slices.Backward(inactiveSegments))
	value, ok := getOperation.get(kv.NewStringKeyWithTimestamp("consensus", 10))

	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestNonDurableOnlyGetFromActiveAndACoupleOfInactiveSegmentsWithGetForAKeyWithOldTimestamp(t *testing.T) {
	activeSegment := memory.NewSortedSegment(1, 1<<10)
	activeSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 9), kv.NewStringValue("VSR"))

	freshInactiveSegment := memory.NewSortedSegment(2, 1<<10)
	freshInactiveSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 8), kv.NewStringValue("raft"))
	freshInactiveSegment.Set(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("etcd"))

	oldInactiveSegment := memory.NewSortedSegment(3, 1<<10)
	oldInactiveSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("paxos"))
	oldInactiveSegment.Set(kv.NewStringKeyWithTimestamp("distributed", 4), kv.NewStringValue("foundation"))

	inactiveSegments := []*memory.SortedSegment{oldInactiveSegment, freshInactiveSegment}
	getOperation := newNonDurableOnlyGet(activeSegment, slices.Backward(inactiveSegments))
	value, ok := getOperation.get(kv.NewStringKeyWithTimestamp("consensus", 7))

	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("paxos"), value)
}

func TestNonDurableOnlyGetFromActiveAndACoupleOfInactiveSegmentsWithGetForAKeyWithNearlyLatestTimestamp(t *testing.T) {
	activeSegment := memory.NewSortedSegment(1, 1<<10)
	activeSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 9), kv.NewStringValue("VSR"))

	freshInactiveSegment := memory.NewSortedSegment(2, 1<<10)
	freshInactiveSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("raft"))
	freshInactiveSegment.Set(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("etcd"))

	oldInactiveSegment := memory.NewSortedSegment(3, 1<<10)
	oldInactiveSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("paxos"))
	oldInactiveSegment.Set(kv.NewStringKeyWithTimestamp("distributed", 4), kv.NewStringValue("foundation"))

	inactiveSegments := []*memory.SortedSegment{oldInactiveSegment, freshInactiveSegment}
	getOperation := newNonDurableOnlyGet(activeSegment, slices.Backward(inactiveSegments))
	value, ok := getOperation.get(kv.NewStringKeyWithTimestamp("consensus", 8))

	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}
