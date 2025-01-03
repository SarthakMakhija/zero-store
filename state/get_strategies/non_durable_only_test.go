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

	getOperation := NewNonDurableOnlyGet(activeSegment, nil)
	getResponse := getOperation.Get(kv.NewStringKeyWithTimestamp("consensus", 2))

	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("raft"), getResponse.Value())
}

func TestNonDurableOnlyGetForANonExistingKeyFromActiveSegment(t *testing.T) {
	activeSegment := memory.NewSortedSegment(1, 1<<10)
	activeSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 1), kv.NewStringValue("raft"))

	getOperation := NewNonDurableOnlyGet(activeSegment, nil)
	getResponse := getOperation.Get(kv.NewStringKeyWithTimestamp("non-existing", 2))

	assert.False(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.EmptyValue, getResponse.Value())
}

func TestNonDurableOnlyGetFromActiveAndSingleInactiveSegment(t *testing.T) {
	activeSegment := memory.NewSortedSegment(1, 1<<10)
	activeSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 9), kv.NewStringValue("raft"))

	inactiveSegment := memory.NewSortedSegment(2, 1<<10)
	inactiveSegment.Set(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("raft"))
	inactiveSegment.Set(kv.NewStringKeyWithTimestamp("distributed", 5), kv.NewStringValue("etcd"))

	inactiveSegments := []*memory.SortedSegment{inactiveSegment}
	getOperation := NewNonDurableOnlyGet(activeSegment, slices.Backward(inactiveSegments))
	getResponse := getOperation.Get(kv.NewStringKeyWithTimestamp("consensus", 10))

	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("raft"), getResponse.Value())
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
	getOperation := NewNonDurableOnlyGet(activeSegment, slices.Backward(inactiveSegments))
	getResponse := getOperation.Get(kv.NewStringKeyWithTimestamp("consensus", 7))

	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("paxos"), getResponse.Value())
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
	getOperation := NewNonDurableOnlyGet(activeSegment, slices.Backward(inactiveSegments))
	getResponse := getOperation.Get(kv.NewStringKeyWithTimestamp("consensus", 8))

	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("raft"), getResponse.Value())
}
