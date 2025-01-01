package state

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory"
)

type nonDurableOnlyGet struct {
	activeSegment    *memory.SortedSegment
	inactiveSegments []*memory.SortedSegment
}

func newNonDurableOnlyGet(activeSegment *memory.SortedSegment, inactiveSegments []*memory.SortedSegment) nonDurableOnlyGet {
	return nonDurableOnlyGet{
		activeSegment:    activeSegment,
		inactiveSegments: inactiveSegments,
	}
}

func (getOperation nonDurableOnlyGet) get(key kv.Key) (kv.Value, bool) {
	if value, ok := getOperation.activeSegment.Get(key); ok {
		return value, true
	}
	for index := len(getOperation.inactiveSegments) - 1; index >= 0; index-- {
		if value, ok := getOperation.inactiveSegments[index].Get(key); ok {
			return value, true
		}
	}
	return kv.EmptyValue, false
}
