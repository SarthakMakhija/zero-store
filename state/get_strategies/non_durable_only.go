package get_strategies

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory"
	"iter"
)

type nonDurableOnlyGet struct {
	activeSegment            *memory.SortedSegment
	inactiveSegmentsSequence iter.Seq2[int, *memory.SortedSegment]
}

func newNonDurableOnlyGet(activeSegment *memory.SortedSegment, inactiveSegmentsSequence iter.Seq2[int, *memory.SortedSegment]) nonDurableOnlyGet {
	return nonDurableOnlyGet{
		activeSegment:            activeSegment,
		inactiveSegmentsSequence: inactiveSegmentsSequence,
	}
}

func (getOperation nonDurableOnlyGet) get(key kv.Key) GetResponse {
	if value, ok := getOperation.activeSegment.Get(key); ok {
		return positiveResponse(value)
	}
	if getOperation.inactiveSegmentsSequence != nil {
		for _, inactiveSegment := range getOperation.inactiveSegmentsSequence {
			if value, ok := inactiveSegment.Get(key); ok {
				return positiveResponse(value)
			}
		}
	}
	return negativeResponse()
}
