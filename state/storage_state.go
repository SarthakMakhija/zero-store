package state

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory"
)

type StorageState struct {
	activeSegment      *memory.SortedSegment
	segmentIdGenerator *SegmentIdGenerator
}

func NewStorageState(options StorageOptions) *StorageState {
	segmentIdGenerator := NewSegmentIdGenerator()
	return &StorageState{
		activeSegment:      memory.NewSortedSegment(segmentIdGenerator.NextId(), options.sortedSegmentSizeInBytes),
		segmentIdGenerator: segmentIdGenerator,
	}
}

func (state *StorageState) Get(key kv.Key) (kv.Value, bool) {
	return state.activeSegment.Get(key)
}

func (state *StorageState) Set(batch *kv.Batch) {
	for _, pair := range batch.Pairs() {
		switch {
		case pair.Kind() == kv.KeyValuePairKindPut:
			state.activeSegment.Set(kv.NewKey(pair.Key()), pair.Value())
		case pair.Kind() == kv.KeyValuePairKindDelete:
			state.activeSegment.Delete(kv.NewKey(pair.Key()))
		default:
			panic("unknown key/value pair kind")
		}
	}
}
