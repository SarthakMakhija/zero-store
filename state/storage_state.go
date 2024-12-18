package state

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory"
)

type StorageState struct {
	activeSegment                    *memory.SortedSegment
	inactiveSegments                 []*memory.SortedSegment
	segmentIdGenerator               *SegmentIdGenerator
	segmentsReadyToMoveToObjectStore chan *memory.SortedSegment
	closeChannel                     chan struct{}
	options                          StorageOptions
}

func NewStorageState(options StorageOptions) *StorageState {
	segmentIdGenerator := NewSegmentIdGenerator()
	storageState := &StorageState{
		activeSegment:                    memory.NewSortedSegment(segmentIdGenerator.NextId(), options.sortedSegmentSizeInBytes),
		segmentIdGenerator:               segmentIdGenerator,
		segmentsReadyToMoveToObjectStore: make(chan *memory.SortedSegment, 64),
		closeChannel:                     make(chan struct{}),
		options:                          options,
	}

	storageState.spawnObjectStoreMovement()
	return storageState
}

func (state *StorageState) Get(key kv.Key) (kv.Value, bool) {
	//TODO: May change as transactions come in .. will not read from the segment unless it is made durable.
	return state.activeSegment.Get(key)
}

func (state *StorageState) Set(batch *kv.Batch) {
	state.mayBeFreezeActiveSegment(batch.SizeInBytes())
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

// mayBeFreezeActiveSegment may freeze the active memory.SortedSegment if it does not have required size.
// It creates a new memory.SortedSegment, and sends the previously active memory.SortedSegment to be moved to object store.
func (state *StorageState) mayBeFreezeActiveSegment(sizeInBytes int) {
	if !state.activeSegment.CanFit(int64(sizeInBytes)) {
		state.segmentsReadyToMoveToObjectStore <- state.activeSegment
		state.activeSegment = memory.NewSortedSegment(state.segmentIdGenerator.NextId(), state.options.sortedSegmentSizeInBytes)
	}
}

// Close closes the StorageState.
func (state *StorageState) Close() {
	close(state.closeChannel)
}

// spawnObjectStoreMovement starts a goroutine that moves the segments ready to move to object store.
// It also adds the segment to the collection of inactiveSegments, after it is moved to the object store.
func (state *StorageState) spawnObjectStoreMovement() {
	go func() {
		for {
			select {
			case segment := <-state.segmentsReadyToMoveToObjectStore:
				//TODO: move to object storage
				state.inactiveSegments = append(state.inactiveSegments, segment)
				if len(state.inactiveSegments) > int(state.options.maximumInactiveSegments) {
					state.inactiveSegments = state.inactiveSegments[1:]
				}
			case <-state.closeChannel:
				return
			}
		}
	}()
}
