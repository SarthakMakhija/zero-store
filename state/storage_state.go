package state

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory"
	"github.com/SarthakMakhija/zero-store/objectstore"
	objectStore "github.com/SarthakMakhija/zero-store/objectstore/segment"
	"log"
	"time"
)

type StorageState struct {
	activeSegment      *memory.SortedSegment
	inactiveSegments   []*memory.SortedSegment
	persistentSegments map[uint64]*objectStore.SortedSegment
	segmentIdGenerator *SegmentIdGenerator
	closeChannel       chan struct{}
	options            StorageOptions
	store              objectstore.Store
}

func NewStorageState(options StorageOptions) (*StorageState, error) {
	segmentIdGenerator := NewSegmentIdGenerator()
	store, err := options.storeType.GetStore(options.rootDirectory)
	if err != nil {
		return nil, err
	}
	storageState := &StorageState{
		activeSegment:      memory.NewSortedSegment(segmentIdGenerator.NextId(), options.sortedSegmentSizeInBytes),
		persistentSegments: make(map[uint64]*objectStore.SortedSegment),
		segmentIdGenerator: segmentIdGenerator,
		closeChannel:       make(chan struct{}),
		options:            options,
		store:              store,
	}

	storageState.spawnObjectStoreMovement()
	return storageState, nil
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
		state.inactiveSegments = append(state.inactiveSegments, state.activeSegment)
		state.activeSegment = memory.NewSortedSegment(state.segmentIdGenerator.NextId(), state.options.sortedSegmentSizeInBytes)
	}
}

// Close closes the StorageState.
func (state *StorageState) Close() {
	close(state.closeChannel)
	state.store.Close()
}

// spawnObjectStoreMovement starts a goroutine that moves the segments ready to move to object store.
// It also adds the segment to the collection of inactiveSegments, after it is moved to the object store.
func (state *StorageState) spawnObjectStoreMovement() {
	go func() {
		timer := time.NewTimer(state.options.flushInactiveSegmentDuration)
		for {
			select {
			case <-timer.C:
				if _, err := state.mayBeFlushOldestInactiveSegment(); err != nil {
					log.Fatalf("could not flush inactive segment, error: %v", err)
				}
				timer.Reset(state.options.flushInactiveSegmentDuration)
			case <-state.closeChannel:
				timer.Stop()
				return
			}
		}
	}()
}

// mayBeFlushOldestInactiveSegment flushes the oldest inactive segment (memory.SortedSegment) to object store.
// It picks the oldest segment from inactiveSegments fields, if available, creates a persistent sorted segment (objectStore.SortedSegment)
// and writes the result to the object store.
// It returns (false, error), if there is an error.
// It returns (true, nil), if an inactive segment was flushed without any error.
// It returns (false, nil), if there was no inactive segment to be flushed.
func (state *StorageState) mayBeFlushOldestInactiveSegment() (bool, error) {
	buildAndWritePersistentSortedSegment := func(inMemorySegmentToFlush *memory.SortedSegment) (*objectStore.SortedSegment, error) {
		sortedSegmentBuilder := objectStore.NewSortedSegmentBuilderWithDefaultBlockSize(state.store, state.options.sortedSegmentBlockCompression)
		inMemorySegmentToFlush.AllEntries(func(key kv.Key, value kv.Value) {
			sortedSegmentBuilder.Add(key, value)
		})
		persistentSortedSegment, err := sortedSegmentBuilder.Build(inMemorySegmentToFlush.Id())
		if err != nil {
			return nil, err
		}
		return persistentSortedSegment, nil
	}
	if len(state.inactiveSegments) > 0 {
		oldestInMemorySegmentToFlush := state.inactiveSegments[0]
		persistentSortedSegment, err := buildAndWritePersistentSortedSegment(oldestInMemorySegmentToFlush)
		if err != nil {
			return false, err
		}
		state.inactiveSegments = state.inactiveSegments[1:]
		state.persistentSegments[oldestInMemorySegmentToFlush.Id()] = persistentSortedSegment
		return true, nil
	}
	return false, nil
}
