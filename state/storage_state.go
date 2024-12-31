package state

import (
	"errors"
	"github.com/SarthakMakhija/zero-store/future"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory"
	"github.com/SarthakMakhija/zero-store/objectstore"
	objectStore "github.com/SarthakMakhija/zero-store/objectstore/segment"
	"log"
	"sync"
	"time"
)

var (
	ErrDbStopped  = errors.New("db is stopped, can not perform the operation")
	ErrEmptyBatch = errors.New("batch is empty, can not perform Set")
)

type StorageState struct {
	activeSegment            *memory.SortedSegment
	inactiveSegments         []*memory.SortedSegment
	persistentSortedSegments *objectStore.SortedSegments
	segmentIdGenerator       *SegmentIdGenerator
	closeChannel             chan struct{}
	options                  StorageOptions
	store                    objectstore.Store
	stateLock                sync.RWMutex
}

func NewStorageState(options StorageOptions) (*StorageState, error) {
	segmentIdGenerator := NewSegmentIdGenerator()
	store, err := options.storeType.GetStore(options.rootDirectory)
	if err != nil {
		return nil, err
	}
	persistentSortedSegments, err := objectStore.NewSortedSegments(
		store,
		objectStore.NewSortedSegmentCacheOptions(options.bloomFilterCacheOptions, options.blockMetaListCacheOptions),
		options.sortedSegmentBlockCompression,
	)
	if err != nil {
		return nil, err
	}
	storageState := &StorageState{
		activeSegment:            memory.NewSortedSegment(segmentIdGenerator.NextId(), options.sortedSegmentSizeInBytes),
		persistentSortedSegments: persistentSortedSegments,
		segmentIdGenerator:       segmentIdGenerator,
		closeChannel:             make(chan struct{}),
		options:                  options,
		store:                    store,
	}

	if !options.inMemoryMode {
		storageState.spawnObjectStoreMovement()
	}
	return storageState, nil
}

func (state *StorageState) Get(key kv.Key) (kv.Value, bool) {
	state.stateLock.RLock()
	defer state.stateLock.RUnlock()

	//TODO: May change as transactions come in .. will not read from the segment unless it is made durable.
	//TODO: Handle in-memory mode.
	return state.activeSegment.Get(key)
}

func (state *StorageState) Set(batch *kv.Batch) (*future.Future, error) {
	if batch.IsEmpty() {
		return nil, ErrEmptyBatch
	}
	state.mayBeFreezeActiveSegment(batch.SizeInBytes())
	state.writeToActiveSegment(batch)
	state.mayBeMarkFlushToObjectStoreFutureDone()
	return state.activeSegment.FlushToObjectStoreFuture(), nil
}

// mayBeFreezeActiveSegment may freeze the active memory.SortedSegment if it does not have required size.
// It creates a new memory.SortedSegment, and sends the previously active memory.SortedSegment to be moved to object store.
func (state *StorageState) mayBeFreezeActiveSegment(sizeInBytes int) {
	if !state.activeSegment.CanFit(int64(sizeInBytes)) {
		state.stateLock.Lock()
		state.inactiveSegments = append(state.inactiveSegments, state.activeSegment)
		state.activeSegment = memory.NewSortedSegment(state.segmentIdGenerator.NextId(), state.options.sortedSegmentSizeInBytes)
		state.stateLock.Unlock()
	}
}

// writeToActiveSegment writes the batch to the active segment.
func (state *StorageState) writeToActiveSegment(batch *kv.Batch) {
	for _, pair := range batch.Pairs() {
		switch {
		case pair.Kind() == kv.KeyValuePairKindPut:
			state.activeSegment.Set(kv.NewKey(pair.Key(), 0), pair.Value())
		case pair.Kind() == kv.KeyValuePairKindDelete:
			state.activeSegment.Delete(kv.NewKey(pair.Key(), 0))
		default:
			panic("unknown key/value pair kind")
		}
	}
}

// mayBeMarkFlushToObjectStoreFutureDone marks the future representing the flush to object store as done, if the mode is in-memory.
func (state *StorageState) mayBeMarkFlushToObjectStoreFutureDone() {
	if state.options.inMemoryMode {
		state.activeSegment.FlushToObjectStoreAsyncAwait().MarkDoneAsOk()
	}
}

// Close closes the StorageState.
func (state *StorageState) Close() {
	close(state.closeChannel)
	state.store.Close()
	for _, segment := range state.inactiveSegments {
		segment.FlushToObjectStoreAsyncAwait().MarkDoneAsError(ErrDbStopped)
	}
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
	//TODO: what if an inactive segment is being flushed (in buildAndWritePersistentSortedSegment),
	//other goroutine comes and starts reading from the same inactive segment in StorageState,
	//and finally that inactive segment gets dropped in updateState (and GCed).
	//Solutions:
	//Maybe, acquire exclusive lock in mayBeFlushOldestInactiveSegment()
	//Or, in updateState, drop the segment from inactiveSegments but move it to another collection "dropped segments"
	//and when reference count of inactive segment reaches zero, move it out.
	updateState := func(segmentId uint64) {
		state.stateLock.Lock()
		defer state.stateLock.Unlock()

		state.inactiveSegments = state.inactiveSegments[1:]
	}
	buildAndWritePersistentSortedSegment := func(inMemorySegmentToFlush *memory.SortedSegment) (*objectStore.SortedSegment, error) {
		return state.persistentSortedSegments.BuildAndWritePersistentSortedSegment(
			memory.NewAllEntriesSortedSegmentIterator(inMemorySegmentToFlush),
			inMemorySegmentToFlush.Id(),
		)
	}
	oldestInactiveSegmentIfAvailable := func() *memory.SortedSegment {
		state.stateLock.RLock()
		defer state.stateLock.RUnlock()

		if len(state.inactiveSegments) > 0 {
			return state.inactiveSegments[0]
		}
		return nil
	}

	if oldestInMemorySegmentToFlush := oldestInactiveSegmentIfAvailable(); oldestInMemorySegmentToFlush != nil {
		_, err := buildAndWritePersistentSortedSegment(oldestInMemorySegmentToFlush)
		if err != nil {
			//TODO: what if flush succeeds later on, how will AsyncAwait handle it?
			oldestInMemorySegmentToFlush.FlushToObjectStoreAsyncAwait().MarkDoneAsError(err)
			return false, err
		}
		oldestInMemorySegmentToFlush.FlushToObjectStoreAsyncAwait().MarkDoneAsOk()
		updateState(oldestInMemorySegmentToFlush.Id())
		return true, nil
	}
	return false, nil
}
