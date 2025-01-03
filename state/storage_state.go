package state

import (
	"errors"
	"github.com/SarthakMakhija/zero-store/future"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory"
	"github.com/SarthakMakhija/zero-store/objectstore"
	objectStore "github.com/SarthakMakhija/zero-store/objectstore/segment"
	"github.com/SarthakMakhija/zero-store/state/get_strategies"
	"log"
	"slices"
	"sync"
	"time"
)

var (
	ErrDbStopped = errors.New("db is stopped, can not perform the operation")
)

type StorageState struct {
	activeSegment            *memory.SortedSegment
	inactiveSegments         *inactiveSegments
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
		inactiveSegments:         newInactiveSegments(),
		persistentSortedSegments: persistentSortedSegments,
		segmentIdGenerator:       segmentIdGenerator,
		closeChannel:             make(chan struct{}),
		options:                  options,
		store:                    store,
	}

	storageState.spawnObjectStoreMovement()
	return storageState, nil
}

func (state *StorageState) Get(key kv.Key, strategy get_strategies.GetStrategyType) get_strategies.GetResponse {
	//TODO: resolveGetStrategy acquires a RLock for getting the current snapshot of the segments.
	//The lock is released after the segments have been acquired.
	//The following issue can happen:
	//A get request comes for nonDurableOnlyGet. resolveGetStrategy acquires the RLock and gets the current active segment,
	//and then releases the lock.
	//In the meantime, a Set request comes in which tries to add the batch to the active segment just to realize that the
	//active segment is full. This changes the active segment to refer to the newly created empty segment by acquiring the write lock.
	//This can now cause issues in the read operation, because the read operation is running on a segment which has been replaced.

	newNonDurableOnlyGet := func() get_strategies.NonDurableOnlyGet {
		return get_strategies.NewNonDurableOnlyGet(state.activeSegment, slices.Backward(state.inactiveSegments.copySegments()))
	}
	newDurableOnlyGet := func() get_strategies.DurableOnlyGet {
		return get_strategies.NewDurableOnlyGet(state.persistentSortedSegments, slices.All(state.persistentSortedSegments.OrderedSegmentsByDescendingSegmentId()))
	}
	newNonDurableAlsoGet := func() get_strategies.NonDurableAlsoGet {
		return get_strategies.NewNonDurableAlsoGet(newNonDurableOnlyGet(), newDurableOnlyGet())
	}
	resolveGetStrategy := func() get_strategies.GetStrategy {
		state.stateLock.RLock()
		defer state.stateLock.RUnlock()

		switch strategy {
		case get_strategies.NonDurableOnlyType:
			return newNonDurableOnlyGet()
		case get_strategies.DurableOnlyType:
			return newDurableOnlyGet()
		case get_strategies.NonDurableAlsoType:
			return newNonDurableAlsoGet()
		default:
			panic("unknown get strategy")
		}
	}
	return resolveGetStrategy().Get(key)
}

func (state *StorageState) Set(batch kv.TimestampedBatch) (*future.Future, error) {
	state.mayBeFreezeActiveSegment(batch.SizeInBytes())
	if err := state.writeToActiveSegment(batch); err != nil {
		return nil, err
	}
	return state.activeSegment.FlushToObjectStoreFuture(), nil
}

// mayBeFreezeActiveSegment may freeze the active memory.SortedSegment if it does not have required size.
// It creates a new memory.SortedSegment, and sends the previously active memory.SortedSegment to be moved to object store.
func (state *StorageState) mayBeFreezeActiveSegment(sizeInBytes int) {
	if !state.activeSegment.CanFit(int64(sizeInBytes)) {
		state.stateLock.Lock()
		state.inactiveSegments.append(state.activeSegment)
		state.activeSegment = memory.NewSortedSegment(state.segmentIdGenerator.NextId(), state.options.sortedSegmentSizeInBytes)
		state.stateLock.Unlock()
	}
}

// writeToActiveSegment writes the batch to the active segment.
func (state *StorageState) writeToActiveSegment(batch kv.TimestampedBatch) error {
	iterator := batch.Iterator()
	for iterator.IsValid() {
		switch {
		case iterator.Kind() == kv.KeyValuePairKindPut:
			state.activeSegment.Set(iterator.Key(), iterator.Value())
		case iterator.Kind() == kv.KeyValuePairKindDelete:
			state.activeSegment.Delete(iterator.Key())
		default:
			panic("unknown key/value pair kind")
		}
		if err := iterator.Next(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the StorageState.
func (state *StorageState) Close() {
	close(state.closeChannel)
	state.store.Close()
	state.inactiveSegments.flushAllToObjectStoreMarkAsError()
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
		state.inactiveSegments.dropOldest()
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

		if segment, ok := state.inactiveSegments.oldest(); ok {
			return segment
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

type inactiveSegments struct {
	segments []*memory.SortedSegment //oldest to latest
}

func newInactiveSegments() *inactiveSegments {
	return &inactiveSegments{}
}

func (segments *inactiveSegments) append(segment *memory.SortedSegment) {
	segments.segments = append(segments.segments, segment)
}

func (segments *inactiveSegments) oldest() (*memory.SortedSegment, bool) {
	if len(segments.segments) > 0 {
		return segments.segments[0], true
	}
	return nil, false
}

func (segments *inactiveSegments) dropOldest() {
	segments.segments = segments.segments[1:]
}

func (segments *inactiveSegments) flushAllToObjectStoreMarkAsError() {
	for _, segment := range segments.segments {
		segment.FlushToObjectStoreAsyncAwait().MarkDoneAsError(ErrDbStopped)
	}
}

func (segments *inactiveSegments) copySegments() []*memory.SortedSegment {
	clonedSegments := make([]*memory.SortedSegment, len(segments.segments))
	copy(clonedSegments, segments.segments)
	return clonedSegments
}
