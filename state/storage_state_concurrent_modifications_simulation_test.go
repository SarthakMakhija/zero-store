package state

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory"
	"github.com/SarthakMakhija/zero-store/state/get_strategies"
	"github.com/stretchr/testify/assert"
	"iter"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestStorageStateWithAGetAgainstAnActiveSegmentWhileItBecomesInactiveByAConcurrentSet(t *testing.T) {
	storageState, err := NewStorageState(NewStorageOptionsBuilder().
		WithFileSystemStoreType(".").
		WithSortedSegmentSizeInBytes(260).
		WithFlushInactiveSegmentDuration(5 * time.Minute).
		Build(),
	)
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
		storageState.removeAllPersistentSortedSegmentsIn(".")
	}()

	batch := kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("raft"))
	timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)

	_, _ = storageState.Set(timestampedBatch)
	assert.Equal(t, 0, len(storageState.inactiveSegments.segments))

	var wg sync.WaitGroup
	wg.Add(1)

	performGetIndicatorChannel := make(chan struct{})
	go func(activeSegment memory.SortedSegment) {
		defer wg.Done()
		getStrategy := get_strategies.NewNonDurableOnlyGet(activeSegment, nil)
		<-performGetIndicatorChannel

		getResponse := getStrategy.Get(kv.NewStringKeyWithTimestamp("consensus", 14))
		assert.True(t, getResponse.IsValueAvailable())
		assert.Equal(t, "raft", getResponse.Value().String())

	}(storageState.activeSegment)

	requiredSizedInBytes := 1024
	storageState.mayBeFreezeActiveSegment(requiredSizedInBytes)
	assert.Equal(t, 1, len(storageState.inactiveSegments.segments))

	performGetIndicatorChannel <- struct{}{}
	wg.Wait()
}

func TestStorageStateWithAGetAgainstAnInActiveSegmentWhileItGetsDroppedByAConcurrentFlush(t *testing.T) {
	storageState, err := NewStorageState(NewStorageOptionsBuilder().
		WithFileSystemStoreType(".").
		WithSortedSegmentSizeInBytes(260).
		WithFlushInactiveSegmentDuration(5 * time.Minute).
		Build(),
	)
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
		storageState.removeAllPersistentSortedSegmentsIn(".")
	}()

	batch := kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("raft"))
	timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)

	_, _ = storageState.Set(timestampedBatch)
	assert.Equal(t, 0, len(storageState.inactiveSegments.segments))

	requiredSizedInBytes := 1024
	storageState.mayBeFreezeActiveSegment(requiredSizedInBytes)
	assert.Equal(t, 1, len(storageState.inactiveSegments.segments))

	var wg sync.WaitGroup
	wg.Add(1)

	performGetIndicatorChannel := make(chan struct{})
	go func(activeSegment memory.SortedSegment, inactiveSegmentsSequence iter.Seq2[int, memory.SortedSegment]) {
		defer wg.Done()
		getStrategy := get_strategies.NewNonDurableOnlyGet(activeSegment, inactiveSegmentsSequence)
		<-performGetIndicatorChannel

		getResponse := getStrategy.Get(kv.NewStringKeyWithTimestamp("consensus", 14))
		assert.True(t, getResponse.IsValueAvailable())
		assert.Equal(t, "raft", getResponse.Value().String())

	}(storageState.activeSegment, slices.Backward(storageState.inactiveSegments.copySegments()))

	storageState.inactiveSegments.dropOldest()
	assert.Equal(t, 0, len(storageState.inactiveSegments.segments))

	performGetIndicatorChannel <- struct{}{}
	wg.Wait()
}

func TestStorageStateWithAGetAgainstAnInActiveSegmentWhileItGetsDroppedByAConcurrentFlushAndGCRuns(t *testing.T) {
	storageState, err := NewStorageState(NewStorageOptionsBuilder().
		WithFileSystemStoreType(".").
		WithSortedSegmentSizeInBytes(260).
		WithFlushInactiveSegmentDuration(5 * time.Minute).
		Build(),
	)
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
		storageState.removeAllPersistentSortedSegmentsIn(".")
	}()

	batch := kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("raft"))
	timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)

	_, _ = storageState.Set(timestampedBatch)
	assert.Equal(t, 0, len(storageState.inactiveSegments.segments))

	requiredSizedInBytes := 1024
	storageState.mayBeFreezeActiveSegment(requiredSizedInBytes)
	assert.Equal(t, 1, len(storageState.inactiveSegments.segments))

	var wg sync.WaitGroup
	wg.Add(1)

	performGetIndicatorChannel := make(chan struct{})
	go func(activeSegment memory.SortedSegment, inactiveSegmentsSequence iter.Seq2[int, memory.SortedSegment]) {
		defer wg.Done()
		getStrategy := get_strategies.NewNonDurableOnlyGet(activeSegment, inactiveSegmentsSequence)
		<-performGetIndicatorChannel

		getResponse := getStrategy.Get(kv.NewStringKeyWithTimestamp("consensus", 14))
		assert.True(t, getResponse.IsValueAvailable())
		assert.Equal(t, "raft", getResponse.Value().String())

	}(storageState.activeSegment, slices.Backward(storageState.inactiveSegments.copySegments()))

	storageState.inactiveSegments.dropOldest()
	assert.Equal(t, 0, len(storageState.inactiveSegments.segments))
	runtime.GC()

	performGetIndicatorChannel <- struct{}{}
	wg.Wait()
}
