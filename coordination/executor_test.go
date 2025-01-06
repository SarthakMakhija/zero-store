package coordination

import (
	"fmt"
	"github.com/SarthakMakhija/zero-store/future"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/state"
	"github.com/SarthakMakhija/zero-store/state/get_strategies"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestSubmitATimestampedBatchToExecutor(t *testing.T) {
	storageState, err := state.NewStorageState(state.NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	executor := NewExecutor(storageState)
	defer func() {
		storageState.Close()
		executor.stop()
	}()

	batch := kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("raft"))
	timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)

	inMemorySegmentSetFuture := executor.submit(timestampedBatch)
	inMemorySegmentSetFuture.Wait()

	getResponse := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11), get_strategies.NonDurableOnlyType)
	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, "raft", getResponse.Value().String())
}

func TestSubmitATimestampedBatchToExecutorAndWaitForTheSegmentContainingTheBatchToBeFlushedToObjectStore(t *testing.T) {
	storageState, err := state.NewStorageState(
		state.
			NewStorageOptionsBuilder().
			WithSortedSegmentSizeInBytes(260).
			WithFlushInactiveSegmentDuration(20 * time.Millisecond).
			WithFileSystemStoreType(".").
			Build(),
	)
	assert.NoError(t, err)

	executor := NewExecutor(storageState)
	defer func() {
		storageState.RemoveAllPersistentSortedSegmentsIn(".")
		storageState.Close()
		executor.stop()
	}()

	batch := kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("raft"))
	timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)

	inMemorySegmentSetFuture := executor.submit(timestampedBatch)
	inMemorySegmentFlushFuture := inMemorySegmentSetFuture.WaitForResponse()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		inMemorySegmentFlushFuture.Wait()
	}()

	go func() {
		defer wg.Done()

		batch := kv.NewBatch()
		_ = batch.Set([]byte("storage"), []byte("NVMe"))
		timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
		assert.NoError(t, err)

		//will cause the active segment in state.StorageState to become inactive, which will then be flushed to object
		//store in background.
		_, err = storageState.Set(timestampedBatch)
		assert.NoError(t, err)
	}()

	wg.Wait()

	getResponse := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11), get_strategies.DurableOnlyType)
	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, "raft", getResponse.Value().String())
}

func TestSubmitATimestampedBatchToStoppedExecutor(t *testing.T) {
	storageState, err := state.NewStorageState(state.NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	executor := NewExecutor(storageState)
	defer func() {
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("raft"))
	timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)

	executor.stop()

	inMemorySegmentSetFuture := executor.submit(timestampedBatch)
	inMemorySegmentSetFuture.Wait()

	assert.Error(t, state.ErrDbStopped, inMemorySegmentSetFuture.Status().Error())
}

func TestSubmitBatchesToExecutorAndStopItInBetween(t *testing.T) {
	storageState, err := state.NewStorageState(state.NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	executor := NewExecutor(storageState)
	defer func() {
		storageState.Close()
		executor.stop()
	}()

	var allFutures []*future.Future[*future.Future[struct{}]]
	executorStopIndicationChannel := make(chan struct{})
	totalBatchesToSubmit, closeExecutorAtBatchIndex := 150, 80

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for count := 1; count <= totalBatchesToSubmit; count++ {
			batch := kv.NewBatch()
			_ = batch.Set([]byte(fmt.Sprintf("consensus%d", count)), []byte("raft"))
			timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
			assert.NoError(t, err)
			allFutures = append(allFutures, executor.submit(timestampedBatch))

			if count == closeExecutorAtBatchIndex {
				executorStopIndicationChannel <- struct{}{}
			}
		}
	}()

	go func() {
		defer wg.Done()
		<-executorStopIndicationChannel
		executor.stop()
	}()

	wg.Wait()

	var errors []error
	for _, aFuture := range allFutures {
		aFuture.Wait()
		if aFuture.Status().IsError() {
			errors = append(errors, aFuture.Status().Error())
		}
	}
	assert.True(t, len(errors) > 0)
}
