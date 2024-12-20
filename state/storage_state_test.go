package state

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStorageStateWithASingleSet(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))

	storageState, err := NewStorageState(NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	_ = storageState.Set(batch)
	value, ok := storageState.Get(kv.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, "raft", value.String())
}

func TestStorageStateWithANonExistingKey(t *testing.T) {
	batch := kv.NewBatch()

	storageState, err := NewStorageState(NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	_ = storageState.Set(batch)
	value, ok := storageState.Get(kv.NewStringKey("non-existing"))
	assert.False(t, ok)
	assert.Equal(t, "", value.String())
}

func TestStorageStateWithASetAndDelete(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	batch.Delete([]byte("consensus"))

	storageState, err := NewStorageState(NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	_ = storageState.Set(batch)
	value, ok := storageState.Get(kv.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, "", value.String())
}

func TestStorageStateWithAFewKeyValuePairsInBatch(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("zero disk"))
	batch.Delete([]byte("consensus"))

	storageState, err := NewStorageState(NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	_ = storageState.Set(batch)
	value, ok := storageState.Get(kv.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, "", value.String())

	value, ok = storageState.Get(kv.NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, "zero disk", value.String())
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentSegment(t *testing.T) {
	storageState, err := NewStorageState(NewStorageOptionsBuilder().
		WithFileSystemStoreType(".").
		WithSortedSegmentSizeInBytes(220).
		WithFlushInactiveSegmentDuration(5 * time.Minute).
		Build(),
	)
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
		storageState.removeAllPersistentSortedSegmentsIn(".")
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = storageState.Set(batch)

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	_ = storageState.Set(batch)

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	_ = storageState.Set(batch)

	keepFlushingInactiveSegmentsUntilNoMoreInactiveSegmentToFlush(t, storageState)

	assert.Equal(t, uint64(3), storageState.activeSegment.Id())
	assert.True(t, storageState.hasPersistentSortedSegmentFor(1))
	assert.True(t, storageState.hasPersistentSortedSegmentFor(2))
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentSegmentWhileWaitingForFlushToObjectStoreToComplete(t *testing.T) {
	storageState, err := NewStorageState(NewStorageOptionsBuilder().
		WithFileSystemStoreType(".").
		WithSortedSegmentSizeInBytes(220).
		WithFlushInactiveSegmentDuration(10 * time.Millisecond).
		Build(),
	)
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
		storageState.removeAllPersistentSortedSegmentsIn(".")
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	flushToObjectStoreFuture := storageState.Set(batch)

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	_ = storageState.Set(batch)

	flushToObjectStoreFuture.Wait()

	assert.True(t, flushToObjectStoreFuture.Status().IsOk())
	assert.Equal(t, uint64(2), storageState.activeSegment.Id())
	assert.True(t, storageState.hasPersistentSortedSegmentFor(1))
}

func keepFlushingInactiveSegmentsUntilNoMoreInactiveSegmentToFlush(t *testing.T, storageState *StorageState) {
	for {
		flushed, err := storageState.mayBeFlushOldestInactiveSegment()
		assert.NoError(t, err)
		if !flushed {
			break
		}
	}
}
