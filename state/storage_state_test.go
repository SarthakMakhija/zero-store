package state

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/state/get_strategies"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStorageStateSetWithAnEmptyBatch(t *testing.T) {
	batch := kv.NewBatch()

	storageState, err := NewStorageState(NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	_, err = kv.NewTimestampedBatch(batch, 10)
	assert.Error(t, err)
}

func TestStorageStateWithASingleSet(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("raft"))

	storageState, err := NewStorageState(NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)
	_, _ = storageState.Set(timestampedBatch)

	getResponse := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 10), get_strategies.NonDurableOnlyType)
	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, "raft", getResponse.Value().String())
}

func TestStorageStateWithANonExistingKey(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("raft"))

	storageState, err := NewStorageState(NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)
	_, _ = storageState.Set(timestampedBatch)

	getResponse := storageState.Get(kv.NewStringKeyWithTimestamp("non-existing", 11), get_strategies.NonDurableOnlyType)
	assert.False(t, getResponse.IsValueAvailable())
	assert.Equal(t, "", getResponse.Value().String())
}

func TestStorageStateWithASetAndDelete(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("raft"))
	batch.Delete([]byte("consensus"))

	storageState, err := NewStorageState(NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)
	_, _ = storageState.Set(timestampedBatch)

	getResponse := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11), get_strategies.NonDurableOnlyType)
	assert.False(t, getResponse.IsValueAvailable())
	assert.Equal(t, "", getResponse.Value().String())
}

func TestStorageStateWithAFewKeyValuePairsInBatch(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("raft"))
	_ = batch.Set([]byte("storage"), []byte("zero disk"))
	batch.Delete([]byte("consensus"))

	storageState, err := NewStorageState(NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	timestampedBatch, err := kv.NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)

	_, _ = storageState.Set(timestampedBatch)

	getResponse := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11), get_strategies.NonDurableOnlyType)
	assert.False(t, getResponse.IsValueAvailable())
	assert.Equal(t, "", getResponse.Value().String())

	getResponse = storageState.Get(kv.NewStringKeyWithTimestamp("storage", 11), get_strategies.NonDurableOnlyType)
	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, "zero disk", getResponse.Value().String())
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentSegment(t *testing.T) {
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

	batch = kv.NewBatch()
	_ = batch.Set([]byte("storage"), []byte("NVMe"))
	timestampedBatch, err = kv.NewTimestampedBatch(batch, 20)
	assert.NoError(t, err)
	_, _ = storageState.Set(timestampedBatch)

	batch = kv.NewBatch()
	_ = batch.Set([]byte("data-structure"), []byte("LSM"))
	timestampedBatch, err = kv.NewTimestampedBatch(batch, 30)
	assert.NoError(t, err)
	_, _ = storageState.Set(timestampedBatch)

	keepFlushingInactiveSegmentsUntilNoMoreInactiveSegmentToFlush(t, storageState)

	assert.Equal(t, uint64(3), storageState.activeSegment.Id())
	assert.True(t, storageState.hasPersistentSortedSegmentFor(1))
	assert.True(t, storageState.hasPersistentSortedSegmentFor(2))
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentSegmentWhileWaitingForFlushToObjectStoreToComplete(t *testing.T) {
	storageState, err := NewStorageState(NewStorageOptionsBuilder().
		WithFileSystemStoreType(".").
		WithSortedSegmentSizeInBytes(250).
		WithFlushInactiveSegmentDuration(10 * time.Millisecond).
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
	flushToObjectStoreFuture, err := storageState.Set(timestampedBatch)
	assert.NoError(t, err)

	batch = kv.NewBatch()
	_ = batch.Set([]byte("storage"), []byte("NVMe"))
	timestampedBatch, err = kv.NewTimestampedBatch(batch, 20)
	assert.NoError(t, err)
	_, err = storageState.Set(timestampedBatch)
	assert.NoError(t, err)

	flushToObjectStoreFuture.Wait()

	assert.True(t, flushToObjectStoreFuture.Status().IsOk())
	assert.Equal(t, uint64(2), storageState.activeSegment.Id())
	assert.True(t, storageState.hasPersistentSortedSegmentFor(1))
}

func TestStorageStateWithAMultiplePutsAndDurableOnlyGet(t *testing.T) {
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

	batch = kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("NVMe"))
	timestampedBatch, err = kv.NewTimestampedBatch(batch, 20)
	assert.NoError(t, err)
	_, _ = storageState.Set(timestampedBatch)

	batch = kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("paxos"))
	timestampedBatch, err = kv.NewTimestampedBatch(batch, 21)
	assert.NoError(t, err)
	_, _ = storageState.Set(timestampedBatch)

	keepFlushingInactiveSegmentsUntilNoMoreInactiveSegmentToFlush(t, storageState)

	assert.Equal(t, uint64(3), storageState.activeSegment.Id())
	assert.True(t, storageState.hasPersistentSortedSegmentFor(1))
	assert.True(t, storageState.hasPersistentSortedSegmentFor(2))

	getResponse := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 21), get_strategies.DurableOnlyType)
	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, "NVMe", getResponse.Value().String())
}

func TestStorageStateWithAMultiplePutsAndNonDurableAlsoGet(t *testing.T) {
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

	batch = kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("NVMe"))
	timestampedBatch, err = kv.NewTimestampedBatch(batch, 20)
	assert.NoError(t, err)
	_, _ = storageState.Set(timestampedBatch)

	batch = kv.NewBatch()
	_ = batch.Set([]byte("consensus"), []byte("paxos"))
	timestampedBatch, err = kv.NewTimestampedBatch(batch, 21)
	assert.NoError(t, err)
	_, _ = storageState.Set(timestampedBatch)

	keepFlushingInactiveSegmentsUntilNoMoreInactiveSegmentToFlush(t, storageState)

	assert.Equal(t, uint64(3), storageState.activeSegment.Id())
	assert.True(t, storageState.hasPersistentSortedSegmentFor(1))
	assert.True(t, storageState.hasPersistentSortedSegmentFor(2))

	getResponse := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 21), get_strategies.NonDurableAlsoType)
	assert.True(t, getResponse.IsValueAvailable())
	assert.Equal(t, "paxos", getResponse.Value().String())
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

//TODO: add tests for checking versioned get, after the get implementation is done
