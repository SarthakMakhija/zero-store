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

	storageState.Set(batch)
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

	storageState.Set(batch)
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

	storageState.Set(batch)
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

	storageState.Set(batch)
	value, ok := storageState.Get(kv.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, "", value.String())

	value, ok = storageState.Get(kv.NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, "zero disk", value.String())
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentSegment(t *testing.T) {
	storageState, err := NewStorageState(NewStorageOptionsBuilder().WithFileSystemStoreType(".").WithSortedSegmentSizeInBytes(170).Build())
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(batch)

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(batch)

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(batch)

	time.Sleep(100 * time.Millisecond)

	assert.True(t, storageState.HasInactiveSegments())
	assert.Equal(t, 3, len(storageState.inactiveSegments))
	assert.Equal(t, []uint64{1, 2, 3}, storageState.sortedInactiveSegmentIds())
	assert.Equal(t, uint64(4), storageState.activeSegment.Id())
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentSegment2(t *testing.T) {
	storageState, err := NewStorageState(
		NewStorageOptionsBuilder().WithFileSystemStoreType(".").
			WithSortedSegmentSizeInBytes(170).
			WithMaximumInactiveSegments(2).
			Build(),
	)
	assert.NoError(t, err)

	defer func() {
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(batch)

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(batch)

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(batch)

	time.Sleep(100 * time.Millisecond)

	assert.True(t, storageState.HasInactiveSegments())
	assert.Equal(t, 2, len(storageState.inactiveSegments))
	assert.Equal(t, []uint64{2, 3}, storageState.sortedInactiveSegmentIds())
	assert.Equal(t, uint64(4), storageState.activeSegment.Id())
}
