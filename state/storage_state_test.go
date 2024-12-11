package state

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStorageStateWithASingleSet(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))

	storageState := NewStorageState(NewStorageOptionsBuilder().Build())
	storageState.Set(batch)

	value, ok := storageState.Get(kv.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, "raft", value.String())
}

func TestStorageStateWithANonExistingKey(t *testing.T) {
	batch := kv.NewBatch()

	storageState := NewStorageState(NewStorageOptionsBuilder().Build())
	storageState.Set(batch)

	value, ok := storageState.Get(kv.NewStringKey("non-existing"))
	assert.False(t, ok)
	assert.Equal(t, "", value.String())
}

func TestStorageStateWithASetAndDelete(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	batch.Delete([]byte("consensus"))

	storageState := NewStorageState(NewStorageOptionsBuilder().Build())
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

	storageState := NewStorageState(NewStorageOptionsBuilder().Build())
	storageState.Set(batch)

	value, ok := storageState.Get(kv.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, "", value.String())

	value, ok = storageState.Get(kv.NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, "zero disk", value.String())
}
