package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmptyBatch(t *testing.T) {
	batch := NewBatch()
	assert.Equal(t, true, batch.IsEmpty())
}

func TestNonEmptyBatch(t *testing.T) {
	batch := NewBatch()
	_ = batch.Set([]byte("HDD"), []byte("Hard disk"))
	assert.Equal(t, false, batch.IsEmpty())
}

func TestAddsDuplicateKeyInBatch(t *testing.T) {
	batch := NewBatch()
	_ = batch.Set([]byte("HDD"), []byte("Hard disk"))
	err := batch.Set([]byte("HDD"), []byte("Hard disk"))

	assert.Error(t, err)
	assert.Equal(t, DuplicateKeyInBatchErr, err)
}

func TestGetTheValueOfAKeyFromBatch(t *testing.T) {
	batch := NewBatch()
	_ = batch.Set([]byte("HDD"), []byte("Hard disk"))

	value, ok := batch.Get([]byte("HDD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Hard disk", value.String())
}

func TestGetTheValueOfANonExistingKeyFromBatch(t *testing.T) {
	batch := NewBatch()
	_ = batch.Set([]byte("HDD"), []byte("Hard disk"))

	_, ok := batch.Get([]byte("non-existing"))
	assert.Equal(t, false, ok)
}

func TestContainsTheKey(t *testing.T) {
	batch := NewBatch()
	_ = batch.Set([]byte("HDD"), []byte("Hard disk"))

	contains := batch.Contains([]byte("HDD"))
	assert.Equal(t, true, contains)
}

func TestDoesNotContainTheKey(t *testing.T) {
	batch := NewBatch()
	_ = batch.Set([]byte("HDD"), []byte("Hard disk"))

	contains := batch.Contains([]byte("SSD"))
	assert.Equal(t, false, contains)
}
