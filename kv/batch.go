package kv

import (
	"bytes"
	"errors"
)

type KeyValuePairKind int

const (
	KeyValuePairKindPut    = 1
	KeyValuePairKindDelete = 2
)

// RawKeyValuePair represents the key/value pair with KeyValuePairKind.
type RawKeyValuePair struct {
	key   []byte
	value Value
	kind  KeyValuePairKind
}

// Key returns the key.
func (pair RawKeyValuePair) Key() []byte {
	return pair.key
}

// Value returns the value.
func (pair RawKeyValuePair) Value() Value {
	return pair.value
}

func (pair RawKeyValuePair) Kind() KeyValuePairKind {
	return pair.kind
}

var DuplicateKeyInBatchErr = errors.New("batch already contains the key")

// Batch is a collection of RawKeyValuePair.
type Batch struct {
	pairs []RawKeyValuePair
}

// NewBatch creates an empty Batch.
func NewBatch() *Batch {
	return &Batch{}
}

// Put puts the key/value pair in Batch.
// Returns DuplicateKeyInBatchErr if the key is already present in the Batch.
func (batch *Batch) Put(key, value []byte) error {
	if batch.Contains(key) {
		return DuplicateKeyInBatchErr
	}
	batch.pairs = append(batch.pairs, RawKeyValuePair{
		key:   key,
		value: NewValue(value),
		kind:  KeyValuePairKindPut,
	})
	return nil
}

// Delete is modeled as an append operation.
// It results in another RawKeyValuePair in the batch with kind as KeyValuePairKindDelete.
func (batch *Batch) Delete(key []byte) {
	batch.pairs = append(batch.pairs, RawKeyValuePair{
		key:   key,
		value: EmptyValue,
		kind:  KeyValuePairKindDelete,
	})
}

// Get returns the Value for the given key if found.
func (batch *Batch) Get(key []byte) (Value, bool) {
	for _, pair := range batch.pairs {
		if bytes.Equal(pair.key, key) {
			return pair.value, true
		}
	}
	return EmptyValue, false
}

// Contains returns true of the key is present in Batch.
func (batch *Batch) Contains(key []byte) bool {
	_, ok := batch.Get(key)
	return ok
}

// IsEmpty returns true if the Batch is empty.
func (batch *Batch) IsEmpty() bool {
	return len(batch.pairs) == 0
}

// Length returns the number of RawKeyValuePair(s) in the Batch.
func (batch *Batch) Length() int {
	return len(batch.pairs)
}

// Pairs returns all the RawKeyValuePair(s) in the Batch.
func (batch *Batch) Pairs() []RawKeyValuePair {
	return batch.pairs
}

// SizeInBytes returns the size of the Batch in bytes.
func (batch *Batch) SizeInBytes() int {
	sizeInBytes := 0
	for _, pair := range batch.pairs {
		sizeInBytes += len(pair.Key()) + pair.Value().SizeInBytes()
	}
	return sizeInBytes
}
