package kv

import (
	"errors"
)

var ErrEmptyBatch = errors.New("batch is empty, can not perform Set")

type TimestampedBatch struct {
	keys   []Key
	values []Value
	kinds  []KeyValuePairKind
}

func NewTimestampedBatch(batch *Batch, timestamp uint64) (TimestampedBatch, error) {
	if batch.IsEmpty() {
		return TimestampedBatch{}, ErrEmptyBatch
	}

	keys := make([]Key, 0, len(batch.pairs))
	values := make([]Value, 0, len(batch.pairs))
	kinds := make([]KeyValuePairKind, 0, len(batch.pairs))

	for _, pair := range batch.pairs {
		keys = append(keys, NewKey(pair.key, timestamp))
		values = append(values, pair.value)
		kinds = append(kinds, pair.Kind())
	}
	return TimestampedBatch{keys, values, kinds}, nil
}

func (batch TimestampedBatch) Iterator() *TimestampedBatchIterator {
	return &TimestampedBatchIterator{
		index: 0,
		batch: batch,
	}
}

func (batch TimestampedBatch) SizeInBytes() int {
	sizeInBytes := 0
	for index, key := range batch.keys {
		sizeInBytes += key.EncodedSizeInBytes() + batch.values[index].SizeInBytes()
	}
	return sizeInBytes
}

type TimestampedBatchIterator struct {
	index int
	batch TimestampedBatch
}

func (iterator *TimestampedBatchIterator) Key() Key {
	return iterator.batch.keys[iterator.index]
}

func (iterator *TimestampedBatchIterator) Value() Value {
	return iterator.batch.values[iterator.index]
}

func (iterator *TimestampedBatchIterator) Kind() KeyValuePairKind {
	return iterator.batch.kinds[iterator.index]
}

func (iterator *TimestampedBatchIterator) Next() error {
	iterator.index = iterator.index + 1
	return nil
}

func (iterator *TimestampedBatchIterator) IsValid() bool {
	return iterator.index < len(iterator.batch.keys)
}

func (iterator *TimestampedBatchIterator) Close() {}
