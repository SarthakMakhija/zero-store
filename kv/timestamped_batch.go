package kv

import (
	"errors"
)

var ErrEmptyBatch = errors.New("batch is empty, can not perform Set")

type TimestampedBatch struct {
	timestamp        uint64
	rawKeyValuePairs []RawKeyValuePair
}

func NewTimestampedBatch(batch *Batch, timestamp uint64) (TimestampedBatch, error) {
	if batch.IsEmpty() {
		return TimestampedBatch{}, ErrEmptyBatch
	}
	return TimestampedBatch{
		rawKeyValuePairs: batch.Pairs(),
		timestamp:        timestamp,
	}, nil
}

func (batch TimestampedBatch) Iterator() *TimestampedBatchIterator {
	return &TimestampedBatchIterator{
		index: 0,
		batch: batch,
	}
}

type TimestampedBatchIterator struct {
	index int
	batch TimestampedBatch
}

func (iterator *TimestampedBatchIterator) Key() Key {
	return NewKey(iterator.batch.rawKeyValuePairs[iterator.index].Key(), iterator.batch.timestamp)
}

func (iterator *TimestampedBatchIterator) Value() Value {
	return iterator.batch.rawKeyValuePairs[iterator.index].Value()
}

func (iterator *TimestampedBatchIterator) Next() error {
	iterator.index = iterator.index + 1
	return nil
}

func (iterator *TimestampedBatchIterator) IsValid() bool {
	return iterator.index < len(iterator.batch.rawKeyValuePairs)
}

func (iterator *TimestampedBatchIterator) Close() {}
