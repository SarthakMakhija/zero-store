package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIterateOverTimestampedBatchWithASingleKey(t *testing.T) {
	batch := NewBatch()
	assert.NoError(t, batch.Set([]byte("raft"), []byte("consensus")))

	timestampedBatch, err := NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)

	iterator := timestampedBatch.Iterator()
	assert.Equal(t, NewStringKeyWithTimestamp("raft", 10), iterator.Key())
	assert.Equal(t, NewValue([]byte("consensus")), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverTimestampedBatchWithACoupleOfKeys(t *testing.T) {
	batch := NewBatch()

	assert.NoError(t, batch.Set([]byte("raft"), []byte("consensus")))
	batch.Delete([]byte("foundationDb"))

	timestampedBatch, err := NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)

	iterator := timestampedBatch.Iterator()
	assert.Equal(t, NewStringKeyWithTimestamp("raft", 10), iterator.Key())
	assert.Equal(t, NewValue([]byte("consensus")), iterator.Value())
	assert.Equal(t, KeyValuePairKindPut, iterator.Kind())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())

	assert.Equal(t, NewStringKeyWithTimestamp("foundationDb", 10), iterator.Key())
	assert.Equal(t, KeyValuePairKindDelete, iterator.Kind())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestSizeInBytesOfTimestampedBatch(t *testing.T) {
	batch := NewBatch()
	assert.NoError(t, batch.Set([]byte("raft"), []byte("consensus")))

	timestampedBatch, err := NewTimestampedBatch(batch, 10)
	assert.NoError(t, err)

	assert.Equal(t, 22, timestampedBatch.SizeInBytes())
}
