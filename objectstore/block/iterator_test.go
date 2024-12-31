package block

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlockSeekToTheMatchingKey(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 10), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("etcd", 10))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyWithCustomBlockSize(t *testing.T) {
	blockBuilder := NewBlockBuilder(8192)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 10), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("etcd", 10))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 10), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 5))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "raft", iterator.Value().String())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyWithTimestampLesserThanTheProvided(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 5), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("etcd", 6))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyGreaterThanTheSpecifiedKey(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 10), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("distributed", 11))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 10), iterator.Key())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyGreaterThanTheSpecifiedKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringValue("kv"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("foundationDb", 7), kv.NewStringValue("distributed-kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("distributed", 8))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 6), iterator.Key())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("foundationDb", 7), iterator.Key())
	assert.Equal(t, "distributed-kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyWithADeletedValue(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewDeletedValue())

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 6))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 5), iterator.Key())
	assert.Equal(t, "", iterator.Value().String())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheNonExistingKey(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("foundationDb", 7))
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}
