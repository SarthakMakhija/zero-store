package block

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlockSeekToTheMatchingKey(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKey("etcd"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyWithCustomBlockSize(t *testing.T) {
	blockBuilder := NewBlockBuilder(8192)
	blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKey("etcd"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKey("consensus"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "raft", iterator.Value().String())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyGreaterThanTheSpecifiedKey(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKey("distributed"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKey("etcd"), iterator.Key())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyGreaterThanTheSpecifiedKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("kv"))
	blockBuilder.Add(kv.NewStringKey("foundationDb"), kv.NewStringValue("distributed-kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKey("distributed"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKey("etcd"), iterator.Key())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKey("foundationDb"), iterator.Key())
	assert.Equal(t, "distributed-kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyWithADeletedValue(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewDeletedValue())

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKey("consensus"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKey("consensus"), iterator.Key())
	assert.Equal(t, "", iterator.Value().String())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheNonExistingKey(t *testing.T) {
	blockBuilder := NewBlockBuilderWithDefaultBlockSize()
	blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKey("foundationDb"))
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}
