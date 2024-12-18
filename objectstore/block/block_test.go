package block

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAttemptToAddAKeyValueToBlockBuilderWithInsufficientSpaceLeftWithBuilder(t *testing.T) {
	blockBuilder := NewBlockBuilder(40)
	assert.True(t, blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft")))
	assert.False(t, blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft")))
}

func TestEncodeAndDecodeBlockWithASingleKeyValueAndSeekToTheFirstKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKey("distributed"), kv.NewStringValue("etcd"))

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	iterator := decodedBlock.SeekToFirst()
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "raft", iterator.Value().String())

	_ = iterator.Next()
	assert.Equal(t, "distributed", iterator.Key().RawString())
	assert.Equal(t, "etcd", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestEncodeAndDecodeBlockWithASingleKeyValueAndSeekToTheKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	iterator := decodedBlock.SeekToKey(kv.NewStringKey("consensus"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "raft", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestEncodeAndDecodeBlockWithTwoKeyValues(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKey("etcd"), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	iterator := decodedBlock.SeekToKey(kv.NewStringKey("consensus"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "raft", iterator.Value().String())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
