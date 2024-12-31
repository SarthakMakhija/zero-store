package block

import (
	"fmt"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAttemptToAddAKeyValueToBlockBuilderWithInsufficientSpaceLeftWithBuilder(t *testing.T) {
	blockBuilder := NewBlockBuilder(40)
	assert.True(t, blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft")))
	assert.False(t, blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft")))
}

func TestEncodeAndDecodeBlockWithASingleKeyValueAndSeekToTheFirstKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 11), kv.NewStringValue("etcd"))

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

func TestEncodeAndDecodeBlockWithASingleKeyValueAndSeekToTheKeySuchThatTheTimestampOfTheStoredKeyIsLessThanTheTimestampOfTheGivenKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	iterator := decodedBlock.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 14))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "raft", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestEncodeAndDecodeBlockWithASingleKeyValueAndAttemptSeekToTheKeySuchThatTheTimestampOfTheStoredKeyIsGreaterThanTheTimestampOfTheGivenKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	iterator := decodedBlock.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 2))
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}

func TestEncodeAndDecodeBlockWithTwoKeyValues(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	iterator := decodedBlock.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 5))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, "raft", iterator.Value().String())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, "kv", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestEncodeAndDecodeBlockWithFewKeyValues(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	numberOfKeyValues := 9

	for count := 1; count <= numberOfKeyValues; count++ {
		key := kv.NewStringKeyWithTimestamp(fmt.Sprintf("consensus%d", count), uint64(count))
		assert.True(t, blockBuilder.Add(key, kv.NewStringValue(fmt.Sprintf("raft%d", count))))
	}

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	for count := 1; count <= numberOfKeyValues; count++ {
		iterator := decodedBlock.SeekToKey(kv.NewStringKeyWithTimestamp(fmt.Sprintf("consensus%d", count), uint64(count)))
		assert.True(t, iterator.IsValid())
		assert.Equal(t, fmt.Sprintf("raft%d", count), iterator.Value().String())
		iterator.Close()
	}
}
