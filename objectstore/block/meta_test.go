package block

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	doNotEnableCompression = false
	enableCompression      = true
)

func TestBlockMetaListWithASingleBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKey("accurate"),
		EndingKey:           kv.NewStringKey("consensus"),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList, _ := DecodeToBlockMetaList(encoded, doNotEnableCompression)

	assert.Equal(t, 1, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
}

func TestBlockMetaListWithThreeBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKey("accurate"),
		EndingKey:           kv.NewStringKey("badger"),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         kv.NewStringKey("bolt"),
		EndingKey:           kv.NewStringKey("calculator"),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         kv.NewStringKey("consensus"),
		EndingKey:           kv.NewStringKey("distributed"),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList, _ := DecodeToBlockMetaList(encoded, doNotEnableCompression)

	assert.Equal(t, 3, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(1)
	assert.Equal(t, "bolt", meta.StartingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(2)
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
}

func TestBlockMetaListWithThreeBlockMetaWithEndingKeyOfEachBlock(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKey("accurate"),
		EndingKey:           kv.NewStringKey("amorphous"),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         kv.NewStringKey("bolt"),
		EndingKey:           kv.NewStringKey("bunt"),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         kv.NewStringKey("consensus"),
		EndingKey:           kv.NewStringKey("distributed"),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList, _ := DecodeToBlockMetaList(encoded, doNotEnableCompression)

	assert.Equal(t, 3, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, uint32(0), meta.BlockStartingOffset)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
	assert.Equal(t, "amorphous", meta.EndingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(1)
	assert.Equal(t, uint32(4096), meta.BlockStartingOffset)
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, "bunt", meta.EndingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(2)
	assert.Equal(t, uint32(8192), meta.BlockStartingOffset)
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, "distributed", meta.EndingKey.RawString())
}

func TestBlockMetaListWithStartingKeyOfFirstBlock(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKey("accurate"),
		EndingKey:           kv.NewStringKey("badger"),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         kv.NewStringKey("bolt"),
		EndingKey:           kv.NewStringKey("calculator"),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         kv.NewStringKey("consensus"),
		EndingKey:           kv.NewStringKey("distributed"),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList, _ := DecodeToBlockMetaList(encoded, doNotEnableCompression)

	startingKeyOfFirstBlock, ok := decodedBlockMetaList.StartingKeyOfFirstBlock()
	assert.True(t, ok)
	assert.Equal(t, "accurate", startingKeyOfFirstBlock.RawString())
}

func TestBlockMetaListWithEndingKeyOfLastBlock(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKey("accurate"),
		EndingKey:           kv.NewStringKey("amorphous"),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         kv.NewStringKey("bolt"),
		EndingKey:           kv.NewStringKey("bunt"),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         kv.NewStringKey("consensus"),
		EndingKey:           kv.NewStringKey("distributed"),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList, _ := DecodeToBlockMetaList(encoded, doNotEnableCompression)

	endingKeyOfLastBlock, ok := decodedBlockMetaList.EndingKeyOfLastBlock()
	assert.True(t, ok)
	assert.Equal(t, "distributed", endingKeyOfLastBlock.RawString())
}

func TestBlockMetaListWithFewBlockMetaAndCompressionEnabled(t *testing.T) {
	blockMetaList := NewBlockMetaList(enableCompression)

	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKey("accurate"),
		EndingKey:           kv.NewStringKey("badger"),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         kv.NewStringKey("bolt"),
		EndingKey:           kv.NewStringKey("calculator"),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         kv.NewStringKey("consensus"),
		EndingKey:           kv.NewStringKey("distributed"),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList, _ := DecodeToBlockMetaList(encoded, enableCompression)

	assert.Equal(t, 3, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(1)
	assert.Equal(t, "bolt", meta.StartingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(2)
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
}

//TODO: tests for may contain ..
