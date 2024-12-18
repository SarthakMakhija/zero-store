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
		BlockBeginOffset: 0,
		StartingKey:      kv.NewStringKey("accurate"),
		EndingKey:        kv.NewStringKey("consensus"),
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
		BlockBeginOffset: 0,
		StartingKey:      kv.NewStringKey("accurate"),
		EndingKey:        kv.NewStringKey("badger"),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKey("bolt"),
		EndingKey:        kv.NewStringKey("calculator"),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 8192,
		StartingKey:      kv.NewStringKey("consensus"),
		EndingKey:        kv.NewStringKey("distributed"),
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
		BlockBeginOffset: 0,
		StartingKey:      kv.NewStringKey("accurate"),
		EndingKey:        kv.NewStringKey("amorphous"),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKey("bolt"),
		EndingKey:        kv.NewStringKey("bunt"),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 8192,
		StartingKey:      kv.NewStringKey("consensus"),
		EndingKey:        kv.NewStringKey("distributed"),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList, _ := DecodeToBlockMetaList(encoded, doNotEnableCompression)

	assert.Equal(t, 3, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, uint32(0), meta.BlockBeginOffset)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
	assert.Equal(t, "amorphous", meta.EndingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(1)
	assert.Equal(t, uint32(4096), meta.BlockBeginOffset)
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, "bunt", meta.EndingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(2)
	assert.Equal(t, uint32(8192), meta.BlockBeginOffset)
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, "distributed", meta.EndingKey.RawString())
}

func TestBlockMetaListWithStartingKeyOfFirstBlock(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{
		BlockBeginOffset: 0,
		StartingKey:      kv.NewStringKey("accurate"),
		EndingKey:        kv.NewStringKey("badger"),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKey("bolt"),
		EndingKey:        kv.NewStringKey("calculator"),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 8192,
		StartingKey:      kv.NewStringKey("consensus"),
		EndingKey:        kv.NewStringKey("distributed"),
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
		BlockBeginOffset: 0,
		StartingKey:      kv.NewStringKey("accurate"),
		EndingKey:        kv.NewStringKey("amorphous"),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKey("bolt"),
		EndingKey:        kv.NewStringKey("bunt"),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 8192,
		StartingKey:      kv.NewStringKey("consensus"),
		EndingKey:        kv.NewStringKey("distributed"),
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
		BlockBeginOffset: 0,
		StartingKey:      kv.NewStringKey("accurate"),
		EndingKey:        kv.NewStringKey("badger"),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKey("bolt"),
		EndingKey:        kv.NewStringKey("calculator"),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 8192,
		StartingKey:      kv.NewStringKey("consensus"),
		EndingKey:        kv.NewStringKey("distributed"),
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

func TestBlockMetaListGetBlockContainingTheKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKey("bolt")})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKey("bolt"))
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, 1, blockIndex)
}

func TestBlockMetaListGetBlockContainingTheKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKey("bolt")})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKey("db")})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKey("exact")})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKey("accurate"))
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
	assert.Equal(t, 0, blockIndex)
}

func TestBlockMetaListGetBlockContainingTheKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKey("bolt")})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKey("db")})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKey("exact")})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKey("exact"))
	assert.Equal(t, "exact", meta.StartingKey.RawString())
	assert.Equal(t, 3, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKey("bolt")})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKey("db")})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKey("exact")})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKey("consensus"))
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, 1, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKey("consensus")})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKey("distributed")})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKey("etcd")})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKey("contribute"))
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, 0, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKey("consensus"), EndingKey: kv.NewStringKey("demo")})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKey("contribute"))
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, "demo", meta.EndingKey.RawString())
	assert.Equal(t, 0, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey4(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKey("bolt")})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKey("db")})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKey("exact")})
	blockMetaList.Add(Meta{BlockBeginOffset: 80, StartingKey: kv.NewStringKey("foundation")})
	blockMetaList.Add(Meta{BlockBeginOffset: 100, StartingKey: kv.NewStringKey("gossip")})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKey("group"))
	assert.Equal(t, "gossip", meta.StartingKey.RawString())
	assert.Equal(t, 5, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey5(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKey("bolt")})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKey("db")})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKey("exact")})
	blockMetaList.Add(Meta{BlockBeginOffset: 80, StartingKey: kv.NewStringKey("foundation")})
	blockMetaList.Add(Meta{BlockBeginOffset: 100, StartingKey: kv.NewStringKey("gossip")})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKey("yugabyte"))
	assert.Equal(t, "gossip", meta.StartingKey.RawString())
	assert.Equal(t, 5, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey6(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKey("bolt")})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKey("db")})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKey("exact")})
	blockMetaList.Add(Meta{BlockBeginOffset: 80, StartingKey: kv.NewStringKey("foundation")})
	blockMetaList.Add(Meta{BlockBeginOffset: 100, StartingKey: kv.NewStringKey("gossip")})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKey("fixed"))
	assert.Equal(t, "exact", meta.StartingKey.RawString())
	assert.Equal(t, 3, blockIndex)
}

//TODO: tests for may contain ..
