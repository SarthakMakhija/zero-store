package block

import (
	"fmt"
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
		StartingKey:      kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:        kv.NewStringKeyWithTimestamp("consensus", 5),
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
		StartingKey:      kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:        kv.NewStringKeyWithTimestamp("badger", 3),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKeyWithTimestamp("bolt", 5),
		EndingKey:        kv.NewStringKeyWithTimestamp("calculator", 6),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 8192,
		StartingKey:      kv.NewStringKeyWithTimestamp("consensus", 5),
		EndingKey:        kv.NewStringKeyWithTimestamp("distributed", 6),
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
		StartingKey:      kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:        kv.NewStringKeyWithTimestamp("amorphous", 5),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKeyWithTimestamp("bolt", 6),
		EndingKey:        kv.NewStringKeyWithTimestamp("bunt", 8),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 8192,
		StartingKey:      kv.NewStringKeyWithTimestamp("consensus", 9),
		EndingKey:        kv.NewStringKeyWithTimestamp("distributed", 10),
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
		StartingKey:      kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:        kv.NewStringKeyWithTimestamp("badger", 5),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKeyWithTimestamp("bolt", 6),
		EndingKey:        kv.NewStringKeyWithTimestamp("calculator", 7),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 8192,
		StartingKey:      kv.NewStringKeyWithTimestamp("consensus", 8),
		EndingKey:        kv.NewStringKeyWithTimestamp("distributed", 10),
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
		StartingKey:      kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:        kv.NewStringKeyWithTimestamp("amorphous", 5),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKeyWithTimestamp("bolt", 6),
		EndingKey:        kv.NewStringKeyWithTimestamp("bunt", 8),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 8192,
		StartingKey:      kv.NewStringKeyWithTimestamp("consensus", 9),
		EndingKey:        kv.NewStringKeyWithTimestamp("distributed", 10),
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
		StartingKey:      kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:        kv.NewStringKeyWithTimestamp("badger", 5),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 4096,
		StartingKey:      kv.NewStringKeyWithTimestamp("bolt", 6),
		EndingKey:        kv.NewStringKeyWithTimestamp("calculator", 8),
	})
	blockMetaList.Add(Meta{
		BlockBeginOffset: 8192,
		StartingKey:      kv.NewStringKeyWithTimestamp("consensus", 9),
		EndingKey:        kv.NewStringKeyWithTimestamp("distributed", 10),
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
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 10)})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 11)})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("bolt", 10))
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, 1, blockIndex)
}

func TestBlockMetaListGetBlockContainingTheKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 5)})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 6)})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 8)})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("accurate", 2))
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
	assert.Equal(t, 0, blockIndex)
}

func TestBlockMetaListGetBlockContainingTheKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 5)})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 6)})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 8)})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("exact", 8))
	assert.Equal(t, "exact", meta.StartingKey.RawString())
	assert.Equal(t, 3, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 5)})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 6)})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 8)})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, 1, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("consensus", 2)})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("distributed", 5)})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("etcd", 6)})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("contribute", 6))
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, 0, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{
		BlockBeginOffset: 0,
		StartingKey:      kv.NewStringKeyWithTimestamp("consensus", 2),
		EndingKey:        kv.NewStringKeyWithTimestamp("demo", 5),
	})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("contribute", 3))
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, "demo", meta.EndingKey.RawString())
	assert.Equal(t, 0, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey4(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 3)})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 4)})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 5)})
	blockMetaList.Add(Meta{BlockBeginOffset: 80, StartingKey: kv.NewStringKeyWithTimestamp("foundation", 6)})
	blockMetaList.Add(Meta{BlockBeginOffset: 100, StartingKey: kv.NewStringKeyWithTimestamp("gossip", 7)})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("group", 8))
	assert.Equal(t, "gossip", meta.StartingKey.RawString())
	assert.Equal(t, 5, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey5(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 3)})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 4)})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 5)})
	blockMetaList.Add(Meta{BlockBeginOffset: 80, StartingKey: kv.NewStringKeyWithTimestamp("foundation", 6)})
	blockMetaList.Add(Meta{BlockBeginOffset: 100, StartingKey: kv.NewStringKeyWithTimestamp("gossip", 7)})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("yugabyte", 8))
	assert.Equal(t, "gossip", meta.StartingKey.RawString())
	assert.Equal(t, 5, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey6(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	blockMetaList.Add(Meta{BlockBeginOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockBeginOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 3)})
	blockMetaList.Add(Meta{BlockBeginOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 4)})
	blockMetaList.Add(Meta{BlockBeginOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 5)})
	blockMetaList.Add(Meta{BlockBeginOffset: 80, StartingKey: kv.NewStringKeyWithTimestamp("foundation", 6)})
	blockMetaList.Add(Meta{BlockBeginOffset: 100, StartingKey: kv.NewStringKeyWithTimestamp("gossip", 7)})

	meta, blockIndex := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("fixed", 6))
	assert.Equal(t, "exact", meta.StartingKey.RawString())
	assert.Equal(t, 3, blockIndex)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey7(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	for count := 2; count <= 8; count += 2 {
		key := fmt.Sprintf("key-%d", count)
		timestamp := uint64(count)
		blockMetaList.Add(Meta{
			BlockBeginOffset: uint32(count),
			StartingKey:      kv.NewStringKeyWithTimestamp(key, timestamp),
		})
	}

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("key-7", 7))
	assert.Equal(t, "key-6", meta.StartingKey.RawString())
	assert.Equal(t, 2, index)
}
