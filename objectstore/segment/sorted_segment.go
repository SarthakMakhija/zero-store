package segment

import (
	"fmt"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/SarthakMakhija/zero-store/objectstore/filter"
)

// SortedSegment is the on-disk representation of the memory.SortedSegment on object store.
// A persistent SortedSegment contains the data sorted by key.
// The abstraction SortedSegment does not contain the data, it mainly contains the bloom filter (filter.BloomFilter) and
// block meta-list (block.MetaList).
type SortedSegment struct {
	id                   uint64
	blockMetaList        *block.MetaList
	bloomFilter          *filter.BloomFilter
	blockMetaBeginOffset uint32
	blockSize            uint
	startingKey          kv.Key
	endingKey            kv.Key
	store                objectstore.Store
}

// MayContain uses bloom filter to determine if the given key maybe present in the SortedSegment.
// Returns true if the key MAYBE present, false otherwise.
func (segment *SortedSegment) MayContain(key kv.Key) bool {
	return segment.bloomFilter.MayContain(key)
}

// Id returns the id of SortedSegment.
func (segment *SortedSegment) Id() uint64 {
	return segment.id
}

// readBlock reads the block at the given blockIndex.
func (segment *SortedSegment) readBlock(blockIndex int) (block.Block, error) {
	startingOffset, endOffset := segment.offsetRangeOfBlockAt(blockIndex)
	buffer, err := segment.store.GetRange(pathSuffix(segment.id), int64(startingOffset), int64(endOffset-startingOffset))
	if err != nil {
		return block.Block{}, err
	}
	return block.DecodeToBlock(buffer), nil
}

// offsetRangeOfBlockAt returns the byte offset range of the block at the given index.
// offsetRangeOfBlockAt works by getting the block.Meta at the given index, and block.Meta at index + 1 (next block meta).
// If the block.Meta is available at the next index, it returns the BlockBeginOffset of block.Meta at the given index,
// and BlockBeginOffset of the block.Meta at index + 1.
// If the block.Meta is not available at the next index, it returns the BlockBeginOffset of block.Meta at the given index,
// and table.blockMetaOffsetMarker, which is essentially the offset which denotes the meta starting offset.
// Please take a look at the segment.SortedSegmentBuilder for encoding of SortedSegment.
func (segment *SortedSegment) offsetRangeOfBlockAt(blockIndex int) (uint32, uint32) {
	blockMeta, blockPresent := segment.blockMetaList.GetAt(blockIndex)
	if !blockPresent {
		panic(fmt.Errorf("block meta not found at index %v", blockIndex))
	}
	nextBlockMeta, nextBlockPresent := segment.blockMetaList.GetAt(blockIndex + 1)

	var endOffset uint32
	if nextBlockPresent {
		endOffset = nextBlockMeta.BlockBeginOffset
	} else {
		endOffset = segment.blockMetaBeginOffset
	}
	return blockMeta.BlockBeginOffset, endOffset
}
