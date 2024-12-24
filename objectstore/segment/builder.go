package segment

import (
	"bytes"
	"fmt"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/SarthakMakhija/zero-store/objectstore/filter"
)

// SortedSegmentBuilder allows building persistent sorted segment in a step-by-step manner.
type SortedSegmentBuilder struct {
	blockBuilder       *block.Builder
	blockMetaList      *block.MetaList
	bloomFilterBuilder *filter.BloomFilterBuilder
	startingKey        kv.Key
	endingKey          kv.Key
	allBlocksData      []byte
	blockSize          uint
	store              objectstore.Store
}

// NewSortedSegmentBuilderWithDefaultBlockSize creates a new instance of SortedSegmentBuilder with block.DefaultBlockSize.
func NewSortedSegmentBuilderWithDefaultBlockSize(store objectstore.Store, enableCompression bool) *SortedSegmentBuilder {
	return NewSortedSegmentBuilder(store, block.DefaultBlockSize, enableCompression)
}

// NewSortedSegmentBuilder creates a new instance of SortedSegmentBuilder with the given block size.
// The specified block size will be used to limit the size of each block that will be a part of the final sorted segment.
func NewSortedSegmentBuilder(store objectstore.Store, blockSize uint, enableCompression bool) *SortedSegmentBuilder {
	return &SortedSegmentBuilder{
		blockBuilder:       block.NewBlockBuilder(blockSize),
		blockMetaList:      block.NewBlockMetaList(enableCompression),
		bloomFilterBuilder: filter.NewBloomFilterBuilder(),
		blockSize:          blockSize,
		store:              store,
	}
}

// Add adds the key/value pair in the current block builder.
// Add involves:
// 1) Keeping a track of the starting key and ending key of the current block.
// 2) Adding the key to the filter.BloomFilter.
// 3) Adding the key/value pair to the current block.Builder.
// 4) Finishing the current block, if it is full and starting a new block (or block.Builder).
func (builder *SortedSegmentBuilder) Add(key kv.Key, value kv.Value) {
	if builder.startingKey.IsRawKeyEmpty() {
		builder.startingKey = key
	}
	builder.endingKey = key
	builder.bloomFilterBuilder.Add(key)
	if builder.blockBuilder.Add(key, value) {
		return
	}
	builder.finishBlock()
	builder.startNewBlockBuilder(key)
	builder.blockBuilder.Add(key, value)
}

// Build builds the SortedSegment using the given segment id.
// It involves the following:
// 1) Encoding the blocks of SortedSegment.
// 2) Writing the entire SortedSegment to object storage.
// 3) Creating an instance of SortedSegment.
// The encoding of the SortedSegment looks like:
/**
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| data block | data block |...| data block | metadata section |  bloom filter section | footer block 																		   |
|										   |				  |			              | blockMetaBeginOffset, blockMetaEndOffset, bloomFilterBeginOffset, bloomFilterEndOffset |
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
*/
//
// The size of the data blocks is fixed, defaults to block.DefaultBlockSize.
// Metadata and bloom filter are variable length byte sections.
// Footer block is a fixed size block, defaults to block.DefaultBlockSize.
func (builder *SortedSegmentBuilder) Build(id uint64) (*SortedSegment, error) {
	blockMetaBeginOffset := func() uint32 {
		return uint32(len(builder.allBlocksData))
	}
	blockMetaEndOffset := func(buffer *bytes.Buffer) uint32 {
		return uint32(buffer.Len())
	}
	bloomFilterBeginOffset := func(buffer *bytes.Buffer) uint32 {
		return uint32(buffer.Len())
	}
	bloomFilterEndOffset := func(buffer *bytes.Buffer) uint32 {
		return uint32(buffer.Len())
	}

	builder.finishBlock()

	buffer := new(bytes.Buffer)
	footerBlock := block.NewFooterBlock(builder.blockSize)

	buffer.Write(builder.allBlocksData)
	buffer.Write(builder.blockMetaList.Encode())

	footerBlock.AddOffset(blockMetaBeginOffset())
	footerBlock.AddOffset(blockMetaEndOffset(buffer))

	bloomFilter := builder.bloomFilterBuilder.Build()
	encodedFilter, err := bloomFilter.Encode()
	if err != nil {
		return nil, err
	}
	footerBlock.AddOffset(bloomFilterBeginOffset(buffer))
	buffer.Write(encodedFilter)

	footerBlock.AddOffset(bloomFilterEndOffset(buffer))
	buffer.Write(footerBlock.Encode())

	// write the result to the object store.
	if err := builder.store.Set(PathSuffixForSegment(id), buffer.Bytes()); err != nil {
		return nil, err
	}
	startingKey, _ := builder.blockMetaList.StartingKeyOfFirstBlock()
	endingKey, _ := builder.blockMetaList.EndingKeyOfLastBlock()
	return &SortedSegment{
		id:                   id,
		blockMetaList:        builder.blockMetaList,
		bloomFilter:          bloomFilter,
		blockMetaBeginOffset: uint32(len(builder.allBlocksData)),
		blockSize:            builder.blockSize,
		startingKey:          startingKey,
		endingKey:            endingKey,
		store:                builder.store,
	}, nil
}

// PathSuffixForSegment returns the segment object path suffix which is of the form: <id>.segment.
func PathSuffixForSegment(id uint64) string {
	return fmt.Sprintf("%v.segment", id)
}

// finishBlock finishes the current block. It involves:
// 1) Encoding the current block.
// 2) Storing the block.Meta in the block meta-list.
// 3) Collecting the encoded data of the current block in allBlocksData.
func (builder *SortedSegmentBuilder) finishBlock() {
	encodedBlock := builder.blockBuilder.Build().Encode()
	builder.blockMetaList.Add(block.Meta{
		BlockBeginOffset: uint32(len(builder.allBlocksData)),
		StartingKey:      builder.startingKey,
		EndingKey:        builder.endingKey,
	})
	builder.allBlocksData = append(builder.allBlocksData, encodedBlock...)
}

// startNewBlockBuilder creates a new instance of block.Builder.
func (builder *SortedSegmentBuilder) startNewBlockBuilder(key kv.Key) {
	builder.blockBuilder = block.NewBlockBuilder(builder.blockSize)
	builder.startingKey = key
	builder.endingKey = key
}
