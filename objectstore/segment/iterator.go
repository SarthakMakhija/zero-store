package segment

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore/block"
)

// Iterator represents SortedSegment iterator.
// A SortedSegment consists of multiple data blocks, so blockIndex maintains the current block which is
// being iterated over.
// blockIterator is a pointer to the block.Iterator.
// Effectively, a SortedSegment Iterator is an iterator which iterates over the blocks of SortedSegment.
type Iterator struct {
	sortedSegment *SortedSegment
	blockIndex    int
	blockIterator *block.Iterator
	blockMetaList *block.MetaList
}

// Key returns the kv.Key from block.Iterator.
func (iterator *Iterator) Key() kv.Key {
	return iterator.blockIterator.Key()
}

// Value returns the kv.Value from block.Iterator.
func (iterator *Iterator) Value() kv.Value {
	return iterator.blockIterator.Value()
}

// IsValid returns true of the block.Iterator is valid.
func (iterator *Iterator) IsValid() bool {
	return iterator.blockIterator.IsValid()
}

// Next advance the block.Iterator to the next key/value within the current block, or
// move to the next block, if such a block exists.
func (iterator *Iterator) Next() error {
	if err := iterator.blockIterator.Next(); err != nil {
		return err
	}
	if !iterator.blockIterator.IsValid() {
		iterator.blockIndex += 1
		if iterator.blockIndex < iterator.sortedSegment.noOfBlocks() {
			readBlock, err := iterator.sortedSegment.readBlock(iterator.blockIndex, iterator.blockMetaList)
			if err != nil {
				return err
			}
			iterator.blockIterator = readBlock.SeekToFirst()
		}
	}
	return nil
}

// Close does nothing.
func (iterator *Iterator) Close() {}
