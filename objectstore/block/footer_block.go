package block

import (
	"encoding/binary"
)

// FooterBlock is the footer block of the persistent sorted segment.
type FooterBlock struct {
	blockSize uint
	offsets   []uint32
}

// NewFooterBlock creates a new footer block.
// TODO: what if the block size is big like 1Mib?
func NewFooterBlock(blockSize uint) *FooterBlock {
	return &FooterBlock{
		blockSize: blockSize,
	}
}

// AddOffset adds the offset in the footer block.
// Footer block contains the following information:
// - begin-offset of the block meta information
// - end-offset of the block meta information
// - begin-offset of the bloom filter
// - end-offset of the bloom filter
// This method does not check if the footer block has sufficient space to contain the given offset.
// At this stage, the block does not contain too much information, so this check is left out.
func (footerBlock *FooterBlock) AddOffset(offset uint32) {
	footerBlock.offsets = append(footerBlock.offsets, offset)
}

// GetOffsetAsInt64At returns the offset at the given index.
// If the index is beyond the total available indices for offsets, 0, false is returned
func (footerBlock *FooterBlock) GetOffsetAsInt64At(index uint) (int64, bool) {
	offset, ok := footerBlock.GetOffsetAt(index)
	if !ok {
		return 0, false
	}
	return int64(offset), true
}

// GetOffsetAt returns the offset at the given index.
// If the index is beyond the total available indices for offsets, 0, false is returned
func (footerBlock *FooterBlock) GetOffsetAt(index uint) (uint32, bool) {
	if index >= uint(len(footerBlock.offsets)) {
		return 0, false
	}
	return footerBlock.offsets[index], true
}

// Encode encodes the FooterBlock as byte slice.
// Encoding includes:
/*
  -----------------------------------------------------------
 | 2 bytes for the number of offsets | 4 bytes for an offset |
  -----------------------------------------------------------
                                    <----for each offset---->
*/
func (footerBlock *FooterBlock) Encode() []byte {
	buffer := make([]byte, footerBlock.blockSize)
	binary.LittleEndian.PutUint16(buffer[:], uint16(len(footerBlock.offsets)))

	index := Uint16Size
	for _, offset := range footerBlock.offsets {
		binary.LittleEndian.PutUint32(buffer[index:], offset)
		index += Uint32Size
	}
	return buffer
}

// DecodeToFooterBlock decodes the byte slice and returns an instance of FooterBlock.
func DecodeToFooterBlock(buffer []byte, blockSize uint) *FooterBlock {
	numberOfOffsets := binary.LittleEndian.Uint16(buffer[:])
	offsets := make([]uint32, 0, numberOfOffsets)

	indexInBuffer := Uint16Size
	for offsetIndex := 0; offsetIndex < int(numberOfOffsets); offsetIndex++ {
		offsets = append(offsets, binary.LittleEndian.Uint32(buffer[indexInBuffer:]))
		indexInBuffer += Uint32Size
	}
	return &FooterBlock{
		offsets:   offsets,
		blockSize: blockSize,
	}
}
