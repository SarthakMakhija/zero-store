package table

import (
	"github.com/SarthakMakhija/zero-store/objectstore/block"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeAndDecodeAFooterBlockWithSingleOffset(t *testing.T) {
	footerBlock := NewFooterBlock(block.DefaultBlockSize)
	footerBlock.addOffset(18)

	encoded := footerBlock.encode()
	decodedFooterBlock := decodeFooterBlock(encoded, block.DefaultBlockSize)

	assert.Equal(t, uint32(18), decodedFooterBlock.offsets[0])
}

func TestEncodeAndDecodeAFooterBlockWithAFewOffsets(t *testing.T) {
	footerBlock := NewFooterBlock(block.DefaultBlockSize)
	footerBlock.addOffset(18)
	footerBlock.addOffset(240)
	footerBlock.addOffset(580)

	encoded := footerBlock.encode()
	decodedFooterBlock := decodeFooterBlock(encoded, block.DefaultBlockSize)

	assert.Equal(t, uint32(18), decodedFooterBlock.offsets[0])
	assert.Equal(t, uint32(240), decodedFooterBlock.offsets[1])
	assert.Equal(t, uint32(580), decodedFooterBlock.offsets[2])
}
