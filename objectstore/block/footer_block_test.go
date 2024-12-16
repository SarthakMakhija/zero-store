package block

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeAndDecodeAFooterBlockWithSingleOffset(t *testing.T) {
	footerBlock := NewFooterBlock(DefaultBlockSize)
	footerBlock.AddOffset(18)

	encoded := footerBlock.Encode()
	decodedFooterBlock := DecodeToFooterBlock(encoded, DefaultBlockSize)

	assert.Equal(t, uint32(18), decodedFooterBlock.offsets[0])
}

func TestEncodeAndDecodeAFooterBlockWithAFewOffsets(t *testing.T) {
	footerBlock := NewFooterBlock(DefaultBlockSize)
	footerBlock.AddOffset(18)
	footerBlock.AddOffset(240)
	footerBlock.AddOffset(580)

	encoded := footerBlock.Encode()
	decodedFooterBlock := DecodeToFooterBlock(encoded, DefaultBlockSize)

	assert.Equal(t, uint32(18), decodedFooterBlock.offsets[0])
	assert.Equal(t, uint32(240), decodedFooterBlock.offsets[1])
	assert.Equal(t, uint32(580), decodedFooterBlock.offsets[2])
}

func TestGetOffsetAtValidIndex(t *testing.T) {
	footerBlock := NewFooterBlock(DefaultBlockSize)
	footerBlock.AddOffset(18)

	offset, ok := footerBlock.GetOffsetAsInt64At(0)
	assert.True(t, ok)
	assert.Equal(t, int64(18), offset)
}

func TestGetOffsetAtAnInvalidIndex(t *testing.T) {
	footerBlock := NewFooterBlock(DefaultBlockSize)
	footerBlock.AddOffset(18)

	offset, ok := footerBlock.GetOffsetAsInt64At(90)
	assert.False(t, ok)
	assert.Equal(t, int64(0), offset)
}
