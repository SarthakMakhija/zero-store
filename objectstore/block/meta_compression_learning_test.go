package block

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestCompressionRatio is less of a unit-test, but more of a learning test which highlights the results of compression of MetaList.
// This test considers that there are 100 data blocks and each block contains the starting key of size 22 bytes key, and ending key of
// size 37 bytes.
func TestCompressionRatio(t *testing.T) {
	blockMetaList := NewBlockMetaList(doNotEnableCompression)
	for blockCount := 0; blockCount < 100; blockCount++ {
		blockMetaList.Add(Meta{
			BlockStartingOffset: uint32(4196 * blockCount),
			StartingKey:         kv.NewStringKey("zero disk architecture"),
			EndingKey:           kv.NewStringKey("zero disk architecture is interesting"),
		})
	}
	uncompressedBuffer := blockMetaList.Encode()

	blockMetaList = NewBlockMetaList(enableCompression)
	for blockCount := 0; blockCount < 100; blockCount++ {
		blockMetaList.Add(Meta{
			BlockStartingOffset: uint32(4196 * blockCount),
			StartingKey:         kv.NewStringKey("zero disk architecture"),
			EndingKey:           kv.NewStringKey("zero disk architecture is interesting"),
		})
	}
	compressedBuffer := blockMetaList.Encode()

	println("Length of uncompressed buffer:", len(uncompressedBuffer))
	println("Length of compressed buffer:", len(compressedBuffer))
	assert.True(t, len(compressedBuffer) < len(uncompressedBuffer))
}
