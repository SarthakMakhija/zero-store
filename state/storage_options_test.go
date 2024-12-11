package state

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStorageOptionsWithSortedSegmentSize(t *testing.T) {
	storageOptions := NewStorageOptionsBuilder().WithSortedSegmentSizeInBytes(2 << 10).Build()
	assert.Equal(t, int64(2<<10), storageOptions.sortedSegmentSizeInBytes)
}
