package state

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateFirstSegmentId(t *testing.T) {
	generator := NewSegmentIdGenerator()
	assert.Equal(t, uint64(1), generator.NextId())
}

func TestGenerateNextSegmentId(t *testing.T) {
	generator := NewSegmentIdGenerator()
	assert.Equal(t, uint64(1), generator.NextId())
	assert.Equal(t, uint64(2), generator.NextId())
	assert.Equal(t, uint64(3), generator.NextId())
}
