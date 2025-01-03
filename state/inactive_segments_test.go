package state

import (
	"github.com/SarthakMakhija/zero-store/memory"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestGetOldestInactiveSegment(t *testing.T) {
	segments := newInactiveSegments()
	segments.append(memory.NewSortedSegment(1, 1<<10))

	oldest, ok := segments.oldest()
	assert.True(t, ok)
	assert.Equal(t, uint64(1), oldest.Id())
}

func TestAttemptToGetOldestInactiveSegment(t *testing.T) {
	segments := newInactiveSegments()

	_, ok := segments.oldest()
	assert.False(t, ok)
}

func TestDropOldestInactiveSegment(t *testing.T) {
	segments := newInactiveSegments()
	segments.append(memory.NewSortedSegment(1, 1<<10))

	segments.dropOldest()
	assert.Equal(t, 0, len(segments.segments))
}

func TestMarkSortedSegmentsFlushToObjectStoreAsError(t *testing.T) {
	segments := newInactiveSegments()
	segment := memory.NewSortedSegment(1, 1<<10)
	segments.append(segment)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		future := segment.FlushToObjectStoreFuture()
		future.Wait()
		assert.Error(t, ErrDbStopped, future.Status().Error())
	}()

	segments.flushAllToObjectStoreMarkAsError()
	wg.Wait()
}

func TestCopyAllInactiveSegments(t *testing.T) {
	segments := newInactiveSegments()
	segments.append(memory.NewSortedSegment(1, 1<<10))
	segments.append(memory.NewSortedSegment(2, 1<<10))

	copied := segments.copySegments()
	assert.Equal(t, 2, len(copied))
	assert.Equal(t, uint64(1), copied[0].Id())
	assert.Equal(t, uint64(2), copied[1].Id())
}
