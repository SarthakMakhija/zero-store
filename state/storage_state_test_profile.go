//go:build test

package state

import "slices"

// HasInactiveSegments returns true if there are inactive segments, it is only for testing.
func (state *StorageState) HasInactiveSegments() bool {
	return len(state.inactiveSegments) > 0
}

// sortedSegmentIds returns the sorted segment ids,  it is only for testing.
func (state *StorageState) sortedSegmentIds() []uint64 {
	ids := make([]uint64, 0, 1+len(state.inactiveSegments))
	ids = append(ids, state.activeSegment.Id())
	for _, segment := range state.inactiveSegments {
		ids = append(ids, segment.Id())
	}
	slices.Sort(ids)
	return ids
}
