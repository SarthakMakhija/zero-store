//go:build test

package state

import "slices"

// HasInactiveSegments returns true if there are inactive segments, it is only for testing.
func (state *StorageState) HasInactiveSegments() bool {
	return len(state.inactiveSegments) > 0
}

// sortedInactiveSegmentIds returns the sorted segment ids for inactive segments,  it is only for testing.
func (state *StorageState) sortedInactiveSegmentIds() []uint64 {
	ids := make([]uint64, 0, len(state.inactiveSegments))
	for _, segment := range state.inactiveSegments {
		ids = append(ids, segment.Id())
	}
	slices.Sort(ids)
	return ids
}
