//go:build test

package state

import (
	objectStore "github.com/SarthakMakhija/zero-store/objectstore/segment"
	"os"
	"path/filepath"
	"slices"
)

// hasInactiveSegments returns true if there are inactive segments, it is only for testing.
func (state *StorageState) hasInactiveSegments() bool {
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

// hasPersistentSortedSegmentFor returns true if there is a persistent-sorted segment for the given segment id.
func (state *StorageState) hasPersistentSortedSegmentFor(id uint64) bool {
	_, ok := state.persistentSegments[id]
	return ok
}

// removeAllPersistentSortedSegmentsIn removes the persistent sorted segment file.
func (state *StorageState) removeAllPersistentSortedSegmentsIn(directory string) {
	for segmentId, _ := range state.persistentSegments {
		delete(state.persistentSegments, segmentId)
		_ = os.Remove(filepath.Join(directory, objectStore.PathSuffixForSegment(segmentId)))
	}
}
