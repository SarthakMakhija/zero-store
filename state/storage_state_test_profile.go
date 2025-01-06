//go:build test

package state

// RemoveAllPersistentSortedSegmentsIn removes the persistent sorted segment file.
func (state *StorageState) RemoveAllPersistentSortedSegmentsIn(directory string) {
	state.persistentSortedSegments.RemoveAllPersistentSortedSegmentsIn(directory)
}

// hasPersistentSortedSegmentFor returns true if there is a persistent-sorted segment for the given segment id.
func (state *StorageState) hasPersistentSortedSegmentFor(id uint64) bool {
	return state.persistentSortedSegments.HasPersistentSortedSegmentFor(id)
}
