//go:build test

package state

// hasPersistentSortedSegmentFor returns true if there is a persistent-sorted segment for the given segment id.
func (state *StorageState) hasPersistentSortedSegmentFor(id uint64) bool {
	return state.persistentSortedSegments.HasPersistentSortedSegmentFor(id)
}

// removeAllPersistentSortedSegmentsIn removes the persistent sorted segment file.
func (state *StorageState) removeAllPersistentSortedSegmentsIn(directory string) {
	state.persistentSortedSegments.RemoveAllPersistentSortedSegmentsIn(directory)
}
