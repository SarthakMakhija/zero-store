//go:build test

package segment

import (
	"os"
	"path/filepath"
)

// HasPersistentSortedSegmentFor returns true if there is a persistent-sorted segment for the given segment id.
func (sortedSegments *SortedSegments) HasPersistentSortedSegmentFor(id uint64) bool {
	_, ok := sortedSegments.persistentSegments[id]
	return ok
}

// RemoveAllPersistentSortedSegmentsIn removes the persistent sorted segment file.
func (sortedSegments *SortedSegments) RemoveAllPersistentSortedSegmentsIn(directory string) {
	for segmentId, _ := range sortedSegments.persistentSegments {
		delete(sortedSegments.persistentSegments, segmentId)
		_ = os.Remove(filepath.Join(directory, PathSuffixForSegment(segmentId)))
	}
}

// sortedSegmentFor returns the SortedSegment for the given segment id.
func (sortedSegments *SortedSegments) sortedSegmentFor(id uint64) SortedSegment {
	return sortedSegments.persistentSegments[id]
}
