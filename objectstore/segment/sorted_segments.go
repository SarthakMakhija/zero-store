package segment

import (
	"github.com/SarthakMakhija/zero-store/iterator"
	"github.com/SarthakMakhija/zero-store/objectstore"
)

type SortedSegments struct {
	persistentSegments map[uint64]*SortedSegment
	store              objectstore.Store
}

func NewSortedSegments(store objectstore.Store) *SortedSegments {
	return &SortedSegments{
		persistentSegments: make(map[uint64]*SortedSegment),
		store:              store,
	}
}

func (sortedSegments *SortedSegments) BuildAndWritePersistentSortedSegment(iterator iterator.Iterator, segmentId uint64, enableBlockCompression bool) (*SortedSegment, error) {
	sortedSegmentBuilder := NewSortedSegmentBuilderWithDefaultBlockSize(sortedSegments.store, enableBlockCompression)
	for iterator.IsValid() {
		sortedSegmentBuilder.Add(iterator.Key(), iterator.Value())
		if err := iterator.Next(); err != nil {
			return nil, err
		}
	}
	persistentSortedSegment, err := sortedSegmentBuilder.Build(segmentId)
	if err != nil {
		return nil, err
	}
	sortedSegments.persistentSegments[segmentId] = persistentSortedSegment
	return persistentSortedSegment, nil
}
