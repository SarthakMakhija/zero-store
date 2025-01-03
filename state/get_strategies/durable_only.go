package get_strategies

import (
	"github.com/SarthakMakhija/zero-store/iterator"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/objectstore/segment"
	"iter"
)

type DurableOnlyGet struct {
	segments                   *segment.SortedSegments
	persistentSegmentsSequence iter.Seq2[int, *segment.SortedSegment]
}

func NewDurableOnlyGet(segments *segment.SortedSegments, persistentSegmentsSequence iter.Seq2[int, *segment.SortedSegment]) DurableOnlyGet {
	return DurableOnlyGet{
		segments:                   segments,
		persistentSegmentsSequence: persistentSegmentsSequence,
	}
}

func (getOperation DurableOnlyGet) Get(key kv.Key) GetResponse {
	mergeIterator, err := getOperation.mergeAllIteratorsFor(key)
	if err != nil {
		return errorResponse(err)
	}
	boundedIterator := iterator.NewInclusiveBoundedIterator(mergeIterator, key)
	defer boundedIterator.Close()

	if boundedIterator.IsValid() && boundedIterator.Key().IsRawKeyEqualTo(key) {
		return positiveResponse(boundedIterator.Value())
	}
	return negativeResponse()
}

func (getOperation DurableOnlyGet) mergeAllIteratorsFor(key kv.Key) (*iterator.MergeIterator, error) {
	var iterators []iterator.Iterator
	for _, sortedSegment := range getOperation.persistentSegmentsSequence {
		mayContain, err := getOperation.segments.MayContain(key, sortedSegment)
		if err != nil {
			return nil, err
		}
		if mayContain {
			segmentIterator, err := getOperation.segments.SeekToKey(key, sortedSegment)
			if err != nil {
				return nil, err
			}
			iterators = append(iterators, segmentIterator)
		}
	}
	return iterator.NewMergeIterator(iterators), nil
}
