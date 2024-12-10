package state

import "sync"

// SegmentIdGenerator represents a mechanism to generate the Ids for Segments.
type SegmentIdGenerator struct {
	idLock sync.Mutex
	nextId uint64
}

// NewSegmentIdGenerator creates a new instance of SegmentIdGenerator.
func NewSegmentIdGenerator() *SegmentIdGenerator {
	return &SegmentIdGenerator{}
}

// NextId generates the next id. It uses sync.Mutex to generate next id.
func (generator *SegmentIdGenerator) NextId() uint64 {
	generator.idLock.Lock()
	defer generator.idLock.Unlock()

	generator.nextId = generator.nextId + 1
	return generator.nextId
}
