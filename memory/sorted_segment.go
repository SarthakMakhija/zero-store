package memory

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/memory/external"
)

// SortedSegment is an in-memory data structure which holds kv.Key and kv.Value pairs.
// SortedSegment uses [Skiplist](https://tech-lessons.in/en/blog/serializable_snapshot_isolation/#skiplist-and-mvcc) as its
// data structure.
// The Skiplist (external.SkipList) is shamelessly taken from [Badger](https://github.com/dgraph-io/badger).
// It is a lock-free implementation of Skiplist.
// It is important to have a lock-free implementation,
// otherwise scan operation will take lock(s) (/read-locks) which will start interfering with write operations.
type SortedSegment struct {
	id                 uint64
	allowedSizeInBytes int64
	entries            *external.SkipList
}

// NewSortedSegment creates a new instance of SortedSegment
func NewSortedSegment(id uint64, allowedSizeInBytes int64) *SortedSegment {
	return &SortedSegment{
		id:                 id,
		allowedSizeInBytes: allowedSizeInBytes,
		entries:            external.NewSkipList(allowedSizeInBytes),
	}
}

// Get returns the value for the key if found.
func (segment *SortedSegment) Get(key kv.Key) (kv.Value, bool) {
	value, ok := segment.entries.Get(key)
	if !ok || value.IsDeleted() {
		return kv.EmptyValue, false
	}
	return value, true
}

// Set sets the key/value pair in the system. It involves writing the key/value pair in the Skiplist.
func (segment *SortedSegment) Set(key kv.Key, value kv.Value) {
	segment.entries.Put(key, value)
}

// Delete is an append operation. It involves writing the key/value pair with kv.EmptyValue in the Skiplist.
func (segment *SortedSegment) Delete(key kv.Key) {
	segment.Set(key, kv.NewDeletedValue())
}

// AllEntries returns all the keys present in the Segment.
func (segment *SortedSegment) AllEntries(callback func(key kv.Key, value kv.Value)) {
	iterator := segment.entries.NewIterator()
	defer func() {
		_ = iterator.Close()
	}()
	for iterator.SeekToFirst(); iterator.Valid(); iterator.Next() {
		callback(iterator.Key(), iterator.Value())
	}
}

// IsEmpty returns true if the SortedSegment is empty.
func (segment *SortedSegment) IsEmpty() bool {
	return segment.entries.Empty()
}

// CanFit returns true if the SortedSegment has the size enough for the requiredSizeInBytes.
func (segment *SortedSegment) CanFit(requiredSizeInBytes int64) bool {
	return segment.sizeInBytes()+requiredSizeInBytes+int64(external.MaxNodeSize) < segment.allowedSizeInBytes
}

// Id returns the id of SortedSegment.
func (segment *SortedSegment) Id() uint64 {
	return segment.id
}

// sizeInBytes returns the size of the SortedSegment.
func (segment *SortedSegment) sizeInBytes() int64 {
	return segment.entries.MemSize()
}
