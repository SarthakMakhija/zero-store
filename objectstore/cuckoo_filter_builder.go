package objectstore

import (
	"github.com/SarthakMakhija/zero-store/kv"
	cuckoo "github.com/seiflotfy/cuckoofilter"
)

const (
	cuckooFilterCapacity           = 10_00_000
	cuckooFilterCapacityMultiplier = 10
)

// FilterBuilder represents a cuckoo filter builder.
type FilterBuilder struct {
	keys []kv.Key
}

// NewCuckooFilterBuilder creates a new instance of cuckoo filter builder.
func NewCuckooFilterBuilder() *FilterBuilder {
	return &FilterBuilder{}
}

// Add adds the given key to its collection.
func (builder *FilterBuilder) Add(key kv.Key) {
	builder.keys = append(builder.keys, key)
}

// Build builds a new cuckoo filter.
func (builder *FilterBuilder) Build() *cuckoo.Filter {
	capacity := cuckooFilterCapacity
	if len(builder.keys) >= cuckooFilterCapacity {
		capacity = len(builder.keys) * cuckooFilterCapacityMultiplier
	}
	filter := cuckoo.NewFilter(uint(capacity))
	for _, key := range builder.keys {
		filter.Insert(key.RawBytes())
	}
	return filter
}
