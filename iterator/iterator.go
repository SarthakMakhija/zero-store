package iterator

import "github.com/SarthakMakhija/zero-store/kv"

// Iterator represents a common interface for all the iterators available in the system.
type Iterator interface {
	Key() kv.Key
	Value() kv.Value
	Next() error
	IsValid() bool
	Close()
}
