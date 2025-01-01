package iterator

import (
	"errors"
	"github.com/SarthakMakhija/zero-store/kv"
)

// NothingIterator is a no-operation iterator.
// It is created from MergeIterator, if all the iterators passed to the MergeIterator are invalid.
type NothingIterator struct{}

var errNoNextSupportedByNothingIterator = errors.New("no support for Next() by NothingIterator")

var nothingIterator = &NothingIterator{}

// Key returns kv.EmptyKey.
func (iterator *NothingIterator) Key() kv.Key {
	return kv.EmptyKey
}

// Value returns kv.EmptyValue.
func (iterator *NothingIterator) Value() kv.Value {
	return kv.EmptyValue
}

// Next returns an error errNoNextSupportedByNothingIterator.
func (iterator *NothingIterator) Next() error {
	return errNoNextSupportedByNothingIterator
}

// IsValid returns false.
func (iterator *NothingIterator) IsValid() bool {
	return false
}

// Close does nothing.
func (iterator *NothingIterator) Close() {}
