package iterator

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestPriorityOfIndexedIteratorBasedOnKey(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 10)},
		[]kv.Value{kv.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("distributed", 2)},
		[]kv.Value{kv.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOne.IsPrioritizedOver(indexedIteratorOther))
}

func TestPriorityOfIndexedIteratorBasedOnSameKeyWithDifferentIteratorIndex(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 5)},
		[]kv.Value{kv.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 5)},
		[]kv.Value{kv.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOne.IsPrioritizedOver(indexedIteratorOther))
}

func TestPriorityOfIndexedIteratorBasedOnSameKeyWithDifferentTimestamp(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 5)},
		[]kv.Value{kv.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 6)},
		[]kv.Value{kv.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOther.IsPrioritizedOver(indexedIteratorOne))
}
