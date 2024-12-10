package external

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPutAndGetTheKey(t *testing.T) {
	skipList := NewSkipList(1 << 10)
	skipList.Put(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	value, ok := skipList.Get(kv.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestGetANonExistingKey(t *testing.T) {
	skipList := NewSkipList(1 << 10)

	value, ok := skipList.Get(kv.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestIterateOverSkipList(t *testing.T) {
	skipList := NewSkipList(1 << 10)

	skipList.Put(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))
	skipList.Put(kv.NewStringKey("bolt"), kv.NewStringValue("kv"))
	skipList.Put(kv.NewStringKey("badger"), kv.NewStringValue("LSM"))

	iterator := skipList.NewIterator()
	iterator.SeekToFirst()

	defer func() {
		_ = iterator.Close()
	}()

	assert.True(t, iterator.Valid())
	assert.Equal(t, kv.NewStringKey("badger"), iterator.Key())
	assert.Equal(t, kv.NewStringValue("LSM"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, kv.NewStringKey("bolt"), iterator.Key())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, kv.NewStringKey("consensus"), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	iterator.Next()
	assert.False(t, iterator.Valid())
}
