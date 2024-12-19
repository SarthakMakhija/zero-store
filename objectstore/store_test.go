package objectstore

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestValidStoreType(t *testing.T) {
	var storeType StoreType = 1
	assert.True(t, storeType.IsValid())
}

func TestInValidStoreType(t *testing.T) {
	var storeType StoreType = 0
	assert.False(t, storeType.IsValid())
}

func TestSetTheObjectToStore(t *testing.T) {
	pathSuffix := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix)
	}()

	assert.NoError(t, store.Set(pathSuffix, []byte("raft is a consensus protocol")))
}

func TestSetAndGetTheObject(t *testing.T) {
	pathSuffix := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix)
	}()

	assert.NoError(t, store.Set(pathSuffix, []byte("paxos is also a consensus protocol")))

	buffer, err := store.Get(pathSuffix)
	assert.NoError(t, err)

	assert.Equal(t, "paxos is also a consensus protocol", string(buffer))
}

func TestAttemptToStoreAnObjectAtAnExistingPath(t *testing.T) {
	pathSuffix := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix)
	}()

	assert.NoError(t, store.Set(pathSuffix, []byte("raft is a consensus protocol")))

	err = store.Set(pathSuffix, []byte("raft is a consensus protocol"))
	assert.True(t, errors.Is(err, errObjectExists))
}

func TestGetRangeOfAnObjectFromFirstOffset(t *testing.T) {
	pathSuffix := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix)
	}()

	assert.NoError(t, store.Set(pathSuffix, []byte("VSR is a consensus protocol")))

	buffer, err := store.GetRange(pathSuffix, 0, 3)
	assert.NoError(t, err)

	assert.Equal(t, "VSR", string(buffer))
}

func TestGetRangeOfAnObjectFromSomeWhereInMiddle(t *testing.T) {
	pathSuffix := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix)
	}()

	assert.NoError(t, store.Set(pathSuffix, []byte("VSR is a protocol")))

	buffer, err := store.GetRange(pathSuffix, 9, 5)
	assert.NoError(t, err)

	assert.Equal(t, "proto", string(buffer))
}

func TestGetSizeOfObject(t *testing.T) {
	pathSuffix := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.NoError(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(pathSuffix)
	}()

	assert.NoError(t, store.Set(pathSuffix, []byte("paxos is also a consensus protocol")))

	size, err := store.SizeInBytes(pathSuffix)
	assert.NoError(t, err)

	assert.Equal(t, int64(34), size)
}
