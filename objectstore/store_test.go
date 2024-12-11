package objectstore

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestSetTheObjectToStore(t *testing.T) {
	objectName := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.Nil(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(objectName)
	}()

	assert.Nil(t, store.Set(objectName, []byte("raft is a consensus protocol")))
}

func TestSetAndGetTheObject(t *testing.T) {
	objectName := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.Nil(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(objectName)
	}()

	assert.Nil(t, store.Set(objectName, []byte("paxos is also a consensus protocol")))

	buffer, err := store.Get(objectName)
	assert.Nil(t, err)

	assert.Equal(t, "paxos is also a consensus protocol", string(buffer))
}

func TestAttemptToStoreAnObjectAtAnExistingPath(t *testing.T) {
	objectName := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.Nil(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(objectName)
	}()

	assert.Nil(t, store.Set(objectName, []byte("raft is a consensus protocol")))

	err = store.Set(objectName, []byte("raft is a consensus protocol"))
	assert.True(t, errors.Is(err, errObjectExists))
}

func TestGetRangeOfAnObjectFromFirstOffset(t *testing.T) {
	objectName := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.Nil(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(objectName)
	}()

	assert.Nil(t, store.Set(objectName, []byte("VSR is a consensus protocol")))

	buffer, err := store.GetRange(objectName, 0, 3)
	assert.Nil(t, err)

	assert.Equal(t, "VSR", string(buffer))
}

func TestGetRangeOfAnObjectFromSomeWhereInMiddle(t *testing.T) {
	objectName := t.Name()
	storeDefinition, err := NewFileSystemStoreDefinition(".")
	assert.Nil(t, err)

	store := NewStore(".", storeDefinition)
	defer func() {
		store.Close()
		_ = os.Remove(objectName)
	}()

	assert.Nil(t, store.Set(objectName, []byte("VSR is a protocol")))

	buffer, err := store.GetRange(objectName, 9, 5)
	assert.Nil(t, err)

	assert.Equal(t, "proto", string(buffer))
}
