package objectstore

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValidStoreType(t *testing.T) {
	storeType := FileSystemStore
	assert.True(t, storeType.IsValid())
}

func TestInValidStoreType(t *testing.T) {
	var storeType StoreType = 0
	assert.False(t, storeType.IsValid())
}

func TestGetFileSystemStore(t *testing.T) {
	storeType := FileSystemStore
	_, err := storeType.GetStore(".")

	assert.NoError(t, err)
}

func TestAttemptToGetStoreBasedOnInvalidStoreType(t *testing.T) {
	var storeType StoreType = 0
	assert.Panics(t, func() {
		_, _ = storeType.GetStore(".")
	})
}
