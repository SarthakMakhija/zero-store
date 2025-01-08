package coordination

import (
	"context"
	"github.com/SarthakMakhija/zero-store/state"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTheReadTimestamp(t *testing.T) {
	storageState, err := state.NewStorageState(state.NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	timeKeeper := NewTimeKeeper(NewExecutor(storageState))
	defer func() {
		storageState.Close()
		timeKeeper.Close()
	}()

	readTimestamp := timeKeeper.readTimestamp()
	assert.Equal(t, uint64(0), readTimestamp)
}

func TestGetTheReadTimestampAfterAPseudoCommit(t *testing.T) {
	storageState, err := state.NewStorageState(state.NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	timeKeeper := NewTimeKeeper(NewExecutor(storageState))
	defer func() {
		storageState.Close()
		timeKeeper.Close()
	}()

	commitTimestamp := uint64(5)
	timeKeeper.nextTimestamp = commitTimestamp + 1

	timeKeeper.writeTimestampMark.Finish(commitTimestamp)
	assert.Equal(t, uint64(5), timeKeeper.readTimestamp())
}

func TestGetTheMaxBeginTimestamp(t *testing.T) {
	storageState, err := state.NewStorageState(state.NewStorageOptionsBuilder().WithFileSystemStoreType(".").Build())
	assert.NoError(t, err)

	timeKeeper := NewTimeKeeper(NewExecutor(storageState))
	defer func() {
		storageState.Close()
		timeKeeper.Close()
	}()

	timeKeeper.FinishReadTimestamp(5)
	assert.Nil(t, timeKeeper.readTimestampMark.WaitForMark(context.Background(), 5))

	assert.Equal(t, uint64(5), timeKeeper.MaxBeginTimestamp())
}
