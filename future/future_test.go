package future

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestFutureWithOkStatus(t *testing.T) {
	future := NewFuture()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		future.Wait()

		assert.True(t, future.isDone)
		assert.True(t, future.Status().IsOk())
	}()

	future.MarkDoneAsOk()
	wg.Wait()
}

func TestFutureWithErrorStatus(t *testing.T) {
	future := NewFuture()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		future.Wait()

		assert.True(t, future.isDone)
		assert.True(t, future.Status().IsError())
		assert.Equal(t, "test error", future.status.Error().Error())
	}()

	future.MarkDoneAsError(errors.New("test error"))
	wg.Wait()
}
