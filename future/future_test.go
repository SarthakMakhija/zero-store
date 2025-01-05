package future

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestFutureWithOkStatus(t *testing.T) {
	asyncAwait := NewAsyncAwait[struct{}]()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		asyncAwait.Future().Wait()

		assert.True(t, asyncAwait.Future().isDone)
		assert.True(t, asyncAwait.Future().Status().IsOk())
	}()

	asyncAwait.MarkDoneAsOk()
	wg.Wait()
}

func TestFutureWithErrorStatus(t *testing.T) {
	asyncAwait := NewAsyncAwait[struct{}]()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		asyncAwait.Future().Wait()

		assert.True(t, asyncAwait.Future().isDone)
		assert.True(t, asyncAwait.Future().Status().IsError())
		assert.Equal(t, "test error", asyncAwait.Future().status.Error().Error())
	}()

	asyncAwait.MarkDoneAsError(errors.New("test error"))
	wg.Wait()
}
