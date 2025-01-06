package coordination

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestWorkUnitTimestampMarkWithASingleWorkUnit(t *testing.T) {
	workUnitTimestampMark := NewWorkUnitTimestampWaterMark()
	workUnitTimestampMark.Begin(1)
	workUnitTimestampMark.Finish(1)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(1), workUnitTimestampMark.DoneTill())
}

func TestWorkUnitTimestampMarkWithTwoWorkUnits(t *testing.T) {
	workUnitTimestampMark := NewWorkUnitTimestampWaterMark()
	workUnitTimestampMark.Begin(1)
	workUnitTimestampMark.Begin(2)

	workUnitTimestampMark.Finish(2) //finish the work at timestamp 2 first
	workUnitTimestampMark.Finish(1) //finish the work at timestamp 1 after the work at timestamp 2

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(2), workUnitTimestampMark.DoneTill())
}

func TestWorkUnitTimestampMarkWithAFewWorkUnits(t *testing.T) {
	workUnitTimestampMark := NewWorkUnitTimestampWaterMark()
	workUnitTimestampMark.Begin(1)
	workUnitTimestampMark.Begin(1)
	workUnitTimestampMark.Begin(1)
	workUnitTimestampMark.Begin(2)

	workUnitTimestampMark.Finish(2)
	workUnitTimestampMark.Finish(1)
	workUnitTimestampMark.Finish(1)
	workUnitTimestampMark.Finish(1)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(2), workUnitTimestampMark.DoneTill())
}

func TestWorkUnitTimestampMarkWithTwoConcurrentWorkUnits(t *testing.T) {
	workUnitTimestampMark := NewWorkUnitTimestampWaterMark()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		workUnitTimestampMark.Begin(1)
		workUnitTimestampMark.Finish(1)
	}()

	time.Sleep(5 * time.Millisecond)

	go func() {
		defer wg.Done()
		workUnitTimestampMark.Begin(2)
		workUnitTimestampMark.Finish(2)
	}()

	wg.Wait()

	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, uint64(2), workUnitTimestampMark.DoneTill())
}

func TestWorkUnitTimestampMarkWithConcurrentWorkUnits(t *testing.T) {
	workUnitTimestampMark := NewWorkUnitTimestampWaterMark()

	var wg sync.WaitGroup
	wg.Add(100)

	for count := 1; count <= 100; count++ {
		go func(index uint64) {
			defer wg.Done()
			workUnitTimestampMark.Begin(index)
			workUnitTimestampMark.Finish(index)
		}(uint64(count))
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()

	assert.NoError(t, workUnitTimestampMark.WaitForMark(context.Background(), 100))
	assert.Equal(t, uint64(100), workUnitTimestampMark.DoneTill())
}

func TestWorkUnitMarkAndWaitForATimestamp(t *testing.T) {
	workUnitTimestampMark := NewWorkUnitTimestampWaterMark()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		workUnitTimestampMark.Begin(1)
		time.Sleep(10 * time.Millisecond)
		workUnitTimestampMark.Finish(1)
	}()

	err := workUnitTimestampMark.WaitForMark(context.Background(), 1)
	assert.Nil(t, err)

	wg.Wait()
}

func TestWorkUnitMarkAndWaitForAnAlreadyFinishedTimestamp(t *testing.T) {
	workUnitTimestampMark := NewWorkUnitTimestampWaterMark()
	workUnitTimestampMark.Begin(1)
	workUnitTimestampMark.Finish(1)

	err := workUnitTimestampMark.WaitForMark(context.Background(), 1)
	assert.Nil(t, err)
}

func TestWorkUnitMarkAndTimeoutWaitingForAnUnfinishedTimestamp(t *testing.T) {
	workUnitTimestampMark := NewWorkUnitTimestampWaterMark()
	workUnitTimestampMark.Begin(1)
	workUnitTimestampMark.Finish(1)

	ctx, cancelFunction := context.WithTimeout(context.Background(), 15*time.Millisecond)
	err := workUnitTimestampMark.WaitForMark(ctx, 2)

	assert.Error(t, err)
	cancelFunction()
}
