package coordination

import (
	"container/heap"
	"context"
	"sync/atomic"
)

// TimestampHeap
// https://pkg.go.dev/container/heap
type TimestampHeap []uint64

func (h TimestampHeap) Len() int           { return len(h) }
func (h TimestampHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h TimestampHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *TimestampHeap) Push(x any)        { *h = append(*h, x.(uint64)) }
func (h *TimestampHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Mark represents timestamp along with its status and notification channel.
type Mark struct {
	timestamp       uint64
	done            bool
	outNotification chan struct{}
}

// WorkUnitTimestampWaterMark keeps track of the timestamps that are processed.
// It could be readTimestamp or the commitTimestamp.
// Let's say a coordination.WorkUnit begins with a timestamp = 2.
// It will invoke Begin method to indicate that a coordination.WorkUnit with timestamp = 2 has started.
// At some later point in time, the same coordination.WorkUnit will end.
// Let's consider that it writes with the timestamp = 5. It will invoke Finish method passing 5 as the argument.
// This will indicate to the WorkUnitTimestampWaterMark that work-units up till timestamp = 5 are done.
// This information can be used for blocking new work-units until work-units upto a given timestamp are done.
// The idea is from [Badger](https://github.com/dgraph-io/badger).
type WorkUnitTimestampWaterMark struct {
	doneTill    atomic.Uint64
	markChannel chan Mark
	stopChannel chan struct{}
}

// NewWorkUnitTimestampWaterMark creates a new instance of WorkUnitTimestampWaterMark
func NewWorkUnitTimestampWaterMark() *WorkUnitTimestampWaterMark {
	workUnitWaterMark := &WorkUnitTimestampWaterMark{
		markChannel: make(chan Mark, 1024),
		stopChannel: make(chan struct{}),
	}
	go workUnitWaterMark.spin()
	return workUnitWaterMark
}

// Begin sends a mark to the markChannel indicating that a coordination.WorkUnit with the given timestamp has started.
func (watermark *WorkUnitTimestampWaterMark) Begin(timestamp uint64) {
	watermark.markChannel <- Mark{timestamp: timestamp, done: false}
}

// Finish sends a mark to the markChannel indicating that a coordination.WorkUnit with the given timestamp is done.
func (watermark *WorkUnitTimestampWaterMark) Finish(timestamp uint64) {
	watermark.markChannel <- Mark{timestamp: timestamp, done: true}
}

// Stop stops the WorkUnitTimestampWaterMark.
func (watermark *WorkUnitTimestampWaterMark) Stop() {
	watermark.stopChannel <- struct{}{}
}

// DoneTill returns the timestamp till which the processing is done.
func (watermark *WorkUnitTimestampWaterMark) DoneTill() uint64 {
	return watermark.doneTill.Load()
}

// WaitForMark is used to wait till the coordination.WorkUnit timestamp >= timestamp is processed.
// It does this by sending a mark to the `markChannel` and waiting for a response on the `waitChannel`.
func (watermark *WorkUnitTimestampWaterMark) WaitForMark(
	ctx context.Context,
	timestamp uint64,
) error {
	if watermark.DoneTill() >= timestamp {
		return nil
	}
	waitChannel := make(chan struct{})
	watermark.markChannel <- Mark{timestamp: timestamp, outNotification: waitChannel}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitChannel:
		return nil
	}
}

// spin processes all the marks that are received on the `markChannel` and is invoked as a single goroutine [`go spin()`].
// Any time it receives a mark, it invokes the `process` function, which determines if the timestamp in the mark is done or not.
// Let's consider the following case:
// Two work-units with endTimestamps 4, 6 are running.
// The coordination.WorkUnit with the endTimestamp 6 invokes Finish(), followed by the coordination.WorkUnit with the endTimestamp 4.
// WorkUnitTimestampWaterMark can not consider the coordination.WorkUnit with endTimestamp = 6 as done because a coordination.WorkUnit with the
// endTimestamp of 4 is not done yet.
// It maintains a binary heap of work-unit timestamps and anytime it identifies that a coordination.WorkUnit is done,
// the timestamp is popped off the heap and the doneTill field of WorkUnitTimestampWaterMark is updated.
// This ensures that doneTill mark is updated in the following order: 4 followed by 6.
func (watermark *WorkUnitTimestampWaterMark) spin() {
	var orderedWorkUnitTimestamps TimestampHeap
	pendingWorkUnitRequestsByTimestamp := make(map[uint64]int)
	notificationChannelsByTimestamp := make(map[uint64][]chan struct{})

	heap.Init(&orderedWorkUnitTimestamps)
	process := func(mark Mark) {
		previous, ok := pendingWorkUnitRequestsByTimestamp[mark.timestamp]
		if !ok {
			heap.Push(&orderedWorkUnitTimestamps, mark.timestamp)
		}

		pendingWorkUnitCount := 1
		if mark.done {
			pendingWorkUnitCount = -1
		}
		pendingWorkUnitRequestsByTimestamp[mark.timestamp] = previous + pendingWorkUnitCount

		doneTill := watermark.DoneTill()
		localDoneTillTimestamp := doneTill
		for len(orderedWorkUnitTimestamps) > 0 {
			minimumTimestamp := orderedWorkUnitTimestamps[0]
			if done := pendingWorkUnitRequestsByTimestamp[minimumTimestamp]; done > 0 {
				break
			}
			heap.Pop(&orderedWorkUnitTimestamps)
			delete(pendingWorkUnitRequestsByTimestamp, minimumTimestamp)
			localDoneTillTimestamp = minimumTimestamp
		}

		if localDoneTillTimestamp != doneTill {
			watermark.doneTill.CompareAndSwap(doneTill, localDoneTillTimestamp)
		}
		for timestamp, notificationChannels := range notificationChannelsByTimestamp {
			if timestamp <= localDoneTillTimestamp {
				for _, channel := range notificationChannels {
					close(channel)
				}
				delete(notificationChannelsByTimestamp, timestamp)
			}
		}
	}
	for {
		select {
		case mark := <-watermark.markChannel:
			if mark.outNotification != nil {
				doneTill := watermark.doneTill.Load()
				if doneTill >= mark.timestamp {
					close(mark.outNotification)
				} else {
					channels, ok := notificationChannelsByTimestamp[mark.timestamp]
					if !ok {
						notificationChannelsByTimestamp[mark.timestamp] = []chan struct{}{mark.outNotification}
					} else {
						notificationChannelsByTimestamp[mark.timestamp] = append(channels, mark.outNotification)
					}
				}
			} else {
				process(mark)
			}
		case <-watermark.stopChannel:
			close(watermark.markChannel)
			close(watermark.stopChannel)
			closeAll(notificationChannelsByTimestamp)
			return
		}
	}
}

// closeAll closes all the channels that are waiting on various timestamps.
func closeAll(notificationChannelsByTimestamp map[uint64][]chan struct{}) {
	for timestamp, notificationChannels := range notificationChannelsByTimestamp {
		for _, channel := range notificationChannels {
			close(channel)
		}
		delete(notificationChannelsByTimestamp, timestamp)
	}
}
