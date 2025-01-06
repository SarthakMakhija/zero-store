package coordination

import (
	"github.com/SarthakMakhija/zero-store/future"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/SarthakMakhija/zero-store/state"
	"sync"
)

const incomingChannelSize = 1 * 1024

// Executor is an implementation of [Singular Update Queue](https://martinfowler.com/articles/patterns-of-distributed-systems/singular-update-queue.html).
// Executor applies all the writes sequentially.
//
// It is a single goroutine that reads kv.TimestampedBatch from the incomingChannel.
// Anytime a work-unit is done, its kv.TimestampedBatch is sent to the Executor via the submit() method.
// Executor applies batches to the instance of state.StorageState sequentially.
type Executor struct {
	state                    *state.StorageState
	incomingChannel          chan ExecutionRequest
	stopChannel              chan struct{}
	stopOnce                 sync.Once
	emptyIncomingChannelLock sync.Mutex
}

// NewExecutor creates a new instance of Executor, and starts a single goroutine which will apply the writes sequentially.
// It is called once in the entire application.
func NewExecutor(state *state.StorageState) *Executor {
	executor := &Executor{
		state:           state,
		incomingChannel: make(chan ExecutionRequest, incomingChannelSize),
		stopChannel:     make(chan struct{}),
	}
	go executor.start()
	return executor
}

// start starts the executor.
// Everytime the executor receives an instance of kv.TimestampedBatch from incomingChannel, it applies it to the state.StorageState,
// and marks the corresponding future.AsyncAwait as done.
//
// The Executor flow can be described as:
//  1. An instance of kv.TimestampedBatch gets submitted to the Executor.
//  2. The client of the submit() method gets a multilevel future.Future.
//  3. The first future.Future gets done when the submitted kv.TimestampedBatch is applied to the memory.SortedSegment.
//  4. The client can wait (future.Future.WaitForResponse()) on the future.Future and get another instance of future.Future.
//  5. The second instance of future.Future gets done after the memory.SortedSegment is flushed to object store.
//  6. The client can choose to wait (future.Future.Wait()) on the second instance of future.Future, which is mainly a notification
//     that the memory.SortedSegment containing the given kv.TimestampedBatch has been flushed to object store.
func (executor *Executor) start() {
	for {
		select {
		case executionRequest := <-executor.incomingChannel:
			segmentFlushFuture, err := executor.state.Set(executionRequest.batch)
			if err != nil {
				executionRequest.asyncAwait.MarkDoneAsError(err)
			} else {
				executionRequest.asyncAwait.MarkDoneAsOkWith(segmentFlushFuture)
			}
		case <-executor.stopChannel:
			executor.emptyIncomingChannel()
			return
		}
	}
}

// submit submits the kv.TimestampedBatch to the Executor.
// It returns an instance of generically typed future.Future and the type argument is an instance of future.Future.
// The client is given a multilevel future as the response type.
//
// The first future.Future is marked done after the kv.TimestampedBatch is applied to the memory.SortedSegment.
// The application of kv.TimestampedBatch to the memory.SortedSegment returns another future.Future which gets done
// after the memory.SortedSegment is flushed to object store.
//
// Before attempting to send to the incomingChannel, the submit() method checks if the stopChannel is closed.
// If closed, it drains the incomingChannel to ensure no stale messages are left.
func (executor *Executor) submit(batch kv.TimestampedBatch) *future.Future[*future.Future[struct{}]] {
	executionRequest := NewExecutionRequest(batch)

	select {
	case <-executor.stopChannel:
		executionRequest.asyncAwait.MarkDoneAsError(state.ErrDbStopped)
		executor.emptyIncomingChannel()
		return executionRequest.asyncAwait.Future()
	default:
		executor.incomingChannel <- executionRequest
		return executionRequest.asyncAwait.Future()
	}
}

// stop stops the Executor.
func (executor *Executor) stop() {
	executor.stopOnce.Do(func() {
		close(executor.stopChannel)
	})
}

// emptyIncomingChannel empties the incoming channel and marks all the asyncAwait with error.
//
// This method uses a select with a default case to drain the channel without blocking indefinitely.
// This ensures the draining loop exits when the channel is empty.
func (executor *Executor) emptyIncomingChannel() {
	executor.emptyIncomingChannelLock.Lock()
	defer executor.emptyIncomingChannelLock.Unlock()
	for {
		select {
		case executionRequest := <-executor.incomingChannel:
			executionRequest.asyncAwait.MarkDoneAsError(state.ErrDbStopped)
		default:
			return
		}
	}
}

//////// ExecutionRequest ////////////

// ExecutionRequest wraps the kv.TimestampedBatch along with a future.AsyncAwait.
type ExecutionRequest struct {
	batch      kv.TimestampedBatch
	asyncAwait *future.AsyncAwait[*future.Future[struct{}]]
}

// NewExecutionRequest creates a new instance of ExecutionRequest.
func NewExecutionRequest(batch kv.TimestampedBatch) ExecutionRequest {
	return ExecutionRequest{
		batch:      batch,
		asyncAwait: future.NewAsyncAwait[*future.Future[struct{}]](),
	}
}
