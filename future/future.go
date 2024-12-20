package future

// AsyncAwait wraps a Future.
// The client is exposed a Future, while AsyncAwait remains an internal object.
type AsyncAwait struct {
	future *Future
}

// Future represents the result of asynchronous computation.
// Eg; flushing an inactive segment to object store.
type Future struct {
	responseChannel chan struct{}
	isDone          bool
	status          Status
}

// NewAsyncAwait creates a new instance of AsyncAwait.
func NewAsyncAwait() *AsyncAwait {
	return &AsyncAwait{
		future: &Future{
			responseChannel: make(chan struct{}),
			isDone:          false,
			status:          PendingStatus(),
		},
	}
}

// MarkDoneAsOk marks the Future as done with Status Ok.
func (asyncAwait *AsyncAwait) MarkDoneAsOk() {
	if !asyncAwait.future.isDone {
		close(asyncAwait.future.responseChannel)
		asyncAwait.future.isDone = true
	}
	asyncAwait.future.status = OkStatus()
}

// MarkDoneAsError marks the Future as done with Status Error.
func (asyncAwait *AsyncAwait) MarkDoneAsError(err error) {
	if !asyncAwait.future.isDone {
		close(asyncAwait.future.responseChannel)
		asyncAwait.future.isDone = true
	}
	asyncAwait.future.status = ErrorStatus(err)
}

// Future returns the Future object.
func (asyncAwait *AsyncAwait) Future() *Future {
	return asyncAwait.future
}

// Wait waits until the Future is marked as done.
func (future *Future) Wait() {
	<-future.responseChannel
}

// Status returns the status.
func (future *Future) Status() Status {
	return future.status
}
