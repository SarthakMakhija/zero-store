package future

// Future represents the result of asynchronous computation.
// Eg; flushing an inactive segment to object store.
type Future struct {
	responseChannel chan struct{}
	isDone          bool
	status          Status
}

// NewFuture creates a new instance of Future.
func NewFuture() *Future {
	return &Future{
		responseChannel: make(chan struct{}),
		isDone:          false,
	}
}

// MarkDoneAsOk marks the Future as done with Status Ok.
func (future *Future) MarkDoneAsOk() {
	if !future.isDone {
		close(future.responseChannel)
		future.isDone = true
	}
	future.status = OkStatus()
}

// MarkDoneAsError marks the Future as done with Status Error.
func (future *Future) MarkDoneAsError(err error) {
	if !future.isDone {
		close(future.responseChannel)
		future.isDone = true
	}
	future.status = ErrorStatus(err)
}

// Wait waits until the Future is marked as done.
func (future *Future) Wait() {
	<-future.responseChannel
}

// Status returns the status.
func (future *Future) Status() Status {
	return future.status
}
