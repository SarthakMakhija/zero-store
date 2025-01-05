package future

// AsyncAwait wraps a Future.
// The client is exposed a Future, while AsyncAwait remains an internal object.
// This layer of abstraction allows exposing `Wait() and Status()` like methods to the client.
type AsyncAwait[T any] struct {
	future *Future[T]
}

// Future represents the result of asynchronous computation.
// Eg; flushing an inactive segment to object store.
type Future[T any] struct {
	responseChannel chan T
	isDone          bool
	status          Status
}

// NewAsyncAwait creates a new instance of AsyncAwait.
func NewAsyncAwait[T any]() *AsyncAwait[T] {
	return &AsyncAwait[T]{
		future: &Future[T]{
			responseChannel: make(chan T, 1),
			isDone:          false,
			status:          PendingStatus(),
		},
	}
}

// MarkDoneAsOk marks the Future as done with Status Ok.
func (asyncAwait *AsyncAwait[T]) MarkDoneAsOk() {
	if !asyncAwait.future.isDone {
		close(asyncAwait.future.responseChannel)
		asyncAwait.future.isDone = true
	}
	asyncAwait.future.status = OkStatus()
}

// MarkDoneAsOkWith marks the Future as done with Status Ok and returns the response of type T on the responseChannel of the
// encapsulating Future.
func (asyncAwait *AsyncAwait[T]) MarkDoneAsOkWith(response T) {
	if !asyncAwait.future.isDone {
		asyncAwait.future.responseChannel <- response
		asyncAwait.future.isDone = true
		close(asyncAwait.future.responseChannel)
	}
	asyncAwait.future.status = OkStatus()
}

// MarkDoneAsError marks the Future as done with Status Error.
func (asyncAwait *AsyncAwait[T]) MarkDoneAsError(err error) {
	if !asyncAwait.future.isDone {
		close(asyncAwait.future.responseChannel)
		asyncAwait.future.isDone = true
	}
	asyncAwait.future.status = ErrorStatus(err)
}

// Future returns the Future object.
func (asyncAwait *AsyncAwait[T]) Future() *Future[T] {
	return asyncAwait.future
}

// Wait waits until the Future is marked as done.
func (future *Future[T]) Wait() {
	_ = future.WaitForResponse()
}

// WaitForResponse waits until the Future is marked as done and returns the response of type T.
func (future *Future[T]) WaitForResponse() T {
	return <-future.responseChannel
}

// Status returns the status.
func (future *Future[T]) Status() Status {
	return future.status
}
