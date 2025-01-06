package future

// AsyncAwait wraps a Future.
// The generic type FutureResponse is the response of the encapsulating Future.
// The client is exposed a Future, while AsyncAwait remains an internal object.
// This layer of abstraction allows exposing `Wait() and Status()` like methods to the client.
type AsyncAwait[FutureResponse any] struct {
	future *Future[FutureResponse]
}

// Future represents the result of asynchronous computation.
// Eg; flushing an inactive segment to object store.
type Future[Response any] struct {
	responseChannel chan Response
	isDone          bool
	status          Status
}

// NewAsyncAwait creates a new instance of AsyncAwait.
func NewAsyncAwait[FutureResponse any]() *AsyncAwait[FutureResponse] {
	return &AsyncAwait[FutureResponse]{
		future: &Future[FutureResponse]{
			responseChannel: make(chan FutureResponse, 1),
			isDone:          false,
			status:          PendingStatus(),
		},
	}
}

// MarkDoneAsOk marks the Future as done with Status Ok.
func (asyncAwait *AsyncAwait[FutureResponse]) MarkDoneAsOk() {
	if !asyncAwait.future.isDone {
		close(asyncAwait.future.responseChannel)
		asyncAwait.future.isDone = true
	}
	asyncAwait.future.status = OkStatus()
}

// MarkDoneAsOkWith marks the Future as done with Status Ok and returns the response of type FutureResponse on the responseChannel of the
// encapsulating Future.
func (asyncAwait *AsyncAwait[FutureResponse]) MarkDoneAsOkWith(response FutureResponse) {
	if !asyncAwait.future.isDone {
		asyncAwait.future.responseChannel <- response
		asyncAwait.future.isDone = true
		close(asyncAwait.future.responseChannel)
	}
	asyncAwait.future.status = OkStatus()
}

// MarkDoneAsError marks the Future as done with Status Error.
func (asyncAwait *AsyncAwait[FutureResponse]) MarkDoneAsError(err error) {
	if !asyncAwait.future.isDone {
		close(asyncAwait.future.responseChannel)
		asyncAwait.future.isDone = true
	}
	asyncAwait.future.status = ErrorStatus(err)
}

// Future returns the Future object.
func (asyncAwait *AsyncAwait[FutureResponse]) Future() *Future[FutureResponse] {
	return asyncAwait.future
}

// Wait waits until the Future is marked as done.
func (future *Future[Response]) Wait() {
	_ = future.WaitForResponse()
}

// WaitForResponse waits until the Future is marked as done and returns the response of type FutureResponse.
func (future *Future[Response]) WaitForResponse() Response {
	return <-future.responseChannel
}

// Status returns the status.
func (future *Future[Response]) Status() Status {
	return future.status
}
