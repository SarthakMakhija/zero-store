package future

type StatusType int

const (
	Ok    StatusType = 1
	Error StatusType = 2
)

// Status represents the status of an async operation that returns a Future.
type Status struct {
	statusType StatusType
	err        error
}

// OkStatus creates a new Status with StatusType as Ok.
func OkStatus() Status {
	return Status{statusType: Ok, err: nil}
}

// ErrorStatus creates a new Status with StatusType as Error.
func ErrorStatus(err error) Status {
	return Status{statusType: Error, err: err}
}

// IsOk returns true if the StatusType is Ok.
func (status Status) IsOk() bool {
	return status.statusType == Ok
}

// IsError returns true if the StatusType is Error.
func (status Status) IsError() bool {
	return status.statusType == Error
}

// Error returns the error of the Status, will be nil if the Status is Ok.
func (status Status) Error() error {
	return status.err
}
