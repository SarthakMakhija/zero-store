package coordination

import (
	"context"
	"sync"
)

// TimeKeeper is the central authority that assigns read and write timestamp to work-units.
// Every coordination.WorkUnit gets a read-timestamp and only a Readwrite work-unit gets a write timestamp.
// The current implementation uses next-timestamp which denotes the timestamp that will be assigned as the write timestamp
// to the next work-unit.
// The read-timestamp is one less than the next-timestamp.
// readTimestampMark is used to indicate till what timestamp have the work-units begun.
// writeTimestampMark is used to block the new work-units, so all previous writes are visible to a new read.
type TimeKeeper struct {
	lock               sync.Mutex
	nextTimestamp      uint64
	readTimestampMark  *WorkUnitTimestampWaterMark
	writeTimestampMark *WorkUnitTimestampWaterMark
	executor           *Executor
}

// NewTimeKeeper creates a new instance of TimeKeeper. It is called once in the entire application.
// TimeKeeper is initialized with nextTimestamp as 1.
// As a part creating a new instance of TimeKeeper, we also mark readTimestampMark and writeTimestampMark
// as finished for timestamp 0.
func NewTimeKeeper(executor *Executor) *TimeKeeper {
	return NewTimeKeeperWithLatestWriteTimestamp(executor, 0)
}

// NewTimeKeeperWithLatestWriteTimestamp creates a new instance of TimeKeeper. It is called once in the entire application.
// TimeKeeper is initialized with nextTimestamp as the lastWriteTimestamp + 1.
// As a part creating a new instance of NewTimeKeeper, we also mark readTimestampMark and writeTimestampMark
// as finished for timestamp lastWriteTimestamp.
func NewTimeKeeperWithLatestWriteTimestamp(executor *Executor, lastWriteTimestamp uint64) *TimeKeeper {
	oracle := &TimeKeeper{
		nextTimestamp:      lastWriteTimestamp + 1,
		readTimestampMark:  NewWorkUnitTimestampWaterMark(),
		writeTimestampMark: NewWorkUnitTimestampWaterMark(),
		executor:           executor,
	}

	oracle.readTimestampMark.Finish(oracle.nextTimestamp - 1)
	oracle.writeTimestampMark.Finish(oracle.nextTimestamp - 1)
	return oracle
}

// Close stops `readTimestampMark`, `writeTimestampMark` and `executor`.
func (timeKeeper *TimeKeeper) Close() {
	timeKeeper.readTimestampMark.Stop()
	timeKeeper.writeTimestampMark.Stop()
	timeKeeper.executor.stop()
}

// FinishReadTimestamp indicates that the readTimestamp of the work-unit is finished.
// This is an indication to the WorkUnitTimestampWaterMark that all the work-units upto a given `readTimestamp`
// are done.
func (timeKeeper *TimeKeeper) FinishReadTimestamp(readTimestamp uint64) {
	timeKeeper.readTimestampMark.Finish(readTimestamp)
}

// MaxBeginTimestamp returns the maximum readTimestamp.
// It is mainly used in compaction to disregard any keys with write-timestamp <= MaxBeginTimestamp().
func (timeKeeper *TimeKeeper) MaxBeginTimestamp() uint64 {
	return timeKeeper.readTimestampMark.DoneTill()
}

// readTimestamp returns the read-timestamp of a coordination.WorkUnit.
// readTimestamp = nextTimestamp - 1
// Before returning the readTimestamp, the system performs a wait on the writeTimestampMark.
// This wait is to ensure that all the writes till readTimestamp are applied in the storage.
func (timeKeeper *TimeKeeper) readTimestamp() uint64 {
	timeKeeper.lock.Lock()
	readTimestamp := timeKeeper.nextTimestamp - 1
	timeKeeper.readTimestampMark.Begin(readTimestamp)
	timeKeeper.lock.Unlock()

	_ = timeKeeper.writeTimestampMark.WaitForMark(context.Background(), readTimestamp)
	return readTimestamp
}
