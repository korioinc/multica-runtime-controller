package execution

import (
	"context"
	"errors"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

const (
	storageLeaseWait = 15 * time.Second
	storageLeasePoll = 250 * time.Millisecond
)

// Only execution admission waits. Recovery/retirement keep the store's
// nonblocking flock contract, and unrelated storage remains independent.
func (r *Runner) acquireExecutionLease(ctx context.Context, task preparedTask) (release func(), err error) {
	attributes := append(diagnostics.TaskAttributes(task.request.TaskID, ""), "storage", task.storageID)
	finish := diagnostics.StartPhase("storage_lease_wait", attributes...)
	outcome := "acquired"
	defer func() { finish(err, "outcome", outcome) }()
	waiting, cancel := context.WithTimeout(ctx, storageLeaseWait)
	defer cancel()
	failed := func(cause error) (func(), error) {
		outcome = "failed"
		reason := "storage_lease_failed"
		if ctx.Err() != nil {
			cause, outcome, reason = ctx.Err(), "cancelled", "storage_lease_cancelled"
			if errors.Is(cause, context.DeadlineExceeded) {
				outcome, reason = "deadline", "storage_lease_deadline"
			}
		} else if errors.Is(cause, context.DeadlineExceeded) {
			outcome, reason = "timeout", "storage_lease_timeout"
		}
		return nil, &diagnostics.Error{Reason: reason, StorageID: task.storageID, Cause: cause}
	}
	for {
		if err := waiting.Err(); err != nil {
			return failed(err)
		}
		release, err = r.store.AcquireLease(task.storageID)
		if err == nil {
			if err := waiting.Err(); err != nil {
				release()
				return failed(err)
			}
			return release, nil
		}
		if !errors.Is(err, workspace.ErrStorageBusy) {
			return failed(err)
		}
		timer := time.NewTimer(storageLeasePoll)
		select {
		case <-waiting.Done():
			timer.Stop()
			return failed(waiting.Err())
		case <-timer.C:
		}
	}
}
