package execution

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func TestExecutionLeaseCancellationPreservesLiveStorage(t *testing.T) {
	for _, exit := range []string{"cancel", "cancel-and-release", "budget"} {
		t.Run(exit, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newMonitorFixture(t)
				storage := f.attempt.Ref.StorageID
				holder, err := f.runner.store.AcquireLease(storage)
				if err != nil {
					t.Fatal(err)
				}
				defer holder()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					release, err := f.runner.acquireExecutionLease(ctx, preparedTask{request: f.request, storageID: storage})
					if release != nil {
						release()
					}
					done <- err
				}()
				synctest.Wait()
				if exit != "budget" {
					cancel()
				}
				if exit == "cancel-and-release" {
					holder()
				}
				err = <-done
				if exit == "budget" && !errors.Is(err, context.DeadlineExceeded) || exit != "budget" && !errors.Is(err, context.Canceled) {
					t.Fatal("an expired/cancelled request acquired execution authority", err)
				}
				probe, err := f.runner.store.AcquireLease(storage)
				if exit != "cancel-and-release" {
					if probe != nil {
						probe()
					}
					if !errors.Is(err, workspace.ErrStorageBusy) {
						t.Fatal("waiting request released another execution's live authority", err)
					}
				} else {
					if err != nil {
						t.Fatal("cancelled waiter leaked acquired storage authority", err)
					}
					probe()
				}
			})
		})
	}
}

func TestExecutionLeaseDoesNotSerializeIndependentStorage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newMonitorFixture(t)
		holder, err := f.runner.store.AcquireLease(f.attempt.Ref.StorageID)
		if err != nil {
			t.Fatal(err)
		}
		defer holder()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			release, err := f.runner.acquireExecutionLease(ctx, preparedTask{storageID: f.attempt.Ref.StorageID})
			if release != nil {
				release()
			}
			done <- err
		}()
		synctest.Wait()
		independent := uuid.NewString()
		release, err := f.runner.acquireExecutionLease(t.Context(), preparedTask{storageID: independent})
		if err != nil {
			t.Fatal("busy storage blocked independent work", err)
		}
		defer release()
		cancel()
		<-done
	})
}

func TestExecutionLeaseRefusesInvalidLockWithoutReplacingIt(t *testing.T) {
	f := newMonitorFixture(t)
	path := filepath.Join(f.options.Directory, "worker-"+f.attempt.Ref.StorageID+".lock")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	release, err := f.runner.acquireExecutionLease(t.Context(), preparedTask{storageID: f.attempt.Ref.StorageID})
	if release != nil {
		release()
	}
	if err == nil {
		t.Fatal("invalid lock granted exclusive storage authority")
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		t.Fatal("invalid lock was replaced to bypass storage authority", err)
	}
}
