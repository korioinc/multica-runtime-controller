package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func TestTaskTreeRetryRechecksTheStrictTree(t *testing.T) {
	for _, unsafe := range []bool{false, true} {
		name := "valid_tree"
		if unsafe {
			name = "untrusted_link"
		}
		t.Run(name, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "retained-file"), []byte("retained"), 0600); err != nil {
				t.Fatal(err)
			}
			if unsafe {
				if err := os.Symlink("retained-file", filepath.Join(root, "untrusted-link")); err != nil {
					t.Fatal(err)
				}
			}
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				started := time.Now()
				err := verifyTaskTreeWithRetry(context.Background(), func() error {
					calls++
					if calls <= 2 {
						return &os.PathError{Op: "readdirent", Path: root, Err: syscall.ESTALE}
					}
					return workspace.VerifyTaskTree(root, nil)
				})
				if (err != nil) != unsafe || errors.Is(err, syscall.ESTALE) {
					t.Fatalf("strict validation result: %v", err)
				}
				if calls != 3 || time.Since(started) != 2*taskTreeRetryInterval {
					t.Fatalf("calls=%d elapsed=%v", calls, time.Since(started))
				}
			})
		})
	}
}

func TestTaskTreeRetryRejectsOtherErrorsImmediately(t *testing.T) {
	for _, cause := range []error{os.ErrNotExist, syscall.EACCES, errors.New("task has a shared hardlink")} {
		t.Run(cause.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				started := time.Now()
				want := &os.PathError{Op: "lstat", Path: "private-task-path", Err: cause}
				err := verifyTaskTreeWithRetry(context.Background(), func() error {
					calls++
					return want
				})
				if err != want || calls != 1 || time.Since(started) != 0 {
					t.Fatalf("err=%v calls=%d elapsed=%v", err, calls, time.Since(started))
				}
			})
		})
	}
}

func TestTaskTreeRetryKeepsTheLastStaleErrorAtItsLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		started := time.Now()
		var last *os.PathError
		err := verifyTaskTreeWithRetry(context.Background(), func() error {
			calls++
			last = &os.PathError{Op: "readdirent", Path: "private-task-path", Err: syscall.ESTALE}
			return last
		})
		if err != last || !errors.Is(err, syscall.ESTALE) || time.Since(started) != taskTreeRetryTimeout {
			t.Fatalf("err=%v elapsed=%v", err, time.Since(started))
		}
		if calls != int(taskTreeRetryTimeout/taskTreeRetryInterval) {
			t.Fatalf("retry count=%d", calls)
		}
	})
}

func TestTaskTreeRetryCancellationStopsFurtherScans(t *testing.T) {
	for _, stage := range []string{"before_scan", "during_scan", "during_wait", "caller_deadline"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if stage == "before_scan" {
					cancel()
				}
				if stage == "during_wait" {
					go func() {
						time.Sleep(taskTreeRetryInterval / 2)
						cancel()
					}()
				}
				want := context.Canceled
				if stage == "caller_deadline" {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, taskTreeRetryInterval/2)
					defer stop()
					want = context.DeadlineExceeded
				}
				calls := 0
				started := time.Now()
				err := verifyTaskTreeWithRetry(ctx, func() error {
					calls++
					if stage == "during_scan" {
						cancel()
						return nil
					}
					return syscall.ESTALE
				})
				wantCalls := 1
				if stage == "before_scan" {
					wantCalls = 0
				}
				wantElapsed := time.Duration(0)
				if stage == "during_wait" || stage == "caller_deadline" {
					wantElapsed = taskTreeRetryInterval / 2
				}
				if !errors.Is(err, want) || calls != wantCalls || time.Since(started) != wantElapsed {
					t.Fatalf("err=%v calls=%d elapsed=%v", err, calls, time.Since(started))
				}
			})
		})
	}
}
