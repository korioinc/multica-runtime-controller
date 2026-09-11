package execution

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"k8s.io/apimachinery/pkg/util/wait"
)

// This socket lives in the controller's private volume, never on the workspace
// PVC or a worker mount. Provider stdin is unrelated: Pi normally closes it.
const AttemptMonitorPath = wire.ControlRoot + "/attempts.sock"

// ListenAttemptMonitor preserves live listeners and non-socket paths. A
// controller container restart can leave a stale socket in its private volume.
func ListenAttemptMonitor(path string) (net.Listener, error) {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("attempt monitor path is not a socket")
		}
		connection, err := net.DialTimeout("unix", path, time.Second)
		if err == nil {
			connection.Close()
			return nil, errors.New("attempt monitor is already running")
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			return nil, err
		}
		current, err := os.Lstat(path)
		if err != nil || !os.SameFile(info, current) {
			return nil, errors.New("attempt monitor path changed")
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

// ServeAttemptMonitor observes shim lifetimes independently of the official
// daemon's process groups. EOF only requests recovery; the lease and journal
// remain the authority even if a live shim loses its connection.
func (r *Runner) ServeAttemptMonitor(ctx context.Context, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	stopListener := context.AfterFunc(ctx, func() { _ = listener.Close() })
	var handlers sync.WaitGroup
	defer func() {
		cancel()
		stopListener()
		_ = listener.Close()
		handlers.Wait()
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			defer connection.Close()
			stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
			defer stop()
			r.monitorAttempt(ctx, connection)
		}()
	}
}

func (r *Runner) monitorAttempt(ctx context.Context, connection net.Conn) {
	if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return
	}
	var id [36]byte
	if _, err := io.ReadFull(connection, id[:]); err != nil {
		return
	}
	a, err := r.journal.read(string(id[:]))
	if err != nil || a.Ref.Owner != r.selection.Controller {
		return
	}
	// Arm recovery before ACK: loss of the acknowledgement must not strand an
	// attempt if the shim is killed while registration is completing.
	defer func() {
		if ctx.Err() != nil {
			return
		}
		cleanup, cancel := context.WithTimeout(ctx, 40*time.Second)
		defer cancel()
		err := wait.PollUntilContextCancel(cleanup, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
			entry, err := r.journal.read(a.Ref.AttemptID)
			if errors.Is(err, os.ErrNotExist) {
				return true, nil // Normal completion already removed the journal.
			}
			if err == nil {
				err = r.reconcileAttempt(ctx, *entry)
			}
			// Kernel FD teardown need not report socket EOF after flock release.
			// A killed create can also become visible just after the first lookup.
			if errors.Is(err, workspace.ErrStorageBusy) || errors.Is(err, errCreateOutcomePending) {
				return false, nil
			}
			return true, err
		})
		if err != nil && ctx.Err() == nil {
			slog.Warn("disconnected task awaits recovery", "phase", "cleanup", "error_class", "cleanup_pending", "task", a.Ref.TaskID, "attempt", a.Ref.AttemptID)
		}
	}()
	if _, err := connection.Write([]byte{1}); err != nil {
		return
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		return
	}
	var end [1]byte
	_, _ = connection.Read(end[:])
}

// registerAttempt completes before resource creation. Its connection then
// stays open through normal cleanup and lease release, or closes on SIGKILL.
func registerAttempt(ctx context.Context, connection net.Conn, id string) error {
	if !wire.UUID(id) {
		return errors.New("attempt monitor requires an attempt UUID")
	}
	deadline := time.Now().Add(10 * time.Second)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := io.WriteString(connection, id); err != nil {
		return err
	}
	var ack [1]byte
	if _, err := io.ReadFull(connection, ack[:]); err != nil {
		return err
	}
	if ack[0] != 1 {
		return errors.New("attempt monitor registration refused")
	}
	return connection.SetDeadline(time.Time{})
}
