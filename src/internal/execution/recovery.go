package execution

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

var errCreateOutcomePending = errors.New("create outcome may still arrive; owner restart required before storage reuse")

func (r *Runner) cleanup(ctx context.Context, a *attempt) error {
	// Persist any UIDs learned after failed create bookkeeping before deletion.
	recordErr := r.journal.save(a)
	unresolved := false
	if a.SecretStarted && a.Ref.SecretUID == "" {
		uid, err := r.resources.ResolveCleanupSecret(ctx, a.Ref)
		if err != nil {
			return errors.Join(recordErr, err)
		}
		if uid == "" {
			unresolved = true
		} else {
			a.Ref.SecretUID = uid
			if err := r.journal.save(a); err != nil {
				return errors.Join(recordErr, err)
			}
		}
	}
	if a.PodStarted && a.Ref.PodUID == "" {
		uid, err := r.resources.ResolveCleanupPod(ctx, a.Ref)
		if err != nil {
			return errors.Join(recordErr, err)
		}
		if uid == "" {
			unresolved = true
		} else {
			a.Ref.PodUID = uid
			if err := r.journal.save(a); err != nil {
				return errors.Join(recordErr, err)
			}
		}
	}
	if err := r.resources.Cleanup(ctx, a.Ref); err != nil {
		return errors.Join(recordErr, err)
	}
	if unresolved {
		alive, err := r.resources.OwnerExists(ctx, a.Ref)
		if err != nil {
			return errors.Join(recordErr, err)
		}
		if alive {
			return errors.Join(recordErr, errCreateOutcomePending)
		}
	}
	if recordErr != nil {
		return recordErr
	}
	if err := removeTaskHomeArchive(wire.WorkspaceRoot, a.Ref.StorageID, a.Ref.AttemptID); err != nil {
		return err
	}
	return r.journal.remove(a)
}
func (r *Runner) recoverStorage(ctx context.Context, storage string) error {
	all, err := r.journal.readAll()
	if err != nil {
		return err
	}
	for _, a := range all {
		if a.Ref.StorageID == storage {
			if err := r.cleanup(ctx, &a); err != nil {
				return err
			}
		}
	}
	return r.resources.StorageAvailable(ctx, r.selection.Worker.WorkspaceClaim, storage, r.selection.Controller)
}
func (r *Runner) Reconcile(ctx context.Context) (map[string]bool, error) {
	all, err := r.journal.readAll()
	if err != nil {
		return nil, err
	}
	active := map[string]bool{}
	var failures []error
	for _, entry := range all {
		err := r.reconcileAttempt(ctx, entry)
		if err != nil {
			active[entry.Ref.StorageID] = true
			if !errors.Is(err, workspace.ErrStorageBusy) {
				failures = append(failures, err)
			}
		}
	}
	return active, errors.Join(failures...)
}

// Both periodic recovery and a disconnected shim must acquire the same lease
// and reread durable authority before touching any resources.
func (r *Runner) reconcileAttempt(ctx context.Context, entry attempt) error {
	release, err := r.store.AcquireLease(entry.Ref.StorageID)
	if err != nil {
		return &diagnostics.Error{Reason: "recovery_lease_failed", AttemptID: entry.Ref.AttemptID, StorageID: entry.Ref.StorageID, Cause: err}
	}
	defer release()
	a, err := r.journal.read(entry.Ref.AttemptID)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if a.Ref.StorageID != entry.Ref.StorageID {
		return errors.New("attempt storage changed during recovery")
	}
	return r.cleanup(ctx, a)
}

func (r *Runner) Collect(ctx context.Context) error {
	active, err := r.Reconcile(ctx)
	if err != nil {
		return err
	}
	live, err := r.resources.ActiveStorage(ctx, r.selection.Worker.WorkspaceClaim, r.selection.Controller)
	if err != nil {
		return err
	}
	for id := range live {
		active[id] = true
	}
	if err := r.collectHomeArtifacts(active); err != nil {
		return err
	}
	cutoff := time.Now().Add(-max(time.Duration(r.selection.Worker.TaskDeadlineSeconds)*time.Second, 6*time.Hour) - 24*time.Hour)
	_, err = r.store.Collect(wire.WorkspaceRoot, cutoff, active)
	return err
}
func (r *Runner) RunCollector(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		if err := r.Collect(ctx); err != nil {
			attributes := []any{"phase", "recovery", "error_class", "cleanup_pending"}
			attributes = append(attributes, diagnostics.Attributes(err)...)
			slog.Warn("workspace recovery pending", attributes...)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
