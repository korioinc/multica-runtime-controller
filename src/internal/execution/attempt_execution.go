package execution

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func (r *Runner) recordAttempt(request wire.Request) (*attempt, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := wire.Decode(raw); err != nil {
		return nil, err
	}
	storageID := filepath.Base(request.WorkerSubPath)
	a := &attempt{SchemaVersion: attemptSchemaVersion, OwnerID: r.selection.OwnerID, Created: time.Now().UTC(), Ref: kubernetes.Reference{Namespace: r.selection.Namespace, Owner: r.selection.Controller, TaskID: request.TaskID, StorageID: storageID, AttemptID: request.AttemptID, PodName: "task-worker-" + storageID, SecretName: "task-request-" + request.AttemptID, RequestDigest: wire.Digest(raw), RuntimeRef: request.RuntimeRef, FixedNode: r.selection.Worker.SingleNodeName}}
	a.Ref.PodDigest, err = kubernetes.PodFingerprint(r.selection.Worker, a.Ref, request, r.selection.Gateway)
	if err != nil {
		return nil, err
	}
	return a, r.journal.save(a)
}

func (r *Runner) executeAttempt(ctx context.Context, request wire.Request, a *attempt, streams kubernetes.Streams) (result Result) {
	result.Code = 1
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		result.CleanupError = r.cleanup(cleanup, a)
		if result.CleanupError != nil {
			slog.Warn("task resources await recovery", "phase", "cleanup", "error_class", "cleanup_pending", "imageBuildID", r.selection.RuntimeRef.ImageBuildID, "task", request.TaskID, "attempt", request.AttemptID)
		}
	}()
	finishSecret := diagnostics.StartPhase("worker_secret_create", diagnostics.TaskAttributes(request.TaskID, request.AttemptID)...)
	err := r.createAttemptSecret(ctx, a, request)
	finishSecret(err)
	if err != nil {
		result.ExecutionError = err
		return
	}
	finishPod := diagnostics.StartPhase("worker_pod_create", diagnostics.TaskAttributes(request.TaskID, request.AttemptID)...)
	err = r.createAttemptPod(ctx, a, request)
	finishPod(err)
	if err != nil {
		result.ExecutionError = err
		return
	}
	execution, cancel := context.WithTimeout(ctx, time.Duration(r.selection.Worker.TaskDeadlineSeconds)*time.Second)
	defer cancel()
	result.ExecutionError = r.resources.Execute(execution, a.Ref, time.Duration(r.selection.Worker.TaskDeadlineSeconds)*time.Second, streams)
	result.Code, result.Exited = kubernetes.ExitCode(result.ExecutionError)
	return
}

func (r *Runner) createAttemptSecret(ctx context.Context, a *attempt, request wire.Request) error {
	a.SecretStarted = true
	if err := r.journal.save(a); err != nil {
		return err
	}
	uid, created := r.resources.CreateSecret(ctx, a.Ref, request)
	if created != nil || uid == "" {
		var err error
		uid, err = r.resources.ResolveSecret(ctx, a.Ref)
		if err != nil || uid == "" {
			return errors.Join(created, err, errors.New("Secret create unresolved"))
		}
	}
	a.Ref.SecretUID = uid
	return r.journal.save(a)
}

func (r *Runner) createAttemptPod(ctx context.Context, a *attempt, request wire.Request) error {
	a.PodStarted = true
	if err := r.journal.save(a); err != nil {
		return err
	}
	uid, created := r.resources.CreatePod(ctx, r.selection.Worker, a.Ref, request, r.selection.Gateway)
	if created != nil || uid == "" {
		var err error
		uid, err = r.resources.ResolvePod(ctx, a.Ref)
		if err != nil || uid == "" {
			return errors.Join(created, err, errors.New("Pod create unresolved"))
		}
	}
	a.Ref.PodUID = uid
	return r.journal.save(a)
}
