package execution

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/environment"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type Runner struct {
	selection Selection
	resources *kubernetes.Client
	store     *workspace.Store
	journal   *journal
	manifest  environment.Manifest
}
type Result struct {
	Code                         int
	Exited                       bool
	ExecutionError, CleanupError error
}

type AttemptError struct {
	TaskID, AttemptID, EnvironmentID string
	Cause                            error
}

func (e *AttemptError) Error() string { return "task execution failed" }
func (e *AttemptError) Unwrap() error { return e.Cause }

func NewRunner(s Selection, resources *kubernetes.Client, store *workspace.Store, manifest environment.Manifest) (*Runner, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if resources == nil || store == nil {
		return nil, errors.New("execution resources required")
	}
	j, err := openJournal(wire.WorkspaceRoot+"/.multica-runtime/attempts", s.OwnerID)
	if err != nil {
		return nil, err
	}
	return &Runner{selection: s, resources: resources, store: store, journal: j, manifest: manifest}, nil
}

func (r *Runner) Run(ctx context.Context, request wire.Request, streams kubernetes.Streams) (result Result) {
	result.Code = 1
	generatedAttempt := ""
	defer func() {
		if result.ExecutionError != nil {
			result.ExecutionError = &AttemptError{TaskID: request.TaskID, AttemptID: generatedAttempt, EnvironmentID: r.selection.Environment.EnvironmentID, Cause: result.ExecutionError}
		}
	}()
	fail := func(err error) Result { return Result{Code: 1, ExecutionError: err} }
	root, err := wire.StorageRoot(request)
	if err != nil {
		return fail(err)
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root {
		return fail(errors.New("task_authorization: noncanonical preparation root"))
	}
	claim, err := r.store.Lookup(request.TaskID, wire.Value(request.Env, "MULTICA_TOKEN"), wire.Value(request.Env, "MULTICA_WORKSPACE_ID"), wire.Value(request.Env, "MULTICA_AGENT_ID"))
	if err != nil {
		return fail(err)
	}
	if !claim.Environment.Equal(r.selection.Environment) {
		return fail(errors.New("task_authorization: claim environment changed"))
	}
	request.Env = slices.DeleteFunc(request.Env, func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		_, inherited := r.manifest.Env[key]
		return inherited && !slices.Contains(r.selection.OperatorKeys, key) && !slices.Contains(claim.TaskEnvKeys, key)
	})
	session, err := wire.PiSession(request)
	if err != nil {
		return fail(err)
	}
	binding, err := r.store.Bind(claim, root, session, r.selection.Environment)
	if err != nil {
		return fail(err)
	}
	request.WorkerSubPath = binding.WorkerSubPath
	storageID := filepath.Base(binding.WorkerSubPath)
	release, err := r.store.AcquireLease(storageID)
	if err != nil {
		return fail(err)
	}
	defer release()
	if err := r.recoverStorage(ctx, storageID); err != nil {
		return fail(err)
	}
	request.RepositoryURLs = claim.RepositoryURLs
	request.SchemaVersion = 1
	request.Environment = r.selection.Environment
	request.EnvironmentInput = r.selection.Input
	request.OwnerID = r.selection.OwnerID
	request.AttemptID = uuid.NewString()
	generatedAttempt = request.AttemptID
	request.TerminationGraceSeconds = int(r.selection.Worker.TerminationGraceSeconds)
	for i, entry := range request.Env {
		if strings.HasPrefix(entry, "MULTICA_SERVER_URL=") {
			request.Env[i] = "MULTICA_SERVER_URL=" + r.selection.Backend
		}
	}
	workerRoot := filepath.Join(wire.WorkspaceRoot, binding.WorkerSubPath)
	if err := checkout.SeedContext(root, workerRoot, wire.WorkspaceRoot+"/.multica-runtime/context/"+storageID+".json", request.Provider); err != nil {
		return fail(err)
	}
	port, token, closeBroker, err := startBroker(request)
	if err != nil {
		return fail(err)
	}
	defer closeBroker()
	request.BrokerPort, request.BrokerToken = port, token
	raw, err := json.Marshal(request)
	if err != nil {
		return fail(err)
	}
	if _, err := wire.Decode(raw); err != nil {
		return fail(err)
	}
	a := &attempt{SchemaVersion: 1, OwnerID: r.selection.OwnerID, Created: time.Now().UTC(), Ref: kubernetes.Reference{Namespace: r.selection.Namespace, Owner: r.selection.Controller, TaskID: request.TaskID, StorageID: storageID, AttemptID: request.AttemptID, PodName: "task-worker-" + storageID, SecretName: "task-request-" + request.AttemptID, RequestDigest: wire.Digest(raw), CoreImage: request.Environment.CoreImage, EnvironmentImage: request.Environment.EnvironmentImage, EnvironmentID: request.Environment.EnvironmentID}}
	a.Ref.FixedNode = r.selection.Worker.SingleNodeName
	a.Ref.PodDigest, err = kubernetes.PodFingerprint(r.selection.Worker, a.Ref, request, r.selection.Gateway)
	if err != nil {
		return fail(err)
	}
	if err := r.journal.save(a); err != nil {
		return fail(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		result.CleanupError = r.cleanup(cleanup, a)
		if result.CleanupError != nil {
			slog.Warn("task resources await recovery", "phase", "cleanup", "error_class", "cleanup_pending", "environmentID", r.selection.Environment.EnvironmentID, "task", request.TaskID, "attempt", request.AttemptID)
		}
	}()
	a.SecretStarted = true
	if err := r.journal.save(a); err != nil {
		return fail(err)
	}
	uid, created := r.resources.CreateSecret(ctx, a.Ref, request)
	if created != nil || uid == "" {
		uid, err = r.resources.ResolveSecret(ctx, a.Ref)
		if err != nil || uid == "" {
			return fail(errors.Join(created, err, errors.New("Secret create unresolved")))
		}
	}
	a.Ref.SecretUID = uid
	if err := r.journal.save(a); err != nil {
		return fail(err)
	}
	a.PodStarted = true
	if err := r.journal.save(a); err != nil {
		return fail(err)
	}
	uid, created = r.resources.CreatePod(ctx, r.selection.Worker, a.Ref, request, r.selection.Gateway)
	if created != nil || uid == "" {
		uid, err = r.resources.ResolvePod(ctx, a.Ref)
		if err != nil || uid == "" {
			return fail(errors.Join(created, err, errors.New("Pod create unresolved")))
		}
	}
	a.Ref.PodUID = uid
	if err := r.journal.save(a); err != nil {
		return fail(err)
	}
	execution, cancel := context.WithTimeout(ctx, time.Duration(r.selection.Worker.TaskDeadlineSeconds)*time.Second)
	defer cancel()
	err = r.resources.Execute(execution, a.Ref, time.Duration(r.selection.Worker.TaskDeadlineSeconds)*time.Second, streams)
	result.Code, result.Exited = kubernetes.ExitCode(err)
	result.ExecutionError = err
	return result
}

func Launch(ctx context.Context, provider string, args, env []string, directory string, streams ProcessStreams) error {
	s, err := LoadSelection()
	if err != nil {
		return err
	}
	_, manifest, err := environment.Check(wire.EnvironmentRoot, wire.CoreRoot, s.Input, &s.Environment)
	if err != nil {
		return err
	}
	path, err := environment.ProviderPath(wire.EnvironmentRoot, manifest, provider)
	if err != nil {
		return err
	}
	if wire.Value(env, "MULTICA_TASK_ID") == "" {
		// The daemon already received the selected image/manifest/operator layer.
		// Applying manifest defaults again here would erase operator overrides.
		return ResultError(RunProcess(ctx, path, args, env, directory, time.Duration(s.Worker.TerminationGraceSeconds)*time.Second, streams))
	}
	request := wire.Request{TaskID: wire.Value(env, "MULTICA_TASK_ID"), Provider: provider, Args: args, Env: wire.TaskEnvironment(env), WorkDir: directory}
	resources, err := kubernetes.InCluster(s.Namespace)
	if err != nil {
		return err
	}
	store, err := OpenWorkspace(s.OwnerID)
	if err != nil {
		return err
	}
	runner, err := NewRunner(s, resources, store, manifest)
	if err != nil {
		return err
	}
	result := runner.Run(ctx, request, kubernetes.Streams{Stdin: streams.Stdin, Stdout: streams.Stdout, Stderr: streams.Stderr})
	if result.Exited && result.Code != 0 {
		return &ExitError{Code: result.Code}
	}
	return result.ExecutionError
}
