package execution

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type Runner struct {
	selection Selection
	resources *kubernetes.Client
	store     *workspace.Store
	journal   *journal
	manifest  runtimeimage.Descriptor
}
type Result struct {
	Code                         int
	Exited                       bool
	ExecutionError, CleanupError error
}

type AttemptError struct {
	TaskID, AttemptID, ImageBuildID string
	Cause                           error
}

func (e *AttemptError) Error() string { return "task execution failed" }
func (e *AttemptError) Unwrap() error { return e.Cause }

func NewRunner(s Selection, resources *kubernetes.Client, store *workspace.Store, manifest runtimeimage.Descriptor) (*Runner, error) {
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

// Run keeps the lease and broker alive until the durable attempt has finished
// cleanup. Preparation cannot race a previous consumer of the same storage.
func (r *Runner) Run(ctx context.Context, request wire.Request, streams kubernetes.Streams) (result Result) {
	result.Code = 1
	var monitor net.Conn
	// Registered first, so normal cleanup and lease release finish before EOF.
	defer func() {
		if monitor != nil {
			_ = monitor.Close()
		}
	}()
	generatedAttempt := ""
	defer func() {
		if result.ExecutionError != nil {
			result.ExecutionError = &AttemptError{TaskID: request.TaskID, AttemptID: generatedAttempt, ImageBuildID: r.selection.RuntimeRef.ImageBuildID, Cause: result.ExecutionError}
		}
	}()
	fail := func(err error) Result { return Result{Code: 1, ExecutionError: err} }
	task, err := r.authorizeTask(request)
	if err != nil {
		return fail(err)
	}
	release, err := r.store.AcquireLease(task.storageID)
	if err != nil {
		return fail(err)
	}
	defer release()
	finishRecovery := diagnostics.StartPhase("attempt_recovery", diagnostics.TaskAttributes(task.request.TaskID, task.request.AttemptID)...)
	err = r.recoverStorage(ctx, task.storageID)
	finishRecovery(err)
	if err != nil {
		return fail(err)
	}
	task.request = r.selectAttempt(task.request)
	generatedAttempt = task.request.AttemptID
	recorded := false
	defer func() {
		if !recorded {
			result.CleanupError = errors.Join(result.CleanupError, removeTaskHomeArchive(wire.WorkspaceRoot, task.storageID, generatedAttempt))
		}
	}()
	finishContext := diagnostics.StartPhase("task_context", diagnostics.TaskAttributes(task.request.TaskID, task.request.AttemptID)...)
	err = r.prepareTaskContext(&task)
	finishContext(err)
	if err != nil {
		return fail(err)
	}
	port, token, closeBroker, err := startBroker(task.request)
	if err != nil {
		return fail(err)
	}
	defer closeBroker()
	task.request.BrokerPort, task.request.BrokerToken = port, token
	a, err := r.recordAttempt(task.request)
	if err != nil {
		return fail(err)
	}
	recorded = true
	monitor, err = (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "unix", AttemptMonitorPath)
	if err == nil {
		err = registerAttempt(ctx, monitor, a.Ref.AttemptID)
	}
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		return Result{Code: 1, ExecutionError: err, CleanupError: r.cleanup(cleanup, a)}
	}
	return r.executeAttempt(ctx, task.request, a, streams)
}

func Launch(ctx context.Context, provider string, args, env []string, directory string, streams ProcessStreams) error {
	finishSelection := diagnostics.StartPhase("shim_selection", diagnostics.TaskAttributes(wire.Value(env, "MULTICA_TASK_ID"), "")...)
	s, err := LoadSelection()
	finishSelection(err)
	if err != nil {
		return err
	}
	finishMetadata := diagnostics.StartPhase("shim_metadata", diagnostics.TaskAttributes(wire.Value(env, "MULTICA_TASK_ID"), "")...)
	manifest, err := runtimeimage.ReadMetadata()
	finishMetadata(err)
	if err != nil {
		return err
	}
	if _, ok := manifest.Providers[provider]; !ok {
		return errors.New("provider is not enabled")
	}
	if wire.Value(env, "MULTICA_TASK_ID") == "" {
		path, err := runtimeimage.ProviderPath(manifest, provider)
		if err != nil {
			return err
		}
		// The daemon already received the selected image/manifest/operator layer.
		// Applying manifest defaults again here would erase operator overrides.
		return ResultError(RunProcess(ctx, path, args, env, directory, time.Duration(s.Worker.TerminationGraceSeconds)*time.Second, streams))
	}
	request := wire.Request{TaskID: wire.Value(env, "MULTICA_TASK_ID"), Provider: provider, Args: args, Env: wire.TaskEnvironment(env), WorkDir: directory}
	resources, err := kubernetes.InCluster(s.Namespace)
	if err != nil {
		return err
	}
	finishOpen := diagnostics.StartPhase("registry_open", diagnostics.TaskAttributes(request.TaskID, "")...)
	store, err := OpenWorkspace(s.OwnerID)
	finishOpen(err)
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
