package execution

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/githubauth"
	"github.com/korioinc/multica-runtime-controller/internal/official"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type preparedTask struct {
	request                     wire.Request
	root, workerRoot, storageID string
}

func (r *Runner) authorizeTask(request wire.Request) (preparedTask, error) {
	return r.prepareAuthorizedTask(request, nil)
}

func (r *Runner) prepareAuthorizedTask(request wire.Request, prior *preparedTask) (preparedTask, error) {
	var empty preparedTask
	root, err := wire.StorageRoot(request)
	if err != nil {
		return empty, err
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root {
		return empty, errors.New("task_authorization: noncanonical preparation root")
	}
	session, err := wire.PiSession(request)
	if err != nil {
		return empty, err
	}
	var claim workspace.Claim
	var binding workspace.Binding
	if prior == nil {
		claim, binding, err = r.store.AuthorizeAndBind(request.TaskID, wire.Value(request.Env, "MULTICA_TOKEN"), wire.Value(request.Env, "MULTICA_WORKSPACE_ID"), wire.Value(request.Env, "MULTICA_AGENT_ID"), root, session, r.selection.RuntimeRef)
	} else {
		if root != prior.root || !r.selection.RuntimeRef.Equal(prior.request.RuntimeRef) {
			return empty, errors.New("task execution authority changed")
		}
		claim, binding, err = r.store.ReauthorizeBinding(request.TaskID, wire.Value(request.Env, "MULTICA_TOKEN"), wire.Value(request.Env, "MULTICA_WORKSPACE_ID"), wire.Value(request.Env, "MULTICA_AGENT_ID"), root, session, prior.request.WorkerSubPath, prior.request.RuntimeRef)
	}
	if err != nil {
		return empty, err
	}
	// Filtering/selectAttempt must not mutate the original request used for
	// reauthorization, or another caller sharing its environment backing array.
	request.Env = slices.Clone(request.Env)
	request.Env = slices.DeleteFunc(request.Env, func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		if key == githubauth.EnabledEnv || githubapp.ControllerEnvironmentKey(key) {
			return true
		}
		_, inherited := r.manifest.Env[key]
		return inherited && !slices.Contains(r.selection.OperatorKeys, key) && !slices.Contains(claim.TaskEnvKeys, key)
	})
	// The official provider boundary drops inherited MULTICA_* variables. The
	// App mode belongs to the admitted controller, so restore it from selection
	// after claim authorization instead of trusting a shim-supplied marker.
	if r.selection.GitHubApp {
		request.Env = append(request.Env, githubauth.EnabledEnv+"=true")
	}
	request.WorkerSubPath = binding.WorkerSubPath
	request.RuntimeRef = r.selection.RuntimeRef
	request.RepositoryURLs = claim.RepositoryURLs
	return preparedTask{request: request, root: root, workerRoot: filepath.Join(wire.WorkspaceRoot, binding.WorkerSubPath), storageID: filepath.Base(binding.WorkerSubPath)}, nil
}

func (r *Runner) selectAttempt(request wire.Request) wire.Request {
	request.SchemaVersion = wire.RequestSchemaVersion
	request.RuntimeRef = r.selection.RuntimeRef
	request.OwnerID = r.selection.OwnerID
	request.AttemptID = uuid.NewString()
	request.TerminationGraceSeconds = int(r.selection.Worker.TerminationGraceSeconds)
	for i, entry := range request.Env {
		if strings.HasPrefix(entry, "MULTICA_SERVER_URL=") {
			request.Env[i] = "MULTICA_SERVER_URL=" + r.selection.Backend
		}
	}
	return request
}

// This mutates only the authorized storage while Run holds its lease. A failed
// publication cannot expose a partial HOME to an init container.
func (r *Runner) prepareTaskContext(task *preparedTask) error {
	prepared, err := official.ReadTaskContext(task.root)
	if err != nil {
		return err
	}
	brief := checkout.ManagedText{Content: prepared.Brief, Begin: prepared.BriefBegin, End: prepared.BriefEnd}
	if err := checkout.SeedContext(task.workerRoot, wire.WorkspaceRoot+"/.multica-runtime/context/"+task.storageID+".json", prepared.Files, brief); err != nil {
		return err
	}
	bundle, err := configuration.Read(wire.ControlRoot)
	if err != nil {
		return err
	}
	task.request.HomeDigest, err = PrepareTaskHome(task.request, task.workerRoot, r.manifest, bundle)
	return err
}
