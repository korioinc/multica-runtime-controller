// Package wire contains the versioned values shared by the controller and worker.
// Parsing these values never grants permission to execute a task.
package wire

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

const (
	RequestSchemaVersion = 3
	ControllerRoot       = core.Root
	PrivateRoot          = "/opt/multica/private"
	WorkspaceRoot        = "/workspace"
	Home                 = configuration.Home
	PiSessionsRoot       = Home + "/.multica/pi-sessions"
	ControlRoot          = "/run/multica"
	RequestPath          = "/etc/multica/task/request.json"
	HomeArtifactPath     = "/etc/multica/home/task-home.tar"
	RequestKey           = "request.json"
	MaxRequestBytes      = 1 << 20
	SelectionPath        = ControlRoot + "/selection.json"
	WorkerConfigPath     = "/etc/multica/runtime/worker.json"
	SecretHeader         = "X-Multica-Request-Secret"
	TaskHeader           = "X-Multica-Task-ID"
	TokenHeader          = "X-Multica-Task-Token"
	CapabilityHeader     = "X-Multica-Broker-Token"
	BranchHeader         = "X-Multica-Checkout-Branch"
	ArchiveType          = "application/vnd.multica.checkout+tar"
)

type Request struct {
	SchemaVersion           int              `json:"schemaVersion"`
	TaskID                  string           `json:"taskID"`
	Provider                string           `json:"provider"`
	Args                    []string         `json:"args"`
	Env                     []string         `json:"env"`
	WorkDir                 string           `json:"workDir"`
	WorkerSubPath           string           `json:"workerSubPath"`
	RepositoryURLs          []string         `json:"repositoryURLs"`
	RuntimeRef              runtimeimage.Ref `json:"runtimeRef"`
	AttemptID               string           `json:"attemptID"`
	OwnerID                 string           `json:"ownerID"`
	HomeDigest              string           `json:"homeDigest"`
	BrokerPort              int              `json:"brokerPort"`
	BrokerToken             string           `json:"brokerToken"`
	TerminationGraceSeconds int              `json:"terminationGraceSeconds"`
}

// Plan and Result preserve the official checkout protocol. The assigned URL is
// checked by the controller before this plan reaches the official daemon.
type Plan struct {
	URL     string `json:"url"`
	Ref     string `json:"ref"`
	WorkDir string `json:"workdir"`
	TaskID  string `json:"task_id"`
}
type Result struct {
	Path       string `json:"path"`
	BranchName string `json:"branch_name"`
}

func Digest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func Value(env []string, key string) string {
	for i := len(env) - 1; i >= 0; i-- {
		if k, v, ok := strings.Cut(env[i], "="); ok && k == key {
			return v
		}
	}
	return ""
}
func UUID(value string) bool { id, err := uuid.Parse(value); return err == nil && id.String() == value }
func Alias(provider string) string {
	switch provider {
	case "antigravity":
		return "agy"
	case "pi", "codex", "copilot":
		return provider
	}
	return ""
}
func Provider(alias string) string {
	if alias == "agy" {
		return "antigravity"
	}
	if Alias(alias) != "" {
		return alias
	}
	return ""
}

// TaskEnvironment removes controller and installation authority before the
// request leaves the controller. Executable selection is applied by the worker.
func TaskEnvironment(input []string) []string {
	values := map[string]string{}
	for _, item := range input {
		key, value, ok := strings.Cut(item, "=")
		if !ok || key == "" || reservedTaskKey(key) {
			continue
		}
		values[key] = value
	}
	delete(values, "PATH")
	values["HOME"], values["TMPDIR"], values["TMP"], values["TEMP"] = Home, "/tmp", "/tmp", "/tmp"
	values["MULTICA_REPO_CHECKOUT_MODE"] = "isolated"
	return Environment(values)
}
func reservedTaskKey(key string) bool {
	if strings.HasPrefix(key, "KUBERNETES_") || strings.HasPrefix(key, "POD_") || strings.HasPrefix(key, "ENV_") {
		return true
	}
	for _, prefix := range []string{"MULTICA_CORE_", "MULTICA_ENVIRONMENT_", "MULTICA_WORKER_", "MULTICA_CONTROLLER_", "MULTICA_TOOLS_", "MULTICA_OWNER_", "MULTICA_RUNTIME_", "MULTICA_OPERATOR_", "MULTICA_REQUEST_", "MULTICA_ATTEMPT_"} {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	if strings.HasPrefix(key, "MULTICA_") && strings.HasSuffix(key, "_PATH") {
		return true
	}
	switch key {
	case "MULTICA_DAEMON_ID", "MULTICA_DAEMON_PROXY_URL", "MULTICA_REQUEST_SECRET_NAME", "MULTICA_WORKSPACE_PVC_NAME", "MULTICA_TASK_DEADLINE", "CODEX_HOME", "PI_CODING_AGENT_DIR":
		return true
	}
	return false
}
func Environment(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

// StorageRoot accepts only the daemon-managed /workspace/<workspace>/<task>
// layout, independent of a repository's editable Git metadata.
func StorageRoot(request Request) (string, error) {
	config := Value(request.Env, "MULTICA_TASK_CONFIG_ROOT")
	if !filepath.IsAbs(config) || filepath.Clean(config) != config || filepath.Base(config) != "multica-config" {
		return "", errors.New("canonical task configuration root required")
	}
	root := filepath.Dir(config)
	rel, err := filepath.Rel(WorkspaceRoot, root)
	if err != nil {
		return "", err
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) != 2 || strings.HasPrefix(parts[0], ".") || strings.HasPrefix(parts[1], ".") || request.WorkDir != filepath.Join(root, "workdir") {
		return "", errors.New("only canonical daemon-managed tasks are supported")
	}
	return root, nil
}
func PiSession(request Request) (string, error) {
	if request.Provider != "pi" {
		return "", nil
	}
	var session string
	for i := 0; i < len(request.Args); i++ {
		arg := request.Args[i]
		var value string
		if arg == "--session" {
			i++
			if i == len(request.Args) {
				return "", errors.New("missing Pi session")
			}
			value = request.Args[i]
		} else if strings.HasPrefix(arg, "--session=") {
			value = strings.TrimPrefix(arg, "--session=")
		} else {
			continue
		}
		if session != "" || filepath.Clean(value) != value || filepath.Dir(value) != PiSessionsRoot || strings.HasPrefix(filepath.Base(value), ".") || filepath.Ext(value) != ".jsonl" {
			return "", errors.New("invalid daemon Pi session")
		}
		session = value
	}
	if session == "" {
		return "", errors.New("daemon Pi session required")
	}
	return session, nil
}
func Decode(raw []byte) (Request, error) {
	var request Request
	if len(raw) > MaxRequestBytes {
		return request, errors.New("task request exceeds Secret payload limit")
	}
	if err := runtimeimage.Decode(raw, &request); err != nil {
		return request, errors.New("invalid task request")
	}
	if request.SchemaVersion != RequestSchemaVersion || !UUID(request.TaskID) || !UUID(request.AttemptID) || !UUID(request.OwnerID) || Alias(request.Provider) == "" || request.TaskID != Value(request.Env, "MULTICA_TASK_ID") || request.BrokerPort < 1 || request.BrokerPort > 65535 || len(request.BrokerToken) < 32 || request.TerminationGraceSeconds < 1 {
		return request, errors.New("invalid task authority or request contract")
	}
	if err := request.RuntimeRef.Validate(); err != nil {
		return request, err
	}
	if !core.ValidSHA(request.HomeDigest) {
		return request, errors.New("task requires its prepared HOME digest")
	}
	if _, enabled := request.RuntimeRef.Providers[request.Provider]; !enabled {
		return request, errors.New("task provider is not enabled in the selected runtime")
	}
	if _, err := StorageRoot(request); err != nil {
		return request, err
	}
	if _, err := PiSession(request); err != nil {
		return request, err
	}
	if request.WorkerSubPath != ".multica-runtime/workers/"+filepath.Base(request.WorkerSubPath) || !UUID(filepath.Base(request.WorkerSubPath)) {
		return request, errors.New("invalid worker storage")
	}
	return request, nil
}
