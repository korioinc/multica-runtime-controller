// verifyprofile proves offline environment preparation and worker execution.
// It does not observe claims or authorize tasks on a deployed Multica backend.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/environment"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
	runtimekube "github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}
func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}
func (b *synchronizedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buffer.Bytes()...)
}

type profile struct {
	Name            string            `json:"name"`
	Input           environment.Input `json:"input"`
	Ref             environment.Ref   `json:"ref"`
	ToolsClaim      string            `json:"toolsClaim"`
	ExpectedVersion string            `json:"expectedVersion"`
}
type report struct {
	Mode               string    `json:"mode"`
	Passed             bool      `json:"passed"`
	CoreImage          string    `json:"coreImage"`
	EnvironmentImage   string    `json:"environmentImage"`
	EnvironmentID      string    `json:"environmentID"`
	ProviderOutput     string    `json:"providerOutput,omitempty"`
	ProbeOutputs       []string  `json:"probeOutputs,omitempty"`
	PodUID             string    `json:"podUID,omitempty"`
	Node               string    `json:"node,omitempty"`
	RejectedReferences []string  `json:"rejectedReferences,omitempty"`
	Completed          time.Time `json:"completed"`
	Error              string    `json:"error,omitempty"`
}
type arguments struct{ mode, input, ref, coreImage, evidence, kubeconfig, selection, profile string }

func main() {
	var a arguments
	flag.StringVar(&a.mode, "mode", "", "consume or worker")
	flag.StringVar(&a.input, "input", "", "prepared environment input")
	flag.StringVar(&a.ref, "ref", "", "actual prepared READY reference")
	flag.StringVar(&a.coreImage, "core-image", "", "shared core image reference")
	flag.StringVar(&a.evidence, "evidence", "", "JSON evidence output")
	flag.StringVar(&a.kubeconfig, "kubeconfig", "", "explicit disposable K3s kubeconfig")
	flag.StringVar(&a.selection, "selection", "", "current controller selection")
	flag.StringVar(&a.profile, "profile", "", "prepared profile descriptor")
	flag.Parse()
	if err := run(a); err != nil {
		fmt.Fprintln(os.Stderr, "verifyprofile:", err)
		os.Exit(1)
	}
}
func load(path string, value any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, value)
}
func run(a arguments) (result error) {
	if os.Getenv("LOCALVERIFY_DISPOSABLE_CLUSTER") != "true" {
		return errors.New("explicit disposable verification guard required")
	}
	if a.mode == "consume" {
		if a.input != "/profile/input.json" || a.ref != "/profile/ready.json" || a.evidence != "/evidence/consumer.json" {
			return errors.New("consumer requires its explicit mounted fixture paths")
		}
	} else if a.mode == "worker" {
		if a.kubeconfig != "/etc/rancher/k3s/k3s.yaml" || a.selection != "/verification-evidence/selection.json" {
			return errors.New("worker requires the explicit local K3s fixture")
		}
		for _, path := range []string{a.profile, a.evidence} {
			if !filepath.IsAbs(path) || filepath.Clean(path) != path || !strings.HasPrefix(path, "/verification-evidence/profiles/") {
				return errors.New("profile evidence paths must stay in the fixture evidence directory")
			}
		}
	} else {
		return errors.New("unknown profile verification mode")
	}
	evidence := report{Mode: a.mode}
	defer func() {
		evidence.Completed = time.Now().UTC()
		evidence.Passed = result == nil
		if result != nil {
			evidence.Error = result.Error()
		}
		raw, err := json.MarshalIndent(evidence, "", "  ")
		if err == nil {
			err = os.WriteFile(a.evidence, append(raw, '\n'), 0600)
		}
		result = errors.Join(result, err)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	if a.mode == "consume" {
		return consume(ctx, a, &evidence)
	}
	return worker(ctx, a, &evidence)
}
func assertReadonly(path string) error {
	name := filepath.Join(path, ".profile-write-"+uuid.NewString())
	file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		file.Close()
		os.Remove(name)
		return fmt.Errorf("immutable mount is writable: %s", path)
	}
	if !errors.Is(err, syscall.EROFS) {
		return fmt.Errorf("read-only mount not established for %s: %w", path, err)
	}
	return nil
}
func consume(ctx context.Context, a arguments, evidence *report) error {
	input, err := environment.ReadInput(a.input)
	if err != nil {
		return err
	}
	var expected environment.Ref
	if err = load(a.ref, &expected); err != nil {
		return err
	}
	if !core.PinnedImage(a.coreImage) || input.CoreImage != a.coreImage {
		return errors.New("profile changed the selected core artifact")
	}
	ref, manifest, err := environment.Check(wire.EnvironmentRoot, wire.CoreRoot, input, &expected)
	if err != nil {
		return err
	}
	evidence.CoreImage = input.CoreImage
	evidence.EnvironmentImage = input.EnvironmentImage
	evidence.EnvironmentID = ref.EnvironmentID
	if _, exists := os.LookupEnv("PROFILE_INSTALL_ONLY"); exists {
		return errors.New("installer credential entered consumer environment")
	}
	if os.Getuid() != 65532 {
		return errors.New("consumer must run as UID 65532")
	}
	for _, root := range []string{wire.CoreRoot, wire.EnvironmentRoot} {
		if err = assertReadonly(root); err != nil {
			return err
		}
	}
	locations := environment.Locations{Root: wire.EnvironmentRoot, Home: wire.Home, TmpDir: "/tmp", Workspace: wire.WorkspaceRoot}
	if err = environment.CheckWritable(locations); err != nil {
		return err
	}
	if err = environment.CopySeed(wire.EnvironmentRoot, manifest, wire.Home); err != nil {
		return err
	}
	vars, err := execution.SelectedEnvironment(manifest, os.Environ(), locations)
	if err != nil {
		return err
	}
	for _, probe := range manifest.Checks {
		path, err := environment.Confined(wire.EnvironmentRoot, probe.Argv[0])
		if err != nil {
			return err
		}
		duration := time.Duration(probe.TimeoutSeconds) * time.Second
		if duration <= 0 {
			duration = 30 * time.Second
		}
		probeCtx, cancel := context.WithTimeout(ctx, duration)
		var output synchronizedBuffer
		result := execution.RunProcess(probeCtx, path, probe.Argv[1:], vars, wire.WorkspaceRoot, 10*time.Second, execution.ProcessStreams{Stdout: &output, Stderr: &output})
		cancel()
		if !result.Exited || result.Code != 0 || result.Err != nil {
			return fmt.Errorf("read-only tool probe failed: %w", errors.Join(result.Err, execution.ResultError(result)))
		}
		evidence.ProbeOutputs = append(evidence.ProbeOutputs, strings.TrimSpace(output.String()))
	}
	return nil
}
func worker(ctx context.Context, a arguments, evidence *report) (result error) {
	config, err := clientcmd.BuildConfigFromFlags("", a.kubeconfig)
	if err != nil {
		return err
	}
	origin, err := url.Parse(config.Host)
	if err != nil {
		return err
	}
	if origin.Scheme != "https" || origin.Hostname() != "127.0.0.1" && origin.Hostname() != "localhost" && origin.Hostname() != "::1" {
		return errors.New("only the node-local K3s API may be used")
	}
	var selected execution.Selection
	if err = load(a.selection, &selected); err != nil {
		return err
	}
	if err = selected.Validate(); err != nil {
		return err
	}
	if selected.Namespace != "runtime-verify" {
		return errors.New("profile selection is not disposable runtime-verify")
	}
	var p profile
	if err = load(a.profile, &p); err != nil {
		return err
	}
	if p.Name != "default" && p.Name != "go-rust" && p.Name != "php-python" {
		return errors.New("unknown fixture profile")
	}
	id, err := environment.Identity(p.Input)
	if err != nil {
		return err
	}
	if err = p.Ref.Validate(); err != nil {
		return err
	}
	if id != p.Ref.EnvironmentID || p.Input.CoreImage != selected.Worker.CoreImage || p.Ref.CoreImage != p.Input.CoreImage || p.Input.EnvironmentImage != p.Ref.EnvironmentImage || p.Input.Platform != selected.Input.Platform || p.ToolsClaim != "verify-profile-"+p.Name+"-"+id[:8] || p.ExpectedVersion == "" {
		return errors.New("profile identity differs from its actual core preparation")
	}
	cfg := selected.Worker
	cfg.EnvironmentImage = p.Input.EnvironmentImage
	cfg.EnvironmentID = id
	cfg.ToolsClaim = p.ToolsClaim
	cfg.ToolsAccessMode = corev1.ReadWriteOnce
	cfg.ConfigEnv = nil
	cfg.ConfigEnvFrom = nil
	cfg.ConfigVolumes = nil
	cfg.ConfigMounts = nil
	if err = cfg.Validate(); err != nil {
		return err
	}
	evidence.CoreImage = cfg.CoreImage
	evidence.EnvironmentImage = cfg.EnvironmentImage
	evidence.EnvironmentID = id
	api, err := clientset.NewForConfig(config)
	if err != nil {
		return err
	}
	client := &runtimekube.Client{API: api, Transport: config, Namespace: selected.Namespace}
	if _, err = client.Controller(ctx, selected.Controller.Name, selected.Controller.UID, cfg.SingleNodeName); err != nil {
		return err
	}
	// This path belongs to the disposable node, and must be the actual fixture PVC.
	pvc, err := api.CoreV1().PersistentVolumeClaims(selected.Namespace).Get(ctx, cfg.WorkspaceClaim, metav1.GetOptions{})
	if err != nil {
		return err
	}
	pv, err := api.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pv.Spec.HostPath == nil || pv.Spec.HostPath.Path != "/verification-workspace" {
		return errors.New("worker storage is not the explicit disposable hostPath")
	}
	storageID, taskID, workspaceID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	subPath := ".multica-runtime/workers/" + storageID
	root := filepath.Join("/verification-workspace", subPath)
	parent := filepath.Dir(root)
	canonical, err := filepath.EvalSymlinks(parent)
	if err != nil || canonical != parent {
		return errors.New("fixture worker parent is not canonical")
	}
	if err = os.Mkdir(root, 0700); err != nil {
		return err
	}
	// Newly allocated UUID directories only; existing task data is never adopted.
	for _, path := range []string{root, root + "/workdir", root + "/multica-config", root + "/codex-skills"} {
		if path != root {
			if err = os.Mkdir(path, 0700); err != nil {
				return err
			}
		}
		if err = os.Chown(path, 65532, 65532); err != nil {
			return err
		}
	}
	taskRoot := wire.WorkspaceRoot + "/" + workspaceID + "/" + taskID
	request := wire.Request{SchemaVersion: 1, TaskID: taskID, Provider: "codex", Args: []string{"--version"}, Env: wire.Environment(map[string]string{"MULTICA_TASK_ID": taskID, "MULTICA_TASK_CONFIG_ROOT": taskRoot + "/multica-config", "MULTICA_DAEMON_PORT": "19516", "MULTICA_TOKEN": "offline-profile-fixture"}), WorkDir: taskRoot + "/workdir", WorkerSubPath: subPath, Environment: p.Ref, EnvironmentInput: p.Input, AttemptID: uuid.NewString(), OwnerID: selected.OwnerID, BrokerPort: 19517, BrokerToken: uuid.NewString() + uuid.NewString(), TerminationGraceSeconds: int(cfg.TerminationGraceSeconds)}
	raw, err := json.Marshal(request)
	if err != nil {
		return err
	}
	if _, err = wire.Decode(raw); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(filepath.Dir(a.evidence), "request.json"), raw, 0600); err != nil {
		return err
	}
	ref := runtimekube.Reference{Namespace: selected.Namespace, Owner: selected.Controller, TaskID: taskID, StorageID: storageID, AttemptID: request.AttemptID, PodName: "task-worker-" + storageID, SecretName: "task-request-" + request.AttemptID, RequestDigest: wire.Digest(raw), CoreImage: cfg.CoreImage, EnvironmentImage: cfg.EnvironmentImage, EnvironmentID: id, FixedNode: cfg.SingleNodeName}
	ref.PodDigest, err = runtimekube.PodFingerprint(cfg, ref, request, selected.Gateway)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		result = errors.Join(result, client.Cleanup(cleanup, ref))
	}()
	ref.SecretUID, err = client.CreateSecret(ctx, ref, request)
	if err != nil {
		return err
	}
	ref.PodUID, err = client.CreatePod(ctx, cfg, ref, request, selected.Gateway)
	if err != nil {
		return err
	}
	var output synchronizedBuffer
	err = client.Execute(ctx, ref, 4*time.Minute, runtimekube.Streams{Stdout: &output, Stderr: &output})
	evidence.ProviderOutput = strings.TrimSpace(output.String())
	if writeErr := os.WriteFile(filepath.Join(filepath.Dir(a.evidence), "provider-output.log"), output.Bytes(), 0600); writeErr != nil {
		return errors.Join(err, writeErr)
	}
	if snapshot, snapshotErr := api.CoreV1().Pods(selected.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{}); snapshotErr == nil {
		body, _ := json.MarshalIndent(snapshot.Status, "", "  ")
		if writeErr := os.WriteFile(filepath.Join(filepath.Dir(a.evidence), "pod-status.json"), body, 0600); writeErr != nil {
			return errors.Join(err, writeErr)
		}
	}
	if err != nil {
		diagnostic := `test "${POD_UID:?}" = "$2"
set +e
id
cat /sys/fs/cgroup/memory.events
ls -ld "$1" "$HOME" /tmp /run/multica
printf 'base PATH=%s\n' "$PATH"
/opt/multica/environment/node/bin/node --version
cd "$1" && PATH="/opt/multica/environment/node/bin:/opt/multica/environment/bin:$PATH" /opt/multica/environment/providers/codex/run --version
cat /sys/fs/cgroup/memory.events
true`
		body, diagnosticErr := execPodOutput(ctx, api, config, selected.Namespace, ref.PodName, []string{"/bin/bash", "-ec", diagnostic, "profile", request.WorkDir, ref.PodUID})
		if diagnosticErr != nil {
			body += "\n" + diagnosticErr.Error()
		}
		if writeErr := os.WriteFile(filepath.Join(filepath.Dir(a.evidence), "failure-diagnostics.log"), []byte(body), 0600); writeErr != nil {
			return errors.Join(err, writeErr)
		}
		return fmt.Errorf("actual worker version process: %w", err)
	}
	if !strings.Contains(output.String(), p.ExpectedVersion) {
		return errors.New("worker did not execute the installed provider version")
	}
	if _, err = client.ResolvePod(ctx, ref); err != nil {
		return err
	}
	// Exec remains bound inside the container to the expected downward-API UID.
	// Kernel mount options plus denied writes distinguish RO mounts from mode bits.
	script := `test "${POD_UID:?}" = "$1"
 test "$(id -u)" = 65532
 test -z "${PROFILE_INSTALL_ONLY+x}"
 for root in /opt/multica/core /opt/multica/environment; do
   awk -v root="$root" '$5==root && $6 ~ /(^|,)ro(,|$)/ {found=1} END {exit !found}' /proc/self/mountinfo
   if touch "$root/$3" 2>/dev/null; then rm -f "$root/$3"; exit 1; fi
 done
 for root in "$HOME" /tmp "$2"; do printf writable > "$root/$3"; rm "$root/$3"; done`
	if err = execPod(ctx, api, config, selected.Namespace, ref.PodName, []string{"/bin/bash", "-ec", script, "profile", ref.PodUID, request.WorkDir, ".profile-" + uuid.NewString()}); err != nil {
		return fmt.Errorf("worker mount and credential boundary: %w", err)
	}
	pod, err := api.CoreV1().Pods(selected.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	evidence.CoreImage = cfg.CoreImage
	evidence.EnvironmentImage = cfg.EnvironmentImage
	evidence.EnvironmentID = id
	evidence.ProviderOutput = strings.TrimSpace(output.String())
	evidence.PodUID = ref.PodUID
	evidence.Node = pod.Spec.NodeName
	if evidence.ProbeOutputs, err = workerChecks(ctx, client, ref, request); err != nil {
		return err
	}
	if err = client.Cleanup(ctx, ref); err != nil {
		return err
	}
	for _, kind := range []string{"content-digest", "core-build"} {
		bad := request
		if kind == "content-digest" {
			bad.Environment.ContentDigest = strings.Repeat("0", 64)
			if bad.Environment.ContentDigest == p.Ref.ContentDigest {
				bad.Environment.ContentDigest = strings.Repeat("1", 64)
			}
		} else {
			bad.Environment.Core.BuildID += "-mismatched"
		}
		if err = rejectReference(ctx, client, cfg, ref, bad, selected.Gateway); err != nil {
			return fmt.Errorf("%s rejection: %w", kind, err)
		}
		evidence.RejectedReferences = append(evidence.RejectedReferences, kind)
	}
	return nil
}
func execPod(ctx context.Context, api *clientset.Clientset, config *rest.Config, namespace, name string, command []string) error {
	output, err := execPodOutput(ctx, api, config, namespace, name, command)
	if err != nil {
		return fmt.Errorf("native worker probe failed: %w: %s", err, strings.TrimSpace(output))
	}
	return nil
}
func execPodOutput(ctx context.Context, api *clientset.Clientset, config *rest.Config, namespace, name string, command []string) (string, error) {
	request := api.CoreV1().RESTClient().Post().Resource("pods").Namespace(namespace).Name(name).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: "worker", Command: command, Stdout: true, Stderr: true}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(config, "POST", request.URL())
	if err != nil {
		return "", err
	}
	var output synchronizedBuffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &output, Stderr: &output})
	return output.String(), err
}

// Keep the installed tools, image, writable storage and process command identical
// to the successful worker. Only its expected immutable reference is changed.
func rejectReference(ctx context.Context, client *runtimekube.Client, cfg runtimekube.Config, ref runtimekube.Reference, request wire.Request, gateway string) (result error) {
	request.AttemptID = uuid.NewString()
	raw, err := json.Marshal(request)
	if err != nil {
		return err
	}
	ref.AttemptID = request.AttemptID
	ref.SecretName = "task-request-" + request.AttemptID
	ref.RequestDigest = wire.Digest(raw)
	ref.SecretUID = ""
	ref.PodUID = ""
	ref.PodDigest, err = runtimekube.PodFingerprint(cfg, ref, request, gateway)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		result = errors.Join(result, client.Cleanup(cleanup, ref))
	}()
	ref.SecretUID, err = client.CreateSecret(ctx, ref, request)
	if err != nil {
		return err
	}
	ref.PodUID, err = client.CreatePod(ctx, cfg, ref, request, gateway)
	if err != nil {
		return err
	}
	return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		pod, err := client.API.CoreV1().Pods(ref.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if string(pod.UID) != ref.PodUID {
			return false, errors.New("invalid-reference worker was unexpectedly replaced")
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != "worker" {
				continue
			}
			if status.Ready {
				return false, errors.New("worker became ready despite a mismatched immutable reference")
			}
			if status.State.Terminated != nil {
				if status.State.Terminated.ExitCode == 0 {
					return false, errors.New("worker accepted an invalid reference as success")
				}
				return true, nil
			}
		}
		return false, nil
	})
}

// Read the verified manifest and image environment from the actual worker,
// then execute each argv literally under its selected task environment.
func workerChecks(ctx context.Context, client *runtimekube.Client, ref runtimekube.Reference, request wire.Request) ([]string, error) {
	if _, err := client.ResolvePod(ctx, ref); err != nil {
		return nil, err
	}
	api, ok := client.API.(*clientset.Clientset)
	if !ok {
		return nil, errors.New("actual Kubernetes client required")
	}
	raw, err := execPodOutput(ctx, api, client.Transport, ref.Namespace, ref.PodName, []string{"/bin/cat", wire.EnvironmentRoot + "/environment.json"})
	if err != nil {
		return nil, err
	}
	if core.Digest([]byte(raw)) != request.Environment.ManifestDigest {
		return nil, errors.New("worker check manifest differs from the executed environment")
	}
	var manifest environment.Manifest
	if err = json.Unmarshal([]byte(raw), &manifest); err != nil {
		return nil, err
	}
	base, err := execPodOutput(ctx, api, client.Transport, ref.Namespace, ref.PodName, []string{"/usr/bin/env", "-0"})
	if err != nil {
		return nil, err
	}
	baseEnv := strings.Split(strings.TrimSuffix(base, "\x00"), "\x00")
	if wire.Value(baseEnv, "POD_UID") != ref.PodUID || wire.Value(baseEnv, "MULTICA_TASK_ID") != request.TaskID {
		return nil, errors.New("worker check environment identity changed")
	}
	// Core already verified these RO directories in the worker. Resolve them in
	// that namespace; the host performs only the shared environment string merge.
	var directories []string
	for _, dir := range manifest.BinDirs {
		resolved, err := execPodOutput(ctx, api, client.Transport, ref.Namespace, ref.PodName, []string{"/usr/bin/realpath", "--", filepath.Join(wire.EnvironmentRoot, dir)})
		if err != nil {
			return nil, err
		}
		path := strings.TrimSuffix(resolved, "\n")
		if !strings.HasPrefix(path, wire.EnvironmentRoot+"/") {
			return nil, errors.New("worker check bin directory escaped the tools prefix")
		}
		directories = append(directories, path)
	}
	stringManifest := manifest
	stringManifest.BinDirs = nil
	vars, err := execution.SelectedEnvironment(stringManifest, baseEnv, environment.Locations{Root: wire.EnvironmentRoot, Home: wire.Home, TmpDir: "/tmp", Workspace: request.WorkDir})
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, entry := range append(vars, request.Env...) {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	parts := append([]string{wire.ControlRoot + "/providers", wire.CoreRoot}, directories...)
	for _, part := range strings.Split(wire.Value(vars, "PATH"), ":") {
		if part != "" && !strings.HasPrefix(part, wire.CoreRoot) {
			parts = append(parts, part)
		}
	}
	values["PATH"] = strings.Join(parts, ":")
	values["HOME"] = wire.Home
	values["TMPDIR"] = "/tmp"
	for key := range values {
		if strings.HasPrefix(key, "MULTICA_") && strings.HasSuffix(key, "_PATH") {
			delete(values, key)
		}
	}
	values["MULTICA_REPO_CHECKOUT_MODE"] = "isolated"
	if request.Provider == "codex" {
		values["CODEX_HOME"] = wire.Home + "/.codex"
	}
	var outputs []string
	for _, probe := range manifest.Checks {
		if _, err = client.ResolvePod(ctx, ref); err != nil {
			return outputs, err
		}
		// Only the fixed wrapper is shell code. Environment assignments and every
		// check argument are distinct positional values, never interpolated as code.
		command := []string{"/bin/bash", "-ec", `test "${POD_UID:?}" = "$1"; cd "$2"; shift 2; exec "$@"`, "profile", ref.PodUID, request.WorkDir, "/usr/bin/env", "-i"}
		command = append(command, wire.Environment(values)...)
		command = append(command, filepath.Join(wire.EnvironmentRoot, probe.Argv[0]))
		command = append(command, probe.Argv[1:]...)
		timeout := time.Duration(probe.TimeoutSeconds) * time.Second
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		output, execErr := execPodOutput(probeCtx, api, client.Transport, ref.Namespace, ref.PodName, command)
		cancel()
		code, exited := runtimekube.ExitCode(execErr)
		if !exited {
			outputs = append(outputs, fmt.Sprintf("%s (execution unconfirmed): %s", probe.Argv[0], strings.TrimSpace(output)))
			return outputs, fmt.Errorf("actual task check transport: %w", execErr)
		}
		outputs = append(outputs, fmt.Sprintf("%s (exit %d): %s", probe.Argv[0], code, strings.TrimSpace(output)))
		if execErr != nil || code != 0 {
			return outputs, fmt.Errorf("actual task check %s failed: %w", probe.Argv[0], execErr)
		}
	}
	return outputs, nil
}
