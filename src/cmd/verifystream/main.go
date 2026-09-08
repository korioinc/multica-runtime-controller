// verifystream severs an upgraded, TLS-preserving SPDY connection in disposable
// K3s while the actual selected provider is alive and has sent no exit status.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
	runtimekube "github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

type fixtureEvidence struct {
	SchemaVersion  int            `json:"schemaVersion"`
	Started        time.Time      `json:"started"`
	Completed      time.Time      `json:"completed"`
	Namespace      string         `json:"namespace"`
	BeforeSHA256   string         `json:"beforeSHA256"`
	AfterSHA256    string         `json:"afterSHA256"`
	FilesPreserved bool           `json:"filesPreserved"`
	Cases          []caseEvidence `json:"cases"`
	Passed         bool           `json:"passed"`
	Error          string         `json:"error,omitempty"`
}
type caseEvidence struct {
	Mode                   string                `json:"mode"`
	Reference              runtimekube.Reference `json:"reference"`
	ProviderPID            int                   `json:"providerPID"`
	ProviderAlive          bool                  `json:"providerAliveBeforeSever"`
	CallerContextAlive     bool                  `json:"callerContextAlive"`
	TunnelConnections      int                   `json:"severedTunnelConnections"`
	Prefix                 string                `json:"prefix"`
	PrefixPreserved        bool                  `json:"prefixPreserved"`
	ReturnAfterSeverMillis int64                 `json:"returnAfterSeverMillis"`
	TransportError         bool                  `json:"transportError"`
	ExitCode               int                   `json:"exitCode"`
	Exited                 bool                  `json:"exited"`
	UIDCleanup             bool                  `json:"uidCleanup"`
	Error                  string                `json:"error,omitempty"`
}
type streamFixture struct {
	api       *clientset.Clientset
	config    *rest.Config
	client    *runtimekube.Client
	selection execution.Selection
	sample    wire.Request
}

func main() {
	kubeconfig := flag.String("kubeconfig", "/etc/rancher/k3s/k3s.yaml", "explicit disposable node kubeconfig")
	namespace := flag.String("namespace", "runtime-verify", "disposable namespace")
	selection := flag.String("selection", "/verification-evidence/selection.json", "current selection")
	request := flag.String("request", "/verification-evidence/request.json", "actual dummy-credential task request")
	evidence := flag.String("evidence", "/verification-evidence/transport.json", "proof output")
	flag.Parse()
	if err := run(*kubeconfig, *namespace, *selection, *request, *evidence); err != nil {
		fmt.Fprintln(os.Stderr, "verifystream:", err)
		os.Exit(1)
	}
	fmt.Println("actual live-provider SPDY socket sever rejected as transport failure; exact-UID cleanup preserved files")
}

func run(kubeconfig, namespace, selectionPath, requestPath, evidencePath string) (result error) {
	if os.Getenv("LOCALVERIFY_DISPOSABLE_CLUSTER") != "true" || kubeconfig != "/etc/rancher/k3s/k3s.yaml" || namespace != "runtime-verify" {
		return errors.New("explicit disposable K3s guard and runtime-verify namespace required")
	}
	for _, path := range []string{selectionPath, requestPath, evidencePath} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != "/verification-evidence" {
			return errors.New("fixture inputs/output must be direct files in /verification-evidence")
		}
	}
	proof := fixtureEvidence{SchemaVersion: 1, Started: time.Now().UTC(), Namespace: namespace}
	defer func() {
		proof.Completed = time.Now().UTC()
		if result != nil {
			proof.Error = result.Error()
		}
		raw, err := json.MarshalIndent(proof, "", "  ")
		if err == nil {
			err = os.WriteFile(evidencePath, append(raw, '\n'), 0600)
		}
		result = errors.Join(result, err)
	}()
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}
	target, err := url.Parse(config.Host)
	if err != nil {
		return err
	}
	if target.Scheme != "https" || target.User != nil || (target.Hostname() != "127.0.0.1" && target.Hostname() != "localhost" && target.Hostname() != "::1") {
		return errors.New("only the node-local HTTPS K3s API is permitted")
	}
	api, err := clientset.NewForConfig(config)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(selectionPath)
	if err != nil {
		return err
	}
	var selection execution.Selection
	if json.Unmarshal(raw, &selection) != nil {
		return errors.New("invalid fixture selection")
	}
	if err := selection.Validate(); err != nil {
		return err
	}
	if selection.Namespace != namespace {
		return errors.New("selection namespace mismatch")
	}
	raw, err = os.ReadFile(requestPath)
	if err != nil {
		return err
	}
	sample, err := wire.Decode(raw)
	if err != nil {
		return err
	}
	if sample.OwnerID != selection.OwnerID || !sample.RuntimeRef.Equal(selection.RuntimeRef) || sample.Provider != "pi" {
		return errors.New("sample request does not belong to the current Pi fixture environment")
	}
	sample.Snapshots = selection.Snapshots
	session, err := wire.PiSession(sample)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	client := &runtimekube.Client{API: api, Transport: config, Namespace: namespace}
	if _, err := client.Controller(ctx, selection.Controller.Name, selection.Controller.UID, selection.Worker.SingleNodeName); err != nil {
		return err
	}
	root, release, err := lockStorage(ctx, api, selection, sample)
	if err != nil {
		return err
	}
	defer release()
	if err := client.StorageAvailable(ctx, selection.Worker.WorkspaceClaim, filepath.Base(sample.WorkerSubPath), selection.Controller); err != nil {
		return err
	}
	before, err := snapshotStorage(root, session)
	if err != nil {
		return err
	}
	if len(before.Files) == 0 {
		return errors.New("sample worker has no existing files to preserve")
	}
	proof.BeforeSHA256 = before.digest()
	fixture := &streamFixture{api: api, config: config, client: client, selection: selection, sample: sample}
	// The SDK control records the previous behavior without requiring a specific
	// SDK bug: the production case must independently reject the missing status.
	for _, mode := range []string{"upstream-sdk-control", "runtime-strict-status"} {
		caseProof, err := fixture.severCase(ctx, target.Host, session, mode)
		proof.Cases = append(proof.Cases, caseProof)
		if err != nil {
			return err
		}
	}
	after, err := snapshotStorage(root, session)
	if err != nil {
		return err
	}
	proof.AfterSHA256 = after.digest()
	proof.FilesPreserved = snapshotsEqual(before, after)
	if !proof.FilesPreserved {
		return errors.New("stream interruption or cleanup changed sample worker/session files")
	}
	proof.Passed = true
	return nil
}

func (f *streamFixture) severCase(ctx context.Context, target, session, mode string) (proof caseEvidence, result error) {
	proof.Mode = mode
	request := f.sample
	request.AttemptID = uuid.NewString()
	request.Args = []string{"--fixture-transport-hold", "--session", session}
	raw, err := json.Marshal(request)
	if err != nil {
		return proof, err
	}
	if _, err := wire.Decode(raw); err != nil {
		return proof, err
	}
	ref := runtimekube.Reference{Namespace: f.selection.Namespace, Owner: f.selection.Controller, TaskID: request.TaskID, StorageID: filepath.Base(request.WorkerSubPath), AttemptID: request.AttemptID, PodName: "task-worker-" + filepath.Base(request.WorkerSubPath), SecretName: "task-request-" + request.AttemptID, RequestDigest: wire.Digest(raw), RuntimeRef: request.RuntimeRef, Snapshots: request.Snapshots, FixedNode: f.selection.Worker.SingleNodeName}
	ref.PodDigest, err = runtimekube.PodFingerprint(f.selection.Worker, ref, request, f.selection.Gateway)
	if err != nil {
		return proof, err
	}
	if _, err := f.api.CoreV1().Pods(ref.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return proof, errors.New("sample worker name is occupied; refusing to replace or adopt it")
	}
	defer func() {
		proof.Reference = ref
		if ref.SecretUID != "" || ref.PodUID != "" {
			cleanup, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			cleanupErr := f.client.Cleanup(cleanup, ref)
			proof.UIDCleanup = cleanupErr == nil
			result = errors.Join(result, cleanupErr)
		}
		if result != nil {
			proof.Error = result.Error()
		}
	}()
	ref.SecretUID, err = f.client.CreateSecret(ctx, ref, request)
	if err != nil {
		ref.SecretUID, _ = f.client.ResolveSecret(ctx, ref)
		return proof, err
	}
	ref.PodUID, err = f.client.CreatePod(ctx, f.selection.Worker, ref, request, f.selection.Gateway)
	if err != nil {
		ref.PodUID, _ = f.client.ResolvePod(ctx, ref)
		return proof, err
	}
	if err := f.awaitReady(ctx, ref); err != nil {
		return proof, err
	}
	tunnel, err := newTunnel(target)
	if err != nil {
		return proof, err
	}
	defer tunnel.close()
	transport := rest.CopyConfig(f.config)
	transport.Proxy = func(*http.Request) (*url.URL, error) { return tunnel.URL(), nil }
	// API, credentials and TLS remain untouched. Only exec's proxy differs.
	client := &runtimekube.Client{API: f.api, Transport: transport, Namespace: f.selection.Namespace}
	executeContext, cancel := context.WithCancel(ctx)
	defer cancel()
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	output := &markerOutput{prefix: "verifystream-ready:" + request.AttemptID + ":", ready: make(chan int, 1)}
	var stderr lockedBuffer
	finished := make(chan error, 1)
	go func() {
		if mode == "upstream-sdk-control" {
			finished <- f.sdkExecute(executeContext, transport, ref, input, output, &stderr)
		} else {
			finished <- client.Execute(executeContext, ref, time.Duration(f.selection.Worker.TaskDeadlineSeconds)*time.Second, runtimekube.Streams{Stdin: input, Stdout: output, Stderr: &stderr})
		}
	}()
	var pid int
	select {
	case pid = <-output.ready:
	case err := <-finished:
		return proof, fmt.Errorf("provider never entered the live stream hold: %w; stderr %s", err, stderr.String())
	case <-time.After(30 * time.Second):
		return proof, errors.New("provider did not produce its live stream marker")
	case <-ctx.Done():
		return proof, ctx.Err()
	}
	proof.ProviderPID = pid
	proof.Prefix = output.line()
	if err := f.providerAlive(ctx, ref, pid); err != nil {
		return proof, err
	}
	proof.ProviderAlive = true
	select {
	case err := <-finished:
		return proof, fmt.Errorf("exec returned before the remote socket sever: %w", err)
	default:
	}
	if executeContext.Err() != nil {
		return proof, errors.New("caller execution context ended before the remote sever")
	}
	severed, err := tunnel.sever()
	if err != nil {
		return proof, err
	}
	proof.TunnelConnections = severed
	cut := time.Now()
	var streamErr error
	select {
	case streamErr = <-finished:
	case <-time.After(10 * time.Second):
		return proof, errors.New("severed upgraded stream did not return while caller context remained live")
	case <-ctx.Done():
		return proof, ctx.Err()
	}
	proof.ReturnAfterSeverMillis = time.Since(cut).Milliseconds()
	proof.CallerContextAlive = executeContext.Err() == nil
	proof.TransportError = errors.Is(streamErr, runtimekube.ErrTransport)
	proof.ExitCode, proof.Exited = runtimekube.ExitCode(streamErr)
	proof.PrefixPreserved = bytes.Equal(output.Bytes(), []byte(proof.Prefix))
	if !proof.CallerContextAlive || !proof.PrefixPreserved {
		return proof, errors.New("socket sever lost the received prefix or relied on caller cancellation")
	}
	if mode == "runtime-strict-status" && (!proof.TransportError || proof.Exited) {
		return proof, fmt.Errorf("missing remote status was not rejected as transport failure: exit=%d exited=%t error=%v", proof.ExitCode, proof.Exited, streamErr)
	}
	fmt.Printf("remote stream case %s: transport=%t exited=%t code=%d\n", mode, proof.TransportError, proof.Exited, proof.ExitCode)
	return proof, nil
}

func (f *streamFixture) awaitReady(ctx context.Context, ref runtimekube.Reference) error {
	return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		uid, err := f.client.ResolvePod(ctx, ref)
		if err != nil || uid != ref.PodUID {
			return false, errors.New("fixture Pod changed before streaming")
		}
		pod, err := f.api.CoreV1().Pods(ref.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return false, errors.New("fixture worker terminated before streaming")
		}
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name == "worker" && container.Ready {
				return true, nil
			}
		}
		return false, nil
	})
}
func (f *streamFixture) sdkExecute(ctx context.Context, config *rest.Config, ref runtimekube.Reference, input io.Reader, output, stderr io.Writer) error {
	options := &corev1.PodExecOptions{Container: "worker", Command: []string{wire.ControllerRoot + "/runtime", "worker", "execute", "--request-digest=" + ref.RequestDigest, "--pod-uid=" + ref.PodUID, "--stdin=true", "--stdout=true", "--stderr=true"}, Stdin: true, Stdout: true, Stderr: true}
	target := f.api.CoreV1().RESTClient().Post().Namespace(ref.Namespace).Resource("pods").Name(ref.PodName).SubResource("exec").VersionedParams(options, scheme.ParameterCodec).URL()
	executor, err := remotecommand.NewSPDYExecutor(config, http.MethodPost, target)
	if err != nil {
		return err
	}
	return executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: input, Stdout: output, Stderr: stderr})
}
func (f *streamFixture) providerAlive(ctx context.Context, ref runtimekube.Reference, pid int) error {
	probe, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	options := &corev1.PodExecOptions{Container: "worker", Command: []string{"/bin/bash", "-c", "kill -0 \"$1\" && printf provider-alive", "verifystream", strconv.Itoa(pid)}, Stdout: true, Stderr: true}
	target := f.api.CoreV1().RESTClient().Post().Namespace(ref.Namespace).Resource("pods").Name(ref.PodName).SubResource("exec").VersionedParams(options, scheme.ParameterCodec).URL()
	executor, err := remotecommand.NewSPDYExecutor(f.config, http.MethodPost, target)
	if err != nil {
		return err
	}
	var output, stderr bytes.Buffer
	if err := executor.StreamWithContext(probe, remotecommand.StreamOptions{Stdout: &output, Stderr: &stderr}); err != nil {
		return fmt.Errorf("held provider PID was not alive: %w: %s", err, stderr.String())
	}
	if output.String() != "provider-alive" {
		return errors.New("held provider liveness was not confirmed")
	}
	return nil
}

type lockedBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(raw []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(raw)
}
func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

type markerOutput struct {
	mutex     sync.Mutex
	buffer    bytes.Buffer
	prefix    string
	ready     chan int
	announced string
}

func (m *markerOutput) Write(raw []byte) (int, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.buffer.Len()+len(raw) > 1<<20 {
		return 0, errors.New("unexpected oversized hold output")
	}
	n, _ := m.buffer.Write(raw)
	if m.announced == "" {
		if line, _, ok := strings.Cut(m.buffer.String(), "\n"); ok {
			if !strings.HasPrefix(line, m.prefix) {
				return 0, errors.New("unexpected provider stdout prefix")
			}
			pid, err := strconv.Atoi(strings.TrimPrefix(line, m.prefix))
			if err != nil || pid < 2 {
				return 0, errors.New("invalid live provider PID marker")
			}
			m.announced = line + "\n"
			m.ready <- pid
		}
	}
	return n, nil
}
func (m *markerOutput) line() string { m.mutex.Lock(); defer m.mutex.Unlock(); return m.announced }
func (m *markerOutput) Bytes() []byte {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return append([]byte{}, m.buffer.Bytes()...)
}
