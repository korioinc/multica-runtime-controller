package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

type monitorFixture struct {
	runner  *Runner
	attempt *attempt
	options workspace.Options
	request wire.Request
}

func newMonitorFixture(t *testing.T) monitorFixture {
	t.Helper()
	j, a := journalAttempt(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := workspace.Options{Directory: filepath.Join(root, ".multica-runtime/state"), WorkspaceRoot: root, SessionRoot: filepath.Join(root, "sessions"), OwnerID: j.owner}
	store, err := workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	a.Ref.FixedNode = "fixture-node"
	controller := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: a.Ref.Owner.Name, Namespace: a.Ref.Namespace, UID: types.UID(a.Ref.Owner.UID)}, Spec: corev1.PodSpec{NodeName: a.Ref.FixedNode}}
	api := fake.NewClientset(controller)
	// Isolate only Kubernetes: use its actual resource builders and metadata
	// checks, and allocate UIDs as the API does. This is not cluster coverage.
	api.PrependReactor("create", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		action.(clienttesting.CreateAction).GetObject().(metav1.Object).SetUID(types.UID(uuid.NewString()))
		return false, nil, nil
	})
	resources := &kubernetes.Client{API: api, Namespace: a.Ref.Namespace}
	taskRoot := "/workspace/fixture/" + a.Ref.TaskID
	request := wire.Request{SchemaVersion: wire.RequestSchemaVersion, TaskID: a.Ref.TaskID, Provider: "pi", Args: []string{"--session", wire.PiSessionsRoot + "/" + uuid.NewString() + ".jsonl"}, Env: []string{"MULTICA_TASK_ID=" + a.Ref.TaskID, "MULTICA_TASK_CONFIG_ROOT=" + taskRoot + "/multica-config", "MULTICA_DAEMON_PORT=4321"}, WorkDir: taskRoot + "/workdir", WorkerSubPath: workspace.StoragePrefix + "/" + a.Ref.StorageID, RuntimeRef: a.Ref.RuntimeRef, AttemptID: a.Ref.AttemptID, OwnerID: j.owner, HomeDigest: a.Ref.PodDigest, BrokerPort: 3210, BrokerToken: strings.Repeat("t", 32), TerminationGraceSeconds: 10}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	a.Ref.RequestDigest = wire.Digest(raw)
	cfg := kubernetes.Config{Platform: a.Ref.RuntimeRef.Platform, ImagePullPolicy: corev1.PullIfNotPresent, WorkspaceClaim: "workspace", WorkspaceAccessMode: corev1.ReadWriteMany, SingleNodeName: a.Ref.FixedNode, ServiceAccount: "worker", TaskDeadlineSeconds: 60, TerminationGraceSeconds: 10}
	a.Ref.PodDigest, err = kubernetes.PodFingerprint(cfg, a.Ref, request, "http://controller:8080")
	if err != nil {
		t.Fatal(err)
	}
	if err := j.save(a); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{selection: Selection{OwnerID: j.owner, Controller: a.Ref.Owner, Namespace: a.Ref.Namespace, Worker: cfg}, resources: resources, store: store, journal: j}
	return monitorFixture{runner: runner, attempt: a, options: options, request: request}
}

func (f monitorFixture) createInitializingPod(t *testing.T) {
	t.Helper()
	a, r := f.attempt, f.runner
	var err error
	a.SecretStarted = true
	a.Ref.SecretUID, err = r.resources.CreateSecret(t.Context(), a.Ref, f.request)
	if err != nil {
		t.Fatal(err)
	}
	a.PodStarted = true
	a.Ref.PodUID, err = r.resources.CreatePod(t.Context(), r.selection.Worker, a.Ref, f.request, "http://controller:8080")
	if err != nil {
		t.Fatal(err)
	}
	pod, err := r.resources.API.CoreV1().Pods(a.Ref.Namespace).Get(t.Context(), a.Ref.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodPending
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "home-layout", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}}}
	if _, err := r.resources.API.CoreV1().Pods(a.Ref.Namespace).UpdateStatus(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.journal.save(a); err != nil {
		t.Fatal(err)
	}
}

func serveTestMonitor(t *testing.T, runner *Runner, listener net.Listener) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.ServeAttemptMonitor(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
			t.Error("attempt monitor stopped unexpectedly", err)
		}
	})
}

func awaitMonitorCleanup(t *testing.T, f monitorFixture) {
	t.Helper()
	// The enclosing go test deadline is the deadlock guard; no cleanup latency
	// or polling policy is asserted, and no collector is running in these tests.
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		_, err := f.runner.journal.read(f.attempt.Ref.AttemptID)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-t.Context().Done():
			t.Fatal("test canceled before orphan cleanup completed", t.Context().Err())
		case <-poll.C:
		}
	}
	r := f.attempt.Ref
	if _, err := f.runner.resources.API.CoreV1().Pods(r.Namespace).Get(t.Context(), r.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("completed recovery left the initializing worker alive", err)
	}
	if _, err := f.runner.resources.API.CoreV1().Secrets(r.Namespace).Get(t.Context(), r.SecretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("completed recovery left the request credentials behind", err)
	}
}

type monitorChildInput struct {
	Options workspace.Options
	Storage string
	Attempt string
	Socket  string
}

func TestAttemptMonitorChildProcess(t *testing.T) {
	raw := os.Getenv("MULTICA_MONITOR_TEST_CHILD")
	if raw == "" {
		return
	}
	ready := os.NewFile(3, "monitor-ready")
	defer ready.Close()
	var input monitorChildInput
	if err := json.Unmarshal([]byte(raw), &input); err != nil {
		t.Fatal(err)
	}
	store, err := workspace.Open(input.Options)
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.AcquireLease(input.Storage)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	conn, err := net.Dial("unix", input.Socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := registerAttempt(t.Context(), conn, input.Attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := ready.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	ready.Close()
	// Keep the process in real I/O until the parent kills it with both lease
	// and socket held, including when this test binary runs without a timeout.
	var next [1]byte
	_, err = conn.Read(next[:])
	t.Fatal("child monitor connection ended before SIGKILL", err)
}

func TestAttemptMonitorReclaimsInitializingPodAfterShimKilled(t *testing.T) {
	for _, losePodResponse := range []bool{false, true} {
		name := "recorded Pod UID"
		if losePodResponse {
			name = "Pod create response not journaled"
		}
		t.Run(name, func(t *testing.T) {
			f := newMonitorFixture(t)
			f.createInitializingPod(t)
			if losePodResponse {
				f.attempt.Ref.PodUID = ""
				if err := f.runner.journal.save(f.attempt); err != nil {
					t.Fatal(err)
				}
			}
			// Keep Unix paths below the operating system's sockaddr limit.
			socketDir, err := os.MkdirTemp("/tmp", "attempt-monitor-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(socketDir) })
			socket := filepath.Join(socketDir, "monitor.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			serveTestMonitor(t, f.runner, listener)
			input, err := json.Marshal(monitorChildInput{Options: f.options, Storage: f.attempt.Ref.StorageID, Attempt: f.attempt.Ref.AttemptID, Socket: socket})
			if err != nil {
				t.Fatal(err)
			}
			readyRead, readyWrite, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer readyRead.Close()
			var output bytes.Buffer
			cmd := exec.Command(os.Args[0], "-test.run=^TestAttemptMonitorChildProcess$")
			cmd.Env = append(os.Environ(), "MULTICA_MONITOR_TEST_CHILD="+string(input))
			cmd.ExtraFiles = []*os.File{readyWrite}
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := cmd.Start(); err != nil {
				readyWrite.Close()
				t.Fatal(err)
			}
			readyWrite.Close()
			done := make(chan struct{})
			go func() { _ = cmd.Wait(); close(done) }()
			t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
			if _, err := io.ReadFull(readyRead, make([]byte, 1)); err != nil {
				<-done
				t.Fatal("child could not acquire its execution authority", err, output.String())
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			<-done
			awaitMonitorCleanup(t, f)
		})
	}
}

// net.Pipe makes the connection-loss/lease interleaving controllable by
// synctest, while the subprocess test above exercises a real Unix socket.
type monitorPipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
	once        sync.Once
}

func (l *monitorPipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *monitorPipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}
func (l *monitorPipeListener) Addr() net.Addr { return &net.UnixAddr{Net: "unix"} }

func connectPipeMonitor(t *testing.T, f monitorFixture) net.Conn {
	t.Helper()
	listener := &monitorPipeListener{connections: make(chan net.Conn, 1), closed: make(chan struct{})}
	client, server := net.Pipe()
	listener.connections <- server
	serveTestMonitor(t, f.runner, listener)
	t.Cleanup(func() { client.Close() })
	if err := registerAttempt(t.Context(), client, f.attempt.Ref.AttemptID); err != nil {
		t.Fatal(err)
	}
	return client
}

func TestAttemptMonitorWaitsForLiveLeaseAndRereadsAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newMonitorFixture(t)
		release, err := f.runner.store.AcquireLease(f.attempt.Ref.StorageID)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		conn := connectPipeMonitor(t, f)
		// Creation and journal updates happen after registration, as in Run.
		f.createInitializingPod(t)
		conn.Close()
		synctest.Wait()
		r := f.attempt.Ref
		if _, err := f.runner.resources.API.CoreV1().Pods(r.Namespace).Get(t.Context(), r.PodName, metav1.GetOptions{}); err != nil {
			t.Fatal("connection loss deleted a worker whose execution still owns storage", err)
		}
		if _, err := f.runner.resources.API.CoreV1().Secrets(r.Namespace).Get(t.Context(), r.SecretName, metav1.GetOptions{}); err != nil {
			t.Fatal("connection loss deleted live execution credentials", err)
		}
		release()
		awaitMonitorCleanup(t, f)
	})
}

func TestAttemptMonitorPreservesReplacementPod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newMonitorFixture(t)
		f.createInitializingPod(t)
		conn := connectPipeMonitor(t, f)
		r := f.attempt.Ref
		pods := f.runner.resources.API.CoreV1().Pods(r.Namespace)
		replacement, err := pods.Get(t.Context(), r.PodName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := pods.Delete(t.Context(), r.PodName, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		replacement.UID, replacement.ResourceVersion = "", ""
		replacement, err = pods.Create(t.Context(), replacement, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
		synctest.Wait()
		got, err := pods.Get(t.Context(), r.PodName, metav1.GetOptions{})
		if err != nil || got.UID != replacement.UID {
			t.Fatal("stale execution authority deleted a replacement worker", err)
		}
		if _, err := f.runner.resources.API.CoreV1().Secrets(r.Namespace).Get(t.Context(), r.SecretName, metav1.GetOptions{}); err != nil {
			t.Fatal("stale execution authority deleted credentials referenced by the replacement", err)
		}
		if _, err := f.runner.journal.read(r.AttemptID); err != nil {
			t.Fatal("unresolved replacement lost its durable recovery record", err)
		}
	})
}
