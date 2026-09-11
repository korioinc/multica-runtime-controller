//go:build handoffintegration

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
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// The real preparation-root authority is absolute. Run this binary only in a
// disposable Linux container with an empty /workspace tmpfs, never a host PVC.
func nativeHandoff(t *testing.T) (monitorFixture, wire.Request) {
	t.Helper()
	if os.Getenv("HANDOFF_NATIVE_TEST") != "1" {
		t.Skip("requires explicitly disposable Linux /workspace tmpfs")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("native handoff requires Linux")
	}
	entries, err := os.ReadDir(wire.WorkspaceRoot)
	if err != nil || len(entries) != 0 {
		t.Fatal("native handoff requires a fresh empty /workspace tmpfs", err)
	}
	t.Cleanup(func() {
		entries, _ := os.ReadDir(wire.WorkspaceRoot)
		for _, entry := range entries {
			_ = os.RemoveAll(filepath.Join(wire.WorkspaceRoot, entry.Name()))
		}
	})
	f := newMonitorFixture(t)
	f.options.Directory = wire.WorkspaceRoot + "/.multica-runtime/state"
	f.options.WorkspaceRoot = wire.WorkspaceRoot
	f.options.SessionRoot = wire.PiSessionsRoot
	f.runner.store, err = workspace.Open(f.options)
	if err != nil {
		t.Fatal(err)
	}
	ref := f.attempt.Ref.RuntimeRef
	provider := ref.Providers["pi"]
	provider.Path = "/opt/tools/codex"
	ref.Providers = map[string]runtimeimage.Executable{"codex": provider}
	f.attempt.Ref.RuntimeRef = ref
	f.request.RuntimeRef, f.request.Provider = ref, "codex"
	f.runner.selection.RuntimeRef = ref
	workspaceID, agentID := uuid.NewString(), uuid.NewString()
	observation := workspace.Observation{ID: f.request.TaskID, WorkspaceID: workspaceID, AgentID: agentID, AuthToken: "mat_" + uuid.NewString(), RuntimeRef: ref}
	if _, err := f.runner.store.ObserveBatch([]workspace.Observation{observation}); err != nil {
		t.Fatal(err)
	}
	root, err := wire.StorageRoot(f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "workdir"), 0700); err != nil {
		t.Fatal(err)
	}
	owner, _ := json.Marshal(map[string]string{"workspace_id": workspaceID, "task_id": observation.ID})
	if err := os.WriteFile(filepath.Join(root, ".task_owner"), owner, 0600); err != nil {
		t.Fatal(err)
	}
	// Codex has no registered native session in this controller. Native Pi
	// session reuse is covered by the real-daemon local integration harness.
	f.request.Args = nil
	f.request.Env = append(f.request.Env, "MULTICA_TOKEN="+observation.AuthToken, "MULTICA_WORKSPACE_ID="+workspaceID, "MULTICA_AGENT_ID="+agentID)
	prepared, err := f.runner.authorizeTask(f.request)
	if err != nil {
		t.Fatal(err)
	}
	f.request = prepared.request
	f.attempt.Ref.StorageID = prepared.storageID
	f.attempt.Ref.PodName = "task-worker-" + prepared.storageID
	encoded, _ := json.Marshal(f.request)
	f.attempt.Ref.RequestDigest = wire.Digest(encoded)
	f.attempt.Ref.PodDigest, err = kubernetes.PodFingerprint(f.runner.selection.Worker, f.attempt.Ref, f.request, "http://controller:8080")
	if err != nil {
		t.Fatal(err)
	}
	f.createInitializingPod(t)
	// B is a distinct claim in the same scope with authorized prior workdir.
	observation.ID = uuid.NewString()
	observation.AuthToken = "mat_" + uuid.NewString()
	observation.PriorWorkDir = f.request.WorkDir
	if _, err := f.runner.store.ObserveBatch([]workspace.Observation{observation}); err != nil {
		t.Fatal(err)
	}
	next := f.request
	next.TaskID, next.AttemptID = observation.ID, ""
	next.Env = []string{"MULTICA_TASK_ID=" + next.TaskID, "MULTICA_TASK_CONFIG_ROOT=" + root + "/multica-config", "MULTICA_TOKEN=" + observation.AuthToken, "MULTICA_WORKSPACE_ID=" + workspaceID, "MULTICA_AGENT_ID=" + agentID}
	// B's daemon-prepared sidecar gives the regression a durable oracle that
	// only B's context preparation can publish after it acquires/rechecks S.
	manifest, _ := json.Marshal(map[string][]string{"files": {root + "/workdir/next-input"}})
	for path, data := range map[string][]byte{
		root + "/.multica_sidecar_manifest.json": manifest,
		root + "/workdir/AGENTS.md":              []byte("follow-up task context"),
		root + "/workdir/next-input":             []byte(next.TaskID),
	} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(wire.WorkspaceRoot+"/.multica-runtime/context", 0700); err != nil {
		t.Fatal(err)
	}
	return f, next
}

func TestNativeHandoffWaitsForMonitorBeforeRecoveringStorage(t *testing.T) {
	for _, first := range []string{"monitor", "follow-up"} {
		t.Run(first, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, request := nativeHandoff(t)
				file := filepath.Join(wire.WorkspaceRoot, f.request.WorkerSubPath, "user-work")
				content := uuid.NewString()
				if err := os.WriteFile(file, []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
				deleting, allowDelete := make(chan struct{}), make(chan struct{})
				api := f.runner.resources.API.(*fake.Clientset)
				api.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, k8sruntime.Object, error) {
					close(deleting)
					<-allowDelete
					return false, nil, nil
				})
				done := make(chan Result, 1)
				if first == "monitor" {
					conn := connectPipeMonitor(t, f)
					conn.Close()
					<-deleting // monitor holds the real storage flock through deletion
					go func() { done <- f.runner.Run(t.Context(), request, kubernetes.Streams{}) }()
				} else {
					go func() { done <- f.runner.Run(t.Context(), request, kubernetes.Streams{}) }()
					<-deleting // B holds the real storage flock through recovery
					conn := connectPipeMonitor(t, f)
					conn.Close()
				}
				synctest.Wait()
				select {
				case result := <-done:
					close(allowDelete)
					t.Fatal("follow-up exited while the monitor still owned cleanup", result.ExecutionError)
				default:
				}
				if _, err := os.Stat(filepath.Join(wire.WorkspaceRoot, f.request.WorkerSubPath, "workdir/next-input")); !os.IsNotExist(err) {
					close(allowDelete)
					t.Fatal("follow-up changed task context while prior cleanup still held storage", err)
				}
				close(allowDelete)
				result := <-done
				// This bounded regression deliberately stops at the next preparation
				// boundary (no committed HOME configuration). It proves handoff/recovery, not provider
				// execution; the real-daemon harness proves the complete next task.
				if result.ExecutionError == nil || errors.Is(result.ExecutionError, workspace.ErrStorageBusy) {
					t.Fatal("follow-up did not reach its own preparation after cleanup", result.ExecutionError)
				}
				assertNativeNextInput(t, f, request)
				awaitMonitorCleanup(t, f)
				got, err := os.ReadFile(file)
				if err != nil || string(got) != content {
					t.Fatal("storage handoff damaged prior user work", err)
				}
				pods, err := api.CoreV1().Pods(f.attempt.Ref.Namespace).List(context.Background(), metav1.ListOptions{})
				if err != nil {
					t.Fatal(err)
				}
				for _, pod := range pods.Items {
					if pod.Name != f.attempt.Ref.Owner.Name {
						t.Fatal("failed preparation left a new storage consumer")
					}
				}
			})
		})
	}
}

func assertNativeNextInput(t *testing.T, f monitorFixture, request wire.Request) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(wire.WorkspaceRoot, f.request.WorkerSubPath, "workdir/next-input"))
	if err != nil || string(data) != request.TaskID {
		t.Fatal("follow-up failed to publish its own task context after recovery", err)
	}
}

// Combine OS process death/socket EOF with the waiting Run path. The synctest
// cases above control both acquisition orders without relying on wall time.
func TestNativeHandoffAfterActualShimSIGKILL(t *testing.T) {
	f, request := nativeHandoff(t)
	socketDir, err := os.MkdirTemp("/tmp", "handoff-monitor-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "monitor.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	serveTestMonitor(t, f.runner, listener)
	deleting, allowDelete := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	releaseCleanup := func() { unblock.Do(func() { close(allowDelete) }) }
	t.Cleanup(releaseCleanup)
	f.runner.resources.API.(*fake.Clientset).PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, k8sruntime.Object, error) {
		close(deleting)
		<-allowDelete
		return false, nil, nil
	})
	input, _ := json.Marshal(monitorChildInput{Options: f.options, Storage: f.attempt.Ref.StorageID, Attempt: f.attempt.Ref.AttemptID, Socket: socket})
	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyRead.Close()
	var output bytes.Buffer
	child := exec.Command(os.Args[0], "-test.run=^TestAttemptMonitorChildProcess$")
	child.Env = append(os.Environ(), "MULTICA_MONITOR_TEST_CHILD="+string(input))
	child.ExtraFiles = []*os.File{readyWrite}
	child.Stdout, child.Stderr = &output, &output
	if err := child.Start(); err != nil {
		readyWrite.Close()
		t.Fatal(err)
	}
	readyWrite.Close()
	childDone := make(chan struct{})
	go func() { _ = child.Wait(); close(childDone) }()
	t.Cleanup(func() { _ = child.Process.Kill(); <-childDone })
	if _, err := io.ReadFull(readyRead, make([]byte, 1)); err != nil {
		<-childDone
		t.Fatal("child did not acquire its storage/monitor authority", err, output.String())
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-childDone
	<-deleting // actual socket EOF has now made the monitor own S
	done := make(chan Result, 1)
	go func() { done <- f.runner.Run(t.Context(), request, kubernetes.Streams{}) }()
	// Release the controlled API deletion only after B has durably bound the
	// same storage. A short injected deletion lag exercises a real wait; no
	// elapsed-time/SLO assertion is used as the functional success oracle.
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		claim, err := f.runner.store.Lookup(request.TaskID, wire.Value(request.Env, "MULTICA_TOKEN"), wire.Value(request.Env, "MULTICA_WORKSPACE_ID"), wire.Value(request.Env, "MULTICA_AGENT_ID"))
		if err != nil {
			t.Fatal(err)
		}
		if claim.BoundRoot != "" {
			break
		}
		select {
		case result := <-done:
			t.Fatal("follow-up exited before binding", result.ExecutionError)
		case <-poll.C:
		}
	}
	lag := time.AfterFunc(time.Second, releaseCleanup)
	defer lag.Stop()
	<-done
	assertNativeNextInput(t, f, request)
	awaitMonitorCleanup(t, f)
}

func TestNativeHandoffCancelledWaitCannotTouchLiveWorker(t *testing.T) {
	for _, mode := range []string{"cancel", "cancel-and-release", "budget", "caller-deadline"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, request := nativeHandoff(t)
				holder, err := f.runner.store.AcquireLease(f.attempt.Ref.StorageID)
				if err != nil {
					t.Fatal(err)
				}
				defer holder()
				home := filepath.Join(wire.WorkspaceRoot, f.request.WorkerSubPath, taskHomeArtifacts)
				if err := os.Mkdir(home, 0700); err != nil {
					t.Fatal(err)
				}
				file := filepath.Join(home, "live-home-input")
				content := uuid.NewString()
				if err := os.WriteFile(file, []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if mode == "caller-deadline" {
					var stop context.CancelFunc
					ctx, stop = context.WithDeadline(ctx, time.Now().Add(time.Nanosecond))
					defer stop()
				}
				done := make(chan Result, 1)
				go func() { done <- f.runner.Run(ctx, request, kubernetes.Streams{}) }()
				synctest.Wait()
				if mode == "cancel" || mode == "cancel-and-release" {
					cancel()
				}
				if mode == "cancel-and-release" {
					holder()
				}
				result := <-done
				want := context.Canceled
				if mode == "budget" || mode == "caller-deadline" {
					want = context.DeadlineExceeded
				}
				if !errors.Is(result.ExecutionError, want) {
					t.Fatal("expired execution continued into preparation", result.ExecutionError)
				}
				assertNativeWorkerPreserved(t, f, file, content)
			})
		})
	}
}

func TestNativeHandoffRechecksAuthorityAfterWaiting(t *testing.T) {
	for _, change := range []string{"token", "grant", "root"} {
		t.Run(change, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, request := nativeHandoff(t)
				holder, err := f.runner.store.AcquireLease(f.attempt.Ref.StorageID)
				if err != nil {
					t.Fatal(err)
				}
				defer holder()
				file := filepath.Join(wire.WorkspaceRoot, f.request.WorkerSubPath, "user-work")
				content := uuid.NewString()
				if err := os.WriteFile(file, []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
				done := make(chan Result, 1)
				go func() { done <- f.runner.Run(t.Context(), request, kubernetes.Streams{}) }()
				synctest.Wait()
				claim, err := f.runner.store.Lookup(request.TaskID, wire.Value(request.Env, "MULTICA_TOKEN"), wire.Value(request.Env, "MULTICA_WORKSPACE_ID"), wire.Value(request.Env, "MULTICA_AGENT_ID"))
				if err != nil {
					t.Fatal(err)
				}
				if change == "root" {
					newRoot := filepath.Join(wire.WorkspaceRoot, "retry", request.TaskID)
					if err := os.MkdirAll(filepath.Join(newRoot, "workdir"), 0700); err != nil {
						t.Fatal(err)
					}
					owner, _ := json.Marshal(map[string]string{"workspace_id": claim.WorkspaceID, "task_id": claim.ID})
					if err := os.WriteFile(filepath.Join(newRoot, ".task_owner"), owner, 0600); err != nil {
						t.Fatal(err)
					}
					_, _, err = f.runner.store.AuthorizeAndBind(claim.ID, wire.Value(request.Env, "MULTICA_TOKEN"), claim.WorkspaceID, claim.AgentID, newRoot, "", *claim.RuntimeRef)
				} else {
					updated := workspace.Observation{ID: claim.ID, WorkspaceID: claim.WorkspaceID, AgentID: claim.AgentID, AuthToken: "mat_" + uuid.NewString(), RuntimeRef: *claim.RuntimeRef, PriorWorkDir: claim.PriorWorkDir}
					if change == "grant" {
						updated.RepositoryURLs = []string{"https://example.invalid/new-grant.git"}
					}
					_, err = f.runner.store.ObserveBatch([]workspace.Observation{updated})
				}
				if err != nil && change != "grant" {
					t.Fatal(err)
				}
				registry := filepath.Join(f.options.Directory, "registry.json")
				before, err := os.ReadFile(registry)
				if err != nil {
					t.Fatal(err)
				}
				holder()
				result := <-done
				if result.ExecutionError == nil {
					t.Fatal("stale waiting execution obtained authority")
				}
				current, err := os.ReadFile(registry)
				if err != nil || string(current) != string(before) {
					t.Fatal("stale waiting execution rewrote current authority", err)
				}
				assertNativeWorkerPreserved(t, f, file, content)
			})
		})
	}
}

func assertNativeWorkerPreserved(t *testing.T, f monitorFixture, file, content string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(wire.WorkspaceRoot, f.request.WorkerSubPath, "workdir/next-input")); !os.IsNotExist(err) {
		t.Fatal("rejected execution published new task context", err)
	}
	got, err := os.ReadFile(file)
	if err != nil || string(got) != content {
		t.Fatal("rejected execution changed the live worker's private files", err)
	}
	ref := f.attempt.Ref
	pod, err := f.runner.resources.API.CoreV1().Pods(ref.Namespace).Get(t.Context(), ref.PodName, metav1.GetOptions{})
	if err != nil || string(pod.UID) != ref.PodUID {
		t.Fatal("rejected execution removed or replaced a live worker", err)
	}
	secret, err := f.runner.resources.API.CoreV1().Secrets(ref.Namespace).Get(t.Context(), ref.SecretName, metav1.GetOptions{})
	if err != nil || string(secret.UID) != ref.SecretUID {
		t.Fatal("rejected execution removed or replaced live credentials", err)
	}
}
