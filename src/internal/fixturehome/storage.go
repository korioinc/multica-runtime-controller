// Package fixturehome prepares captured HOME artifacts for the disposable
// Kubernetes verifiers. It never composes or installs a provider HOME.
package fixturehome

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/korioinc/multica-runtime-controller/internal/execution"
	runtimekube "github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

const workspaceRoot = "/verification-workspace"

type Fixture struct {
	root      string
	sample    wire.Request
	selection execution.Selection
	client    *runtimekube.Client
	release   func()
}

func Open(ctx context.Context, api clientset.Interface, selection execution.Selection, sample wire.Request) (*Fixture, error) {
	if os.Getenv("LOCALVERIFY_DISPOSABLE_CLUSTER") != "true" || selection.Namespace != "runtime-verify" || sample.OwnerID != selection.OwnerID || !sample.RuntimeRef.Equal(selection.RuntimeRef) {
		return nil, errors.New("HOME fixture requires the current disposable task selection")
	}
	if raw, err := json.Marshal(sample); err != nil {
		return nil, err
	} else if _, err := wire.Decode(raw); err != nil {
		return nil, err
	}
	pvc, err := api.CoreV1().PersistentVolumeClaims(selection.Namespace).Get(ctx, selection.Worker.WorkspaceClaim, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if pvc.Spec.VolumeName == "" || pvc.UID == "" {
		return nil, errors.New("fixture workspace PVC is unbound")
	}
	pv, err := api.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if pv.Spec.HostPath == nil || pv.Spec.HostPath.Path != workspaceRoot || pv.Spec.HostPath.Type == nil || *pv.Spec.HostPath.Type != corev1.HostPathDirectory || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID || pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.Name != pvc.Name {
		return nil, errors.New("fixture requires the exact disposable hostPath workspace binding")
	}
	root := filepath.Join(workspaceRoot, sample.WorkerSubPath)
	if err := realDirectory(root); err != nil {
		return nil, err
	}
	if err := realDirectory(filepath.Join(workspaceRoot, ".multica-runtime/state")); err != nil {
		return nil, err
	}
	path := filepath.Join(workspaceRoot, ".multica-runtime/state", "worker-"+filepath.Base(sample.WorkerSubPath)+".lock")
	file, err := openRegular(path, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	fd := int(file.Fd())
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		file.Close()
		return nil, errors.New("sample worker storage is still leased")
	}
	var once sync.Once
	release := func() { once.Do(func() { _ = unix.Flock(fd, unix.LOCK_UN); _ = file.Close() }) }
	f := &Fixture{root: root, sample: sample, selection: selection, client: &runtimekube.Client{API: api, Namespace: selection.Namespace}, release: release}
	if err := f.available(ctx); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (f *Fixture) Root() string { return f.root }

func (f *Fixture) Close() {
	if f.release != nil {
		f.release()
		f.release = nil
	}
}

func (f *Fixture) available(ctx context.Context) error {
	if f.release == nil {
		return errors.New("HOME fixture storage lease is no longer held")
	}
	if err := f.authority(); err != nil {
		return err
	}
	// This is a fixture mutation precondition, not an oracle for a tested
	// authorization decision. Include init and ephemeral volume consumers.
	return f.client.StorageAvailable(ctx, f.selection.Worker.WorkspaceClaim, filepath.Base(f.sample.WorkerSubPath), f.selection.Controller)
}

func (f *Fixture) authority() error {
	file, err := openRegular(filepath.Join(workspaceRoot, ".multica-runtime/state/registry.json"), os.O_RDONLY)
	if err != nil {
		return err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (64<<20)+1))
	if err != nil {
		return err
	}
	var registry struct {
		SchemaVersion int    `json:"schemaVersion"`
		OwnerID       string `json:"ownerID"`
		WorkspaceRoot string `json:"workspaceRoot"`
		Claims        map[string]struct {
			ID, WorkspaceID, AgentID string
			TokenHash                string            `json:"credentialFingerprint"`
			WorkerSubPath            string            `json:"workerSubPath"`
			BoundRoot                string            `json:"boundRoot"`
			Denied                   bool              `json:"denied"`
			ExecutionState           string            `json:"executionState"`
			RuntimeRef               *runtimeimage.Ref `json:"runtimeRef"`
		} `json:"claims"`
		Retired map[string]string `json:"retired"`
	}
	if len(raw) > 64<<20 || json.Unmarshal(raw, &registry) != nil || registry.SchemaVersion != 2 || registry.OwnerID != f.selection.OwnerID || registry.WorkspaceRoot != wire.WorkspaceRoot {
		return errors.New("fixture workspace owner registry mismatch")
	}
	claim, ok := registry.Claims[f.sample.TaskID]
	if !ok || claim.ID != f.sample.TaskID || claim.Denied || claim.ExecutionState != "observed" || claim.WorkerSubPath != f.sample.WorkerSubPath || claim.BoundRoot != filepath.Dir(f.sample.WorkDir) || claim.RuntimeRef == nil || !claim.RuntimeRef.Equal(f.sample.RuntimeRef) || claim.WorkspaceID != wire.Value(f.sample.Env, "MULTICA_WORKSPACE_ID") || claim.AgentID != wire.Value(f.sample.Env, "MULTICA_AGENT_ID") || claim.TokenHash != wire.Digest([]byte(wire.Value(f.sample.Env, "MULTICA_TOKEN"))) || registry.Retired[f.sample.WorkerSubPath] != "" {
		return errors.New("sample request has no matching durable workspace authority")
	}
	return nil
}

func realDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || !info.IsDir() || canonical != path {
		return errors.New("HOME fixture path is not a canonical directory")
	}
	return nil
}

func openRegular(path string, flags int) (*os.File, error) {
	file, err := os.OpenFile(path, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return nil, errors.New("HOME fixture input is not a regular existing file")
	}
	return file, nil
}
