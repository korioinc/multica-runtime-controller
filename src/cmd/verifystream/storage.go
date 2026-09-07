package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"golang.org/x/sys/unix"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

type storageSnapshot struct {
	Files   map[string]string `json:"files"`
	Session string            `json:"session"`
}

func (s storageSnapshot) digest() string {
	raw, _ := json.Marshal(s)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func lockStorage(ctx context.Context, api clientset.Interface, selection execution.Selection, request wire.Request) (string, func(), error) {
	pvc, err := api.CoreV1().PersistentVolumeClaims(selection.Namespace).Get(ctx, selection.Worker.WorkspaceClaim, metav1.GetOptions{})
	if err != nil {
		return "", nil, err
	}
	if pvc.Spec.VolumeName == "" {
		return "", nil, errors.New("fixture workspace PVC is unbound")
	}
	pv, err := api.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return "", nil, err
	}
	if pv.Spec.HostPath == nil || pv.Spec.HostPath.Path != "/verification-workspace" || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID {
		return "", nil, errors.New("fixture requires the exact disposable hostPath workspace binding")
	}
	raw, err := os.ReadFile("/verification-workspace/.multica-runtime/state/registry.json")
	if err != nil {
		return "", nil, err
	}
	var registry struct {
		SchemaVersion int    `json:"schemaVersion"`
		OwnerID       string `json:"ownerID"`
		Claims        map[string]struct {
			TokenHash     string `json:"credentialFingerprint"`
			WorkerSubPath string `json:"workerSubPath"`
			Denied        bool   `json:"denied"`
		} `json:"claims"`
	}
	if json.Unmarshal(raw, &registry) != nil || registry.SchemaVersion != 1 || registry.OwnerID != selection.OwnerID {
		return "", nil, errors.New("fixture workspace owner registry mismatch")
	}
	claim, ok := registry.Claims[request.TaskID]
	if !ok || claim.Denied || claim.WorkerSubPath != request.WorkerSubPath || claim.TokenHash != wire.Digest([]byte(wire.Value(request.Env, "MULTICA_TOKEN"))) {
		return "", nil, errors.New("sample request has no matching durable workspace authority")
	}
	path := filepath.Join("/verification-workspace/.multica-runtime/state", "worker-"+filepath.Base(request.WorkerSubPath)+".lock")
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return "", nil, errors.New("fixture storage lease is not a regular existing lock")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		file.Close()
		return "", nil, errors.New("sample worker storage is still leased")
	}
	var once sync.Once
	release := func() { once.Do(func() { _ = unix.Flock(fd, unix.LOCK_UN); _ = file.Close() }) }
	return filepath.Join("/verification-workspace", request.WorkerSubPath), release, nil
}

func snapshotStorage(root, session string) (storageSnapshot, error) {
	snapshot := storageSnapshot{Files: map[string]string{}}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root {
		return snapshot, errors.New("bound worker storage must remain a canonical directory")
	}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			snapshot.Files[relative] = "symlink:" + target
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("unsupported file in the fixture storage snapshot")
		}
		digest, err := core.HashFile(path)
		if err != nil {
			return err
		}
		snapshot.Files[relative] = digest
		return nil
	})
	if err != nil {
		return snapshot, err
	}
	if !strings.HasPrefix(session, wire.PiSessionsRoot+"/") || filepath.Dir(session) != wire.PiSessionsRoot {
		return snapshot, errors.New("sample Pi session is unconfined")
	}
	digest, err := core.HashFile(filepath.Join("/verification-workspace/.multica-runtime/sessions", filepath.Base(session)))
	if err != nil {
		return snapshot, err
	}
	snapshot.Session = digest
	return snapshot, nil
}
func snapshotsEqual(a, b storageSnapshot) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}
