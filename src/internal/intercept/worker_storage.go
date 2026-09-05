package intercept

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/taskstate"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"time"
)

func (l *Launcher) prepareTaskStorage(ctx context.Context, taskID string, request *Request) (func(), error) {
	root, _, err := taskStorageRoot(*request)
	if err != nil {
		return nil, err
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root || request.WorkDir != filepath.Join(root, "workdir") {
		return nil, errors.New("only canonical daemon-managed work directories are supported")
	}
	store, err := taskstate.New(taskstate.DefaultDirectory)
	if err != nil {
		return nil, err
	}
	claim, err := store.Lookup(taskID, requestEnvironmentValue(request.Env, "MULTICA_TOKEN"), requestEnvironmentValue(request.Env, "MULTICA_WORKSPACE_ID"), requestEnvironmentValue(request.Env, "MULTICA_AGENT_ID"))
	if err != nil {
		return nil, fmt.Errorf("validate official task claim: %w", err)
	}
	request.RepositoryURLs = claim.RepositoryURLs
	piSession, err := taskPiSession(*request)
	if err != nil {
		return nil, err
	}
	binding, err := store.Bind(claim, root, piSession)
	if err != nil {
		return nil, err
	}
	request.WorkerSubPath = binding.WorkerSubPath
	storageID := filepath.Base(binding.WorkerSubPath)
	lease, err := os.OpenFile(filepath.Join(taskstate.DefaultDirectory, "worker-"+storageID+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	release := func() { _ = unix.Flock(int(lease.Fd()), unix.LOCK_UN); _ = lease.Close() }
	if err := unix.Flock(int(lease.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lease.Close()
		return nil, errors.New("another execution holds this worker storage")
	}
	succeeded := false
	defer func() {
		if !succeeded {
			release()
		}
	}()
	// The file lock covers live shims. This API check also covers a worker
	// whose shim died and consequently released its file lock before cleanup.
	pods, err := l.client.CoreV1().Pods(l.config.Namespace).List(ctx, metav1.ListOptions{LabelSelector: StorageIDLabel + "=" + storageID})
	if err != nil {
		return nil, err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return nil, errors.New("a previous task Pod still holds this worker storage; wait for its termination")
		}
		if pod.Name == "task-worker-"+storageID {
			if pod.Labels[ManagedByLabel] != ManagedByValue || pod.UID == "" {
				return nil, errors.New("task Pod name is occupied by an unknown object")
			}
			uid := pod.UID
			if err := l.client.CoreV1().Pods(l.config.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
				return nil, err
			}
			if err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
				current, err := l.client.CoreV1().Pods(l.config.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				if err != nil {
					return false, err
				}
				if current.UID != uid {
					return false, errors.New("another task Pod replaced the terminal execution")
				}
				return false, nil
			}); err != nil {
				return nil, err
			}
		}
	}
	if l.config.BackendURL == "" {
		return nil, errors.New("original backend URL is required for worker API requests")
	}
	for i, entry := range request.Env {
		if strings.HasPrefix(entry, "MULTICA_SERVER_URL=") {
			request.Env[i] = "MULTICA_SERVER_URL=" + l.config.BackendURL
		}
	}
	workerRoot := filepath.Join(workspaceRoot, binding.WorkerSubPath)
	if err := seedWorkerContext(root, workerRoot, filepath.Join(taskstate.DefaultDirectory, "seed-"+storageID+".json"), request.Provider); err != nil {
		return nil, err
	}
	succeeded = true
	return release, nil
}

// Only the official current sidecar manifest and runtime brief are copied.
// Checkout staging, previous repositories and controller session stores are
// never traversed here. All writes replace files inside the worker root.
func seedWorkerContext(source, destination, manifestPath, provider string) error {
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(destination)
	if err != nil || canonical != destination {
		return errors.New("worker storage is not a canonical directory")
	}
	worker, err := os.OpenRoot(destination)
	if err != nil {
		return err
	}
	defer worker.Close()
	prepared, err := os.OpenRoot(source)
	if err != nil {
		return err
	}
	defer prepared.Close()
	var previous []string
	if raw, err := os.ReadFile(manifestPath); err == nil {
		if err := json.Unmarshal(raw, &previous); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, name := range previous {
		if !filepath.IsLocal(name) || !strings.HasPrefix(name, "workdir/") || name == "workdir/AGENTS.md" {
			return errors.New("invalid prior context manifest")
		}
	}
	for _, name := range []string{"workdir", "output", "logs", "multica-config", "codex-skills"} {
		if err := plainParents(worker, name+"/placeholder"); err != nil {
			return err
		}
	}
	var manifest struct {
		Files []string `json:"files"`
	}
	raw, err := prepared.ReadFile(".multica_sidecar_manifest.json")
	if err != nil || json.Unmarshal(raw, &manifest) != nil {
		return errors.New("official task context manifest is unavailable")
	}
	files := make(map[string][]byte)
	var copied []string
	for _, filename := range manifest.Files {
		rel, err := filepath.Rel(source, filename)
		if err != nil || !filepath.IsLocal(rel) || !strings.HasPrefix(rel, "workdir/") {
			return errors.New("official task context escaped its work directory")
		}
		if rel == "workdir/AGENTS.md" {
			continue
		}
		data, err := readPlainFile(prepared, rel)
		if err != nil {
			return err
		}
		if _, err := worker.Lstat(rel); err == nil && !slices.Contains(previous, rel) {
			return fmt.Errorf("current context conflicts with a worker-owned file at %s; preserve or move it and retry", rel)
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		files[rel] = data
		copied = append(copied, rel)
	}
	brief, err := readPlainFile(prepared, "workdir/AGENTS.md")
	if err != nil {
		return fmt.Errorf("read current official runtime brief: %w", err)
	}
	var existing []byte
	if info, err := worker.Lstat("workdir/AGENTS.md"); err == nil {
		if !info.Mode().IsRegular() {
			return errors.New("worker runtime brief is not a regular file")
		}
		existing, err = worker.ReadFile("workdir/AGENTS.md")
		if err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// A durable union records ownership before any refresh writes. A crash
	// midway through seeding can therefore clean both old and new sidecars.
	pending := append(slices.Clone(previous), copied...)
	slices.Sort(pending)
	pending = slices.Compact(pending)
	if err := publishSeedManifest(manifestPath, pending); err != nil {
		return err
	}
	for _, name := range pending {
		if err := plainParents(worker, name); err != nil {
			return err
		}
		if err := worker.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for name, data := range files {
		if err := replaceWorkerFile(worker, name, data); err != nil {
			return err
		}
	}
	if err := replaceWorkerFile(worker, "workdir/AGENTS.md", refreshBrief(existing, brief)); err != nil {
		return err
	}
	if provider == "codex" {
		if err := worker.RemoveAll("codex-skills"); err != nil {
			return err
		}
		if err := worker.Mkdir("codex-skills", 0o700); err != nil {
			return err
		}
		const skills = "codex-home/skills"
		if _, err := prepared.Lstat(skills); err == nil {
			err := fs.WalkDir(prepared.FS(), skills, func(name string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() {
					return nil
				}
				data, err := readPlainFile(prepared, name)
				if err != nil {
					return err
				}
				return replaceWorkerFile(worker, "codex-skills/"+strings.TrimPrefix(name, skills+"/"), data)
			})
			if err != nil {
				return err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return publishSeedManifest(manifestPath, copied)
}

func publishSeedManifest(manifestPath string, files []string) error {
	raw, err := json.Marshal(files)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(manifestPath), ".seed-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(raw)
	syncErr := file.Sync()
	if err := errors.Join(writeErr, syncErr, file.Close()); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), manifestPath); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(manifestPath))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func plainParents(root *os.Root, name string) error {
	if !filepath.IsLocal(name) {
		return errors.New("context path is not local")
	}
	current := ""
	for _, part := range strings.Split(filepath.Dir(name), string(filepath.Separator)) {
		if part == "." {
			continue
		}
		current = filepath.Join(current, part)
		if err := root.Mkdir(current, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err := root.Lstat(current)
		if err != nil || !info.IsDir() {
			return errors.New("context parent is not a plain directory")
		}
	}
	return nil
}

func readPlainFile(root *os.Root, name string) ([]byte, error) {
	current := ""
	for _, part := range strings.Split(name, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := root.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("context file contains a symlink or is missing")
		}
		if current == name && !info.Mode().IsRegular() {
			return nil, errors.New("context is not a regular file")
		}
	}
	return root.ReadFile(name)
}

func replaceWorkerFile(root *os.Root, name string, data []byte) error {
	if err := plainParents(root, name); err != nil {
		return err
	}
	temporary := filepath.Join(filepath.Dir(name), ".context-"+uuid.NewString())
	file, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer root.Remove(temporary)
	_, writeErr := file.Write(data)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return err
	}
	return root.Rename(temporary, name)
}

func refreshBrief(existing, prepared []byte) []byte {
	const begin = "<!-- BEGIN MULTICA-RUNTIME (auto-managed; do not edit) -->"
	const end = "<!-- END MULTICA-RUNTIME -->"
	old := string(existing)
	start := strings.Index(old, begin)
	if start >= 0 {
		finish := strings.Index(old[start+len(begin):], end)
		if finish >= 0 {
			finish += start + len(begin) + len(end)
			return []byte(old[:start] + strings.TrimRight(string(prepared), "\n") + old[finish:])
		}
	}
	if len(existing) == 0 {
		return prepared
	}
	return []byte(old + "\n\n" + string(prepared))
}
