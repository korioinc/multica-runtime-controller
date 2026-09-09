package execution

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

func journalAttempt(t *testing.T) (*journal, *attempt) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owner := uuid.NewString()
	j, err := openJournal(root, owner)
	if err != nil {
		t.Fatal(err)
	}
	sha := core.Digest([]byte("installed code"))
	c := core.Contract{SchemaVersion: 2, ControllerABI: 2, BuildID: sha, Platform: "linux/amd64", RuntimePath: core.Root + "/runtime", RuntimeSHA256: sha, GoVersion: "go1.26.1", ShimPaths: map[string]string{}}
	for _, alias := range []string{"pi", "codex", "copilot", "agy"} {
		c.ShimPaths[alias] = core.Root + "/shims/" + alias
	}
	g := configuration.Group{Name: "provider", Directories: []string{".fixture"}, Files: []configuration.File{}}
	runtime := runtimeimage.Ref{SchemaVersion: 2, Image: "registry.example/runtime@sha256:" + sha, Platform: c.Platform, ImageBuildID: uuid.NewString(), DescriptorDigest: sha, Controller: c, Daemon: runtimeimage.Daemon{Executable: runtimeimage.Executable{Path: "/opt/tools/multica", Version: "0.4.40", SHA256: sha}, AdapterContract: runtimeimage.AdapterContract}, Providers: map[string]runtimeimage.Executable{"pi": {Path: "/opt/tools/pi", Version: "1.0.0", SHA256: sha}}, ConfigurationDigest: configuration.Digest([]configuration.Group{g})}
	storage, task, id := uuid.NewString(), uuid.NewString(), uuid.NewString()
	a := &attempt{SchemaVersion: attemptSchemaVersion, OwnerID: owner, Created: time.Now().UTC(), Ref: kubernetes.Reference{Namespace: "fixture", Owner: kubernetes.Owner{Name: "controller", UID: uuid.NewString()}, TaskID: task, StorageID: storage, AttemptID: id, PodName: "task-worker-" + storage, SecretName: "task-request-" + id, PodDigest: sha, RequestDigest: sha, RuntimeRef: runtime}}
	return j, a
}

func TestAttemptReopenPreservesExecutionAuthority(t *testing.T) {
	j, a := journalAttempt(t)
	if err := j.save(a); err != nil {
		t.Fatal(err)
	}
	reopened, err := openJournal(j.directory, j.owner)
	if err != nil {
		t.Fatal("controller could not reopen its own durable attempts", err)
	}
	recovered, err := reopened.read(a.Ref.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered.Ref.RuntimeRef.Equal(a.Ref.RuntimeRef) {
		t.Fatal("recovery changed selected execution authority")
	}
	if err := reopened.remove(recovered); err != nil {
		t.Fatal("completed attempt became permanently uncollectable", err)
	}
}

func TestRejectedJournalWritePreservesExistingRecoveryState(t *testing.T) {
	j, a := journalAttempt(t)
	if err := j.save(a); err != nil {
		t.Fatal(err)
	}
	foreign := *a
	foreign.OwnerID = uuid.NewString()
	if err := j.save(&foreign); err == nil {
		t.Fatal("foreign attempt replaced durable recovery state")
	}
	got, err := j.read(a.Ref.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Ref.TaskID != a.Ref.TaskID || !got.Ref.RuntimeRef.Equal(a.Ref.RuntimeRef) {
		t.Fatal("failed write damaged the prior task's recovery authority")
	}
}
