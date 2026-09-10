package execution

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func TestHomeCleanupRemovesOnlyTheFinishedAttemptsInputs(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storage, finished, pending := uuid.NewString(), uuid.NewString(), uuid.NewString()
	worker := filepath.Join(root, workspace.StoragePrefix, storage)
	completed := filepath.Join(worker, taskHomeArtifacts, finished+".tar")
	other := filepath.Join(worker, taskHomeArtifacts, pending+".tar")
	work := filepath.Join(worker, "workdir/unfinished")
	configFile(t, completed, "finished attempt credentials")
	configFile(t, other, "pending attempt inputs")
	configFile(t, work, "private work")
	for range 2 {
		if err := removeTaskHomeArchive(root, storage, finished); err != nil {
			t.Fatal("completed attempt inputs could not be reclaimed", err)
		}
	}
	if _, err := os.ReadFile(completed); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("finished credentials remain in worker storage", err)
	}
	for path, expected := range map[string]string{other: "pending attempt inputs", work: "private work"} {
		got, err := os.ReadFile(path)
		if err != nil || string(got) != expected {
			t.Fatal("cleanup changed data outside the finished attempt", err)
		}
	}
}

func TestHomeCleanupDoesNotFollowAnArchiveDirectoryLink(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storage, attempt := uuid.NewString(), uuid.NewString()
	worker := filepath.Join(root, workspace.StoragePrefix, storage)
	outside := t.TempDir()
	protected := filepath.Join(outside, attempt+".tar")
	configFile(t, protected, "unrelated private data")
	if err := os.MkdirAll(worker, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(worker, taskHomeArtifacts)); err != nil {
		t.Fatal(err)
	}
	if err := removeTaskHomeArchive(root, storage, attempt); err == nil {
		t.Fatal("cleanup followed an ungranted archive directory")
	}
	got, err := os.ReadFile(protected)
	if err != nil || string(got) != "unrelated private data" {
		t.Fatal("cleanup removed another owner's data", err)
	}
}
