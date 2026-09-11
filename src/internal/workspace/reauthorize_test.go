package workspace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestReauthorizationCannotRestoreChangedAuthority(t *testing.T) {
	for _, change := range []string{"token", "scope", "runtime", "root", "marker", "session"} {
		t.Run(change, func(t *testing.T) {
			store, options := testStore(t)
			task := testObservation(testEnvironment("a"))
			approve(t, store, task)
			root := prepareRoot(t, options.WorkspaceRoot, task)
			session := prepareSession(t, options.SessionRoot)
			_, binding, err := store.AuthorizeAndBind(task.ID, task.AuthToken, task.WorkspaceID, task.AgentID, root, session, task.RuntimeRef)
			if err != nil {
				t.Fatal(err)
			}
			work := filepath.Join(options.WorkspaceRoot, binding.WorkerSubPath, "unfinished-work")
			content := []byte(uuid.NewString())
			writeFile(t, work, content)
			switch change {
			case "token", "scope", "runtime":
				updated := task
				switch change {
				case "token":
					updated.AuthToken = "mat_" + uuid.NewString()
				case "scope":
					updated.RepositoryURLs = []string{"https://example.invalid/other.git"}
				case "runtime":
					updated.RuntimeRef = testEnvironment("b")
				}
				_, err := store.ObserveBatch([]Observation{updated})
				if err != nil && change != "scope" {
					t.Fatal(err)
				}
			case "root":
				newRoot := filepath.Join(options.WorkspaceRoot, "retry", task.ID)
				if err := os.MkdirAll(filepath.Join(newRoot, "workdir"), 0700); err != nil {
					t.Fatal(err)
				}
				owner, _ := json.Marshal(map[string]string{"workspace_id": task.WorkspaceID, "task_id": task.ID})
				writeFile(t, filepath.Join(newRoot, ".task_owner"), owner)
				if _, _, err := store.AuthorizeAndBind(task.ID, task.AuthToken, task.WorkspaceID, task.AgentID, newRoot, session, task.RuntimeRef); err != nil {
					t.Fatal(err)
				}
			case "marker":
				if err := os.Remove(filepath.Join(root, rootMarker)); err != nil {
					t.Fatal(err)
				}
			case "session":
				// A replacement session file has not acquired this task's grant.
				session = prepareSession(t, options.SessionRoot)
			}
			registry := filepath.Join(options.Directory, "registry.json")
			before, err := os.ReadFile(registry)
			if err != nil {
				t.Fatal(err)
			}
			release, err := store.AcquireLease(filepath.Base(binding.WorkerSubPath))
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if _, _, err := store.ReauthorizeBinding(task.ID, task.AuthToken, task.WorkspaceID, task.AgentID, root, session, binding.WorkerSubPath, task.RuntimeRef); err == nil {
				t.Fatal("stale task acquired changed execution authority")
			}
			assertData(t, registry, before)
			assertData(t, work, content)
			if change == "marker" {
				if _, err := os.Lstat(filepath.Join(root, rootMarker)); !os.IsNotExist(err) {
					t.Fatal("revalidation recreated revoked root authority", err)
				}
			}
		})
	}
}

func TestReauthorizationAcceptsPriorRootWithoutFreezingUnrelatedMetadata(t *testing.T) {
	store, options := testStore(t)
	first := testObservation(testEnvironment("a"))
	approve(t, store, first)
	root := prepareRoot(t, options.WorkspaceRoot, first)
	session := prepareSession(t, options.SessionRoot)
	_, binding, err := store.AuthorizeAndBind(first.ID, first.AuthToken, first.WorkspaceID, first.AgentID, root, session, first.RuntimeRef)
	if err != nil {
		t.Fatal(err)
	}
	next := testObservation(first.RuntimeRef)
	next.PriorWorkDir, next.PriorSession = filepath.Join(root, "workdir"), session
	approve(t, store, next)
	if _, _, err := store.AuthorizeAndBind(next.ID, next.AuthToken, next.WorkspaceID, next.AgentID, root, session, next.RuntimeRef); err != nil {
		t.Fatal(err)
	}
	// Another same-scope task can register its own session while this request
	// waits. That does not revoke the original live task/session grant.
	other := testObservation(first.RuntimeRef)
	other.PriorWorkDir = next.PriorWorkDir
	approve(t, store, other)
	otherSession := prepareSession(t, options.SessionRoot)
	if _, _, err := store.AuthorizeAndBind(other.ID, other.AuthToken, other.WorkspaceID, other.AgentID, root, otherSession, other.RuntimeRef); err != nil {
		t.Fatal(err)
	}
	writeFile(t, session, []byte("prior session's live work"))
	claim, current, err := store.ReauthorizeBinding(next.ID, next.AuthToken, next.WorkspaceID, next.AgentID, root, session, binding.WorkerSubPath, next.RuntimeRef)
	if err != nil || claim.ID != next.ID || current.WorkerSubPath != binding.WorkerSubPath {
		t.Fatal("legitimate follow-up lost its own authorized storage/session", err)
	}
}
