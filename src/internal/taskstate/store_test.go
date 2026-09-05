package taskstate_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/taskstate"
)

func TestAuthorizedContinuationAndRetryPreserveWork(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := newStore(t, workspace)
	first := uuid.NewString()
	root := preparedRoot(t, workspace, first)
	session := newSession(t, workspace)
	claim := observe(t, store, first, "repo-a", "", "")
	binding, err := store.Bind(claim, root, session)
	if err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(workspace, binding.WorkerSubPath, "uncommitted")
	if err := os.WriteFile(data, []byte("agent's unfinished work"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(session, []byte("conversation history"), 0o600); err != nil {
		t.Fatal(err)
	}
	continued := observe(t, store, uuid.NewString(), "repo-a", filepath.Join(root, "workdir"), session)
	resumed, err := store.Bind(continued, root, session)
	if err != nil {
		t.Fatal(err)
	}
	assertWork(t, filepath.Join(workspace, resumed.WorkerSubPath, "uncommitted"))
	// B may first reuse A, then be retried from B's own candidate root.
	// The worker binding belongs to the task, not only that earlier pathname.
	candidate := preparedRoot(t, workspace, continued.ID)
	retryB := observe(t, store, continued.ID, "repo-a", filepath.Join(root, "workdir"), session)
	rebound, err := store.Bind(retryB, candidate, session)
	if err != nil {
		t.Fatal(err)
	}
	assertWork(t, filepath.Join(workspace, rebound.WorkerSubPath, "uncommitted"))
	// Official same-task preparation removes its scratch root. Retry storage
	// must survive independently, including a new Store after process restart.
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	root = preparedRoot(t, workspace, first)
	store = newStore(t, workspace)
	retried := observe(t, store, first, "repo-a", "", "")
	reused, err := store.Bind(retried, root, "")
	if err != nil {
		t.Fatal(err)
	}
	assertWork(t, filepath.Join(workspace, reused.WorkerSubPath, "uncommitted"))
}

func TestAnotherRepositoryCannotAcquirePreviousStorageOrSession(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := newStore(t, workspace)
	a := uuid.NewString()
	rootA := preparedRoot(t, workspace, a)
	sessionA := newSession(t, workspace)
	claimA := observe(t, store, a, "repo-a", "", "")
	if _, err := store.Bind(claimA, rootA, sessionA); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sessionA, []byte("private repository A history"), 0o600); err != nil {
		t.Fatal(err)
	}
	b := uuid.NewString()
	claimB := observe(t, store, b, "repo-b", filepath.Join(rootA, "workdir"), sessionA)
	if _, err := store.Bind(claimB, rootA, sessionA); err == nil {
		t.Fatal("another repository acquired A's storage and session")
	}
	rootB := preparedRoot(t, workspace, b)
	if _, err := store.Bind(claimB, rootB, sessionA); err == nil {
		t.Fatal("another repository acquired A's session through its own root")
	}
	if _, err := store.Bind(claimB, rootB, newSession(t, workspace)); err != nil {
		t.Fatal(err)
	}
	// Even a valid B root cannot launder an unrelated nonempty A session.
	claimC := observe(t, store, uuid.NewString(), "repo-b", filepath.Join(rootB, "workdir"), sessionA)
	if _, err := store.Bind(claimC, rootB, sessionA); err == nil {
		t.Fatal("matching root scope bypassed the session's own provenance")
	}
}

func TestUnobservedOrChangedTaskCannotAcquireStorage(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := newStore(t, workspace)
	id := uuid.NewString()
	if _, err := store.Lookup(id, "mat_unseen", "workspace", "agent"); err == nil {
		t.Fatal("an unobserved claim was authorized")
	}
	observe(t, store, id, "repo-a", "", "")
	if _, err := store.Lookup(id, "mat_wrong", "workspace", "agent"); err == nil {
		t.Fatal("a stale task credential was authorized")
	}
	if _, err := store.Observe(claimBytes(t, id, "repo-b", "", "")); err == nil {
		t.Fatal("same task changed its repository scope")
	}
	if _, err := store.Lookup(id, "mat_"+id, "workspace", "agent"); err == nil {
		t.Fatal("denied scope change left the old claim authorized")
	}
	if _, err := store.Observe(claimBytes(t, id, "repo-a", "", "")); err == nil {
		t.Fatal("a denied task was reauthorized by replaying its original scope")
	}
}

func TestCollectionRemovesOnlyRetiredInactiveWorkerData(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := newStore(t, workspace)
	files := make(map[string]string)
	active := make(map[string]bool)
	for _, state := range []string{"retired", "existing preparation", "active Pod"} {
		id := uuid.NewString()
		root := preparedRoot(t, workspace, id)
		claim := observe(t, store, id, "repo", "", "")
		binding, err := store.Bind(claim, root, "")
		if err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(workspace, binding.WorkerSubPath, "uncommitted")
		if err := os.WriteFile(file, []byte("agent's unfinished work"), 0o600); err != nil {
			t.Fatal(err)
		}
		files[state] = file
		if state != "existing preparation" {
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
		}
		if state == "active Pod" {
			active[filepath.Base(binding.WorkerSubPath)] = true
		}
	}
	unknown := filepath.Join(workspace, ".runtime-workers", uuid.NewString(), "uncommitted")
	if err := os.MkdirAll(filepath.Dir(unknown), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unknown, []byte("agent's unfinished work"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A caller-supplied cutoff before these claims protects the pre-Prepare
	// gap even though the old official root is no longer present.
	if _, err := store.Collect(workspace, time.Time{}, active); err != nil {
		t.Fatal(err)
	}
	assertWork(t, files["retired"])
	if _, err := store.Collect(workspace, time.Now().Add(time.Hour), active); err != nil {
		t.Fatal(err)
	}
	if _, err := os.ReadFile(files["retired"]); !os.IsNotExist(err) {
		t.Fatal("retired worker data was not reclaimed")
	}
	assertWork(t, files["existing preparation"])
	assertWork(t, files["active Pod"])
	assertWork(t, unknown)
}

func newStore(t *testing.T, workspace string) *taskstate.Store {
	t.Helper()
	store, err := taskstate.New(filepath.Join(workspace, ".state", "records"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func claimBytes(t *testing.T, id, repo, prior, session string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"id": id, "workspace_id": "workspace", "agent_id": "agent", "issue_id": "issue", "auth_token": "mat_" + id, "repos": []map[string]string{{"url": repo}}, "prior_work_dir": prior, "prior_session_id": session})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func observe(t *testing.T, store *taskstate.Store, id, repo, prior, session string) taskstate.Claim {
	t.Helper()
	if _, err := store.Observe(claimBytes(t, id, repo, prior, session)); err != nil {
		t.Fatal(err)
	}
	claim, err := store.Lookup(id, "mat_"+id, "workspace", "agent")
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

func preparedRoot(t *testing.T, workspace, id string) string {
	t.Helper()
	root := filepath.Join(workspace, "team", id)
	if err := os.MkdirAll(filepath.Join(root, "workdir"), 0o700); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]string{"workspace_id": "workspace", "task_id": id})
	if err := os.WriteFile(filepath.Join(root, ".task_owner"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

func newSession(t *testing.T, workspace string) string {
	t.Helper()
	file, err := os.CreateTemp(workspace, "session-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	return file.Name()
}

func assertWork(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) != "agent's unfinished work" {
		t.Fatal("authorized continuation lost unfinished work")
	}
}
