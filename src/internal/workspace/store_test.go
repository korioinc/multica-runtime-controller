package workspace

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

func TestEnvironmentChangePreservesWorkButCannotResumeOldSession(t *testing.T) {
	store, options := testStore(t)
	a, b := testEnvironment("a"), testEnvironment("b")
	first := testObservation(a)
	claim := approve(t, store, first)
	root := prepareRoot(t, options.WorkspaceRoot, first)
	session := prepareSession(t, options.SessionRoot)
	binding, err := store.Bind(claim, root, session, a)
	if err != nil {
		t.Fatal(err)
	}
	edits := filepath.Join(options.WorkspaceRoot, binding.WorkerSubPath, "uncommitted")
	writeFile(t, edits, []byte("unfinished repository work"))
	writeFile(t, session, []byte("prior private session history"))
	same := testObservation(a)
	same.PriorWorkDir = filepath.Join(root, "workdir")
	same.PriorSession = session
	if _, err := store.Bind(approve(t, store, same), root, session, a); err != nil {
		t.Fatalf("authorized same-environment continuation: %v", err)
	}
	store, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	next := testObservation(b)
	next.PriorWorkDir = filepath.Join(root, "workdir")
	next.PriorSession = session
	nextClaim := approve(t, store, next)
	if _, err := store.Bind(nextClaim, root, session, b); err == nil {
		t.Fatal("new environment acquired old provider session")
	}
	fresh := prepareSession(t, options.SessionRoot)
	continued, err := store.Bind(nextClaim, root, fresh, b)
	if err != nil {
		t.Fatal(err)
	}
	assertData(t, filepath.Join(options.WorkspaceRoot, continued.WorkerSubPath, "uncommitted"), []byte("unfinished repository work"))
	assertData(t, session, []byte("prior private session history"))
	// An official same-TaskID retry can reset its preparation root while worker
	// edits and their authorization remain in the installation registry.
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	retryRoot := prepareRoot(t, options.WorkspaceRoot, next)
	retried, err := store.Bind(approve(t, store, next), retryRoot, fresh, b)
	if err != nil {
		t.Fatal(err)
	}
	assertData(t, filepath.Join(options.WorkspaceRoot, retried.WorkerSubPath, "uncommitted"), []byte("unfinished repository work"))
}

func TestScopeAndCredentialCannotAuthorizeAnotherStorage(t *testing.T) {
	store, options := testStore(t)
	ref := testEnvironment("a")
	first := testObservation(ref)
	if _, err := store.Lookup(first.ID, first.AuthToken, first.WorkspaceID, first.AgentID); err == nil {
		t.Fatal("unobserved task authorized")
	}
	claim := approve(t, store, first)
	root := prepareRoot(t, options.WorkspaceRoot, first)
	session := prepareSession(t, options.SessionRoot)
	if _, err := store.Bind(claim, root, session, ref); err != nil {
		t.Fatal(err)
	}
	writeFile(t, session, []byte("repository A history"))
	if _, err := store.Lookup(first.ID, "mat_wrong", first.WorkspaceID, first.AgentID); err == nil {
		t.Fatal("wrong credential authorized")
	}
	other := testObservation(ref)
	other.RepositoryURLs = []string{"https://example.invalid/private-other.git"}
	other.PriorWorkDir = filepath.Join(root, "workdir")
	other.PriorSession = session
	otherClaim := approve(t, store, other)
	if _, err := store.Bind(otherClaim, root, session, ref); err == nil {
		t.Fatal("another repository acquired prior root")
	}
	otherRoot := prepareRoot(t, options.WorkspaceRoot, other)
	if _, err := store.Bind(otherClaim, otherRoot, session, ref); err == nil {
		t.Fatal("another repository acquired prior session through a new root")
	}
	changed := first
	changed.RepositoryURLs = other.RepositoryURLs
	if _, err := store.ObserveBatch([]Observation{changed}); err == nil {
		t.Fatal("same TaskID changed repository authority")
	}
	if _, err := store.Lookup(first.ID, first.AuthToken, first.WorkspaceID, first.AgentID); err == nil {
		t.Fatal("revoked claim remained authorized")
	}
	if _, err := store.ObserveBatch([]Observation{first}); err == nil {
		t.Fatal("replaying old scope restored revoked authorization")
	}
	assertData(t, session, []byte("repository A history"))
}

func TestInvalidBatchAndLocalDirectoryGrantNoAuthority(t *testing.T) {
	store, _ := testStore(t)
	ref := testEnvironment("a")
	first := testObservation(ref)
	local := testObservation(ref)
	local.LocalDirectory = "/home/operator/project"
	if _, err := store.ObserveBatch([]Observation{first, local}); err == nil {
		t.Fatal("local-directory claim accepted")
	}
	if _, err := store.Lookup(first.ID, first.AuthToken, first.WorkspaceID, first.AgentID); err == nil {
		t.Fatal("partial failed batch authorized an earlier task")
	}
	if _, err := store.Lookup(local.ID, local.AuthToken, local.WorkspaceID, local.AgentID); err == nil {
		t.Fatal("local-directory task authorized")
	}
}

func TestOwnerMismatchAndCorruptionPreserveAuthorityBytes(t *testing.T) {
	store, options := testStore(t)
	task := testObservation(testEnvironment("a"))
	approve(t, store, task)
	path := filepath.Join(options.Directory, "registry.json")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	foreign := options
	foreign.OwnerID = uuid.NewString()
	if _, err := Open(foreign); err == nil {
		t.Fatal("foreign installation adopted authority")
	}
	assertData(t, path, original)
	damaged := append(append([]byte{}, original...), []byte("incomplete-write")...)
	writeFile(t, path, damaged)
	if _, err := Open(options); err == nil {
		t.Fatal("corrupt registry allowed startup")
	}
	if _, err := store.ObserveBatch([]Observation{task}); err == nil {
		t.Fatal("new claim repaired corrupt authority by overwriting it")
	}
	assertData(t, path, damaged)
}

func TestMissingRegistryDoesNotAdoptExistingWorkerFiles(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := Options{Directory: filepath.Join(root, ".multica-runtime/state"), WorkspaceRoot: root, SessionRoot: filepath.Join(root, ".multica-runtime/sessions"), OwnerID: uuid.NewString()}
	file := filepath.Join(root, StoragePrefix, uuid.NewString(), "private-data")
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, file, []byte("unregistered data"))
	if _, err := Open(options); err == nil {
		t.Fatal("unregistered data was adopted as a current installation")
	}
	assertData(t, file, []byte("unregistered data"))
}

func TestLeaseAndActiveAttemptProtectRetirement(t *testing.T) {
	store, options := testStore(t)
	task := testObservation(testEnvironment("a"))
	claim := approve(t, store, task)
	root := prepareRoot(t, options.WorkspaceRoot, task)
	binding, err := store.Bind(claim, root, "", task.RuntimeRef)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(options.WorkspaceRoot, binding.WorkerSubPath, "work")
	writeFile(t, file, []byte("valuable work"))
	// Advance the fixture's claim age, without depending on a wall-clock sleep.
	if err := store.locked(func() error {
		state, err := store.read()
		if err != nil {
			return err
		}
		c := state.Claims[task.ID]
		c.ObservedAt = time.Now().Add(-365 * 24 * time.Hour)
		state.Claims[task.ID] = c
		return store.write(state)
	}); err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().Add(-60 * 24 * time.Hour)
	if _, err := store.Collect(options.WorkspaceRoot, cutoff, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	assertData(t, file, []byte("valuable work"))
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	release, err := store.AcquireLease(filepath.Base(binding.WorkerSubPath))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(options)
	if err != nil {
		release()
		t.Fatal(err)
	}
	if _, err := reopened.AcquireLease(filepath.Base(binding.WorkerSubPath)); err == nil {
		release()
		t.Fatal("another store acquired live storage lease")
	}
	if _, err := reopened.Collect(options.WorkspaceRoot, cutoff, map[string]bool{}); err != nil {
		release()
		t.Fatal(err)
	}
	assertData(t, file, []byte("valuable work"))
	release()
	if _, err := store.Collect(options.WorkspaceRoot, cutoff, map[string]bool{filepath.Base(binding.WorkerSubPath): true}); err != nil {
		t.Fatal(err)
	}
	assertData(t, file, []byte("valuable work"))
	if _, err := store.Collect(options.WorkspaceRoot, cutoff, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.ReadFile(file); !os.IsNotExist(err) {
		t.Fatal("retired inactive storage was not reclaimed")
	}
	if _, err := store.Lookup(task.ID, task.AuthToken, task.WorkspaceID, task.AgentID); err == nil {
		t.Fatal("retired task retained execution authority")
	}
}

func testStore(t *testing.T) (*Store, Options) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := Options{Directory: filepath.Join(root, ".multica-runtime/state"), WorkspaceRoot: root, SessionRoot: filepath.Join(root, ".multica-runtime/sessions"), OwnerID: uuid.NewString()}
	store, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	return store, options
}
func testEnvironment(name string) runtimeimage.Ref {
	sha := core.Digest([]byte(name))
	controller := core.Contract{SchemaVersion: 2, ControllerABI: 2, BuildID: sha, Platform: "linux/amd64", RuntimePath: core.Root + "/runtime", RuntimeSHA256: sha, GoVersion: "go1.26.1", ShimPaths: map[string]string{}}
	for _, alias := range []string{"pi", "codex", "copilot", "agy"} {
		controller.ShimPaths[alias] = core.Root + "/shims/" + alias
	}
	executable := runtimeimage.Executable{Path: "/opt/multica/tools/bin/pi", Version: "1.0.0", SHA256: sha}
	return runtimeimage.Ref{SchemaVersion: 2, Image: "example.invalid/runtime@sha256:" + sha, Platform: "linux/amd64", ImageBuildID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String(), DescriptorDigest: sha, Controller: controller, Daemon: runtimeimage.Daemon{Executable: runtimeimage.Executable{Path: "/opt/multica/tools/bin/multica", Version: "0.4.40", SHA256: sha}, AdapterContract: runtimeimage.AdapterContract}, Providers: map[string]runtimeimage.Executable{"pi": executable}, ConfigurationDigest: sha}
}
func testObservation(ref runtimeimage.Ref) Observation {
	id := uuid.NewString()
	return Observation{ID: id, WorkspaceID: "workspace", AgentID: "agent", IssueID: "issue", AuthToken: "mat_" + id, RepositoryURLs: []string{"https://example.invalid/private.git"}, RuntimeRef: ref}
}
func approve(t *testing.T, store *Store, task Observation) Claim {
	t.Helper()
	if _, err := store.ObserveBatch([]Observation{task}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.Lookup(task.ID, task.AuthToken, task.WorkspaceID, task.AgentID)
	if err != nil {
		t.Fatal(err)
	}
	return claim
}
func prepareRoot(t *testing.T, workspace string, task Observation) string {
	t.Helper()
	root := filepath.Join(workspace, "team", task.ID)
	if err := os.MkdirAll(filepath.Join(root, "workdir"), 0700); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]string{"workspace_id": task.WorkspaceID, "task_id": task.ID})
	writeFile(t, filepath.Join(root, ".task_owner"), raw)
	return root
}
func prepareSession(t *testing.T, root string) string {
	t.Helper()
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(root, "session-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return file.Name()
}
func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func assertData(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("protected data changed at %s: %v", path, err)
	}
}
