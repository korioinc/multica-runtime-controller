package migration

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"golang.org/x/sys/unix"
)

type fixture struct {
	root, state, registry, worker, session, preparation, registryLock, workerLock string
	owner, task, token, denied                                                    string
	source                                                                        []byte
	legacy                                                                        legacyRegistry
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := fixture{root: root, state: filepath.Join(root, ".multica-runtime/state"), owner: uuid.NewString(), task: uuid.NewString(), token: "mat_synthetic", denied: uuid.NewString()}
	f.registry = filepath.Join(f.state, "registry.json")
	f.registryLock = filepath.Join(f.state, "registry.lock")
	storage := filepath.Join(workspace.StoragePrefix, uuid.NewString())
	f.worker = filepath.Join(root, storage, "uncommitted")
	f.workerLock = filepath.Join(f.state, "worker-"+filepath.Base(storage)+".lock")
	f.session = filepath.Join(root, ".multica-runtime/sessions", uuid.NewString()+".jsonl")
	logicalSession := filepath.Join(workspace.DefaultSessionRoot, filepath.Base(f.session))
	f.preparation = filepath.Join(root, "team", f.task)
	for _, dir := range []string{f.state, filepath.Dir(f.worker), filepath.Dir(f.session), filepath.Join(f.preparation, "workdir"), filepath.Join(root, ".multica-runtime/attempts")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	put(t, f.registryLock, nil)
	put(t, f.workerLock, nil)
	put(t, f.worker, []byte("unfinished user work"))
	put(t, f.session, []byte("private provider history"))
	bindingID := uuid.NewString()
	put(t, filepath.Join(f.preparation, ".multica-runtime-binding"), []byte(bindingID))
	ownerBytes, _ := json.Marshal(map[string]string{"workspace_id": "workspace", "task_id": f.task})
	put(t, filepath.Join(f.preparation, ".task_owner"), ownerBytes)
	urls := []string{"https://example.invalid/private.git"}
	scopeBytes, _ := json.Marshal([]any{"workspace", "agent", "issue", "", "", urls})
	scope := core.Digest(scopeBytes)
	claim := legacyClaim{ID: f.task, WorkspaceID: "workspace", AgentID: "agent", IssueID: "issue", Grant: scope, TokenHash: core.Digest([]byte(f.token)), RepositoryURLs: urls, TaskEnvKeys: []string{"USER_FLAG"}, Environment: oldRef("a"), ObservedAt: time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), BoundRoot: f.preparation, WorkerSubPath: storage, PriorWorkDir: filepath.Join(f.preparation, "workdir"), PriorSession: logicalSession}
	denied := claim
	denied.ID = f.denied
	denied.Denied = true
	denied.TokenHash = ""
	denied.BoundRoot = ""
	denied.WorkerSubPath = ""
	denied.PriorWorkDir = ""
	denied.PriorSession = ""
	f.legacy = legacyRegistry{SchemaVersion: 1, OwnerID: f.owner, WorkspaceRoot: root, Claims: map[string]legacyClaim{f.task: claim, f.denied: denied}, Bindings: map[string]legacyBinding{f.preparation: {Root: f.preparation, Identity: bindingID, Grant: scope, WorkerSubPath: storage, Sessions: map[string]legacyRef{logicalSession: claim.Environment}}}, Retired: map[string]string{}}
	f.write(t)
	return f
}
func oldRef(label string) legacyRef {
	hash := core.Digest([]byte(label))
	return legacyRef{SchemaVersion: 1, EnvironmentID: hash, ContentDigest: hash, ManifestDigest: hash, CoreImage: "core@sha256:" + hash, EnvironmentImage: "environment@sha256:" + hash, Platform: "linux/amd64", Core: legacyCore{ContractVersion: 1, BuildID: "legacy-build", Platform: "linux/amd64", OfficialVersion: "0.4.40", OfficialSHA256: hash, Files: map[string]string{"runtime": hash, "multica": hash}}, Providers: map[string]legacyFingerprint{"pi": {Entrypoint: "providers/pi/run", SHA256: hash, VersionOutput: "pi fixture"}}}
}
func currentRef() runtimeimage.Ref {
	hash := core.Digest([]byte("current"))
	controller := core.Contract{SchemaVersion: 2, ControllerABI: 2, BuildID: hash, Platform: "linux/amd64", RuntimePath: core.Root + "/runtime", RuntimeSHA256: hash, GoVersion: "go1.26.1", ShimPaths: map[string]string{}}
	for _, alias := range []string{"pi", "codex", "copilot", "agy"} {
		controller.ShimPaths[alias] = core.Root + "/shims/" + alias
	}
	daemon := runtimeimage.Daemon{Executable: runtimeimage.Executable{Path: "/opt/multica/tools/bin/multica", Version: "0.4.40", SHA256: hash}, AdapterContract: runtimeimage.AdapterContract}
	return runtimeimage.Ref{SchemaVersion: 2, Image: "example.invalid/runtime@sha256:" + hash, Platform: "linux/amd64", ImageBuildID: uuid.NewString(), DescriptorDigest: hash, Controller: controller, Daemon: daemon, Providers: map[string]runtimeimage.Executable{"pi": {Path: "/opt/multica/tools/bin/pi", Version: "1.0.0", SHA256: hash}}, ConfigurationDigest: hash}
}
func (f *fixture) write(t *testing.T) {
	t.Helper()
	raw, err := json.MarshalIndent(f.legacy, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	f.source = append(raw, '\n')
	put(t, f.registry, f.source)
}
func (f fixture) options() workspace.Options {
	return workspace.Options{Directory: f.state, WorkspaceRoot: f.root, SessionRoot: workspace.DefaultSessionRoot, OwnerID: f.owner}
}
func (f fixture) commit() Options {
	return Options{Root: f.root, OwnerID: f.owner, ExpectedSourceSHA256: core.Digest(f.source), Commit: true}
}
func (f fixture) observation(id string, ref runtimeimage.Ref) workspace.Observation {
	return workspace.Observation{ID: id, WorkspaceID: "workspace", AgentID: "agent", IssueID: "issue", AuthToken: f.token, RepositoryURLs: []string{"https://example.invalid/private.git"}, TaskEnvKeys: []string{"USER_FLAG"}, RuntimeRef: ref}
}
func put(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func dataEquals(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("durable data changed: %s: %v", path, err)
	}
}
func stat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestMigrationPreservesPrivateWorkAndRequiresFreshAuthority(t *testing.T) {
	f := newFixture(t)
	lockBefore := stat(t, f.registryLock)
	workerLockBefore := stat(t, f.workerLock)
	workBefore := stat(t, f.worker)
	sessionBefore := stat(t, f.session)
	if _, err := workspace.Open(f.options()); !errors.Is(err, workspace.ErrMigrationRequired) {
		t.Fatal("normal startup did not require explicit migration of old authority", err)
	}
	dataEquals(t, f.registry, f.source)
	if _, err := Migrate(Options{Root: f.root, OwnerID: f.owner}); err != nil {
		t.Fatal(err)
	}
	dataEquals(t, f.registry, f.source)
	if _, err := os.Stat(filepath.Join(f.state, "migrations")); !os.IsNotExist(err) {
		t.Fatal("dry-run created migration artifacts", err)
	}
	if _, err := Migrate(f.commit()); err != nil {
		t.Fatal(err)
	}
	dataEquals(t, filepath.Join(f.state, "migrations/v1-to-v2", core.Digest(f.source), "registry.v1.json"), f.source)
	dataEquals(t, f.worker, []byte("unfinished user work"))
	dataEquals(t, f.session, []byte("private provider history"))
	for _, pair := range []struct {
		path   string
		before os.FileInfo
	}{{f.registryLock, lockBefore}, {f.workerLock, workerLockBefore}, {f.worker, workBefore}, {f.session, sessionBefore}} {
		after := stat(t, pair.path)
		beforeOwner, _ := ownership(pair.before)
		afterOwner, _ := ownership(after)
		if !os.SameFile(pair.before, after) || beforeOwner != afterOwner || pair.before.Mode() != after.Mode() {
			t.Fatal("migration replaced or changed ownership of existing private data/lock")
		}
	}
	store, err := workspace.Open(f.options())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Lookup(f.task, f.token, "workspace", "agent"); err == nil {
		t.Fatal("legacy credential executed before a fresh observation")
	}
	ref := currentRef()
	observation := f.observation(f.task, ref)
	observation.PriorWorkDir = filepath.Join(f.preparation, "workdir")
	observation.PriorSession = filepath.Join(workspace.DefaultSessionRoot, filepath.Base(f.session))
	decision, err := store.Observe(observation)
	if err != nil {
		t.Fatal(err)
	}
	if decision.ResetWorkDir || !decision.ResetSession {
		t.Fatal("migration must preserve authorized work but exclude archived session continuation")
	}
	claim, err := store.Lookup(f.task, f.token, "workspace", "agent")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := store.Bind(claim, f.preparation, "", ref)
	if err != nil {
		t.Fatal(err)
	}
	dataEquals(t, filepath.Join(f.root, binding.WorkerSubPath, "uncommitted"), []byte("unfinished user work"))
	if _, err := store.Observe(f.observation(f.denied, ref)); err == nil {
		t.Fatal("a fresh observation restored permanently denied authority")
	}
}

func TestMigrationRefusesBusyIncompleteOrConflictingAuthority(t *testing.T) {
	cases := map[string]func(*testing.T, *fixture) func(){
		"registry writer":  func(t *testing.T, f *fixture) func() { return hold(t, f.registryLock) },
		"worker execution": func(t *testing.T, f *fixture) func() { return hold(t, f.workerLock) },
		"unbound worker lock": func(t *testing.T, f *fixture) func() {
			path := filepath.Join(f.state, "worker-"+uuid.NewString()+".lock")
			put(t, path, nil)
			return hold(t, path)
		},
		"pending attempt": func(t *testing.T, f *fixture) func() {
			put(t, filepath.Join(f.root, ".multica-runtime/attempts/.pending-interrupted"), []byte("incomplete"))
			return func() {}
		},
		"unknown attempt": func(t *testing.T, f *fixture) func() {
			if err := os.Mkdir(filepath.Join(f.root, ".multica-runtime/attempts/unknown"), 0700); err != nil {
				t.Fatal(err)
			}
			return func() {}
		},
		"redirected worker root": func(t *testing.T, f *fixture) func() {
			root := filepath.Dir(f.worker)
			moved := root + "-user-data"
			if err := os.Rename(root, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(moved, root); err != nil {
				t.Fatal(err)
			}
			return func() {}
		},
		"redirected session leaf": func(t *testing.T, f *fixture) func() {
			moved := f.session + "-user-data"
			if err := os.Rename(f.session, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(moved, f.session); err != nil {
				t.Fatal(err)
			}
			return func() {}
		},
		"invalid legacy reference": func(t *testing.T, f *fixture) func() {
			claim := f.legacy.Claims[f.task]
			claim.Environment.Core.OfficialSHA256 = "invalid"
			f.legacy.Claims[f.task] = claim
			f.write(t)
			return func() {}
		},
		"conflicting legacy session": func(t *testing.T, f *fixture) func() {
			binding := f.legacy.Bindings[f.preparation]
			binding.Root = filepath.Join(f.root, "team", uuid.NewString())
			binding.Identity = uuid.NewString()
			binding.Sessions = map[string]legacyRef{filepath.Join(workspace.DefaultSessionRoot, filepath.Base(f.session)): oldRef("different")}
			f.legacy.Bindings[binding.Root] = binding
			f.write(t)
			return func() {}
		},
		"duplicate authority key": func(t *testing.T, f *fixture) func() {
			f.source = bytes.Replace(f.source, []byte(`"schemaVersion": 1,`), []byte(`"schemaVersion": 1, "schemaVersion": 1,`), 1)
			put(t, f.registry, f.source)
			return func() {}
		},
	}
	for name, prepare := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			release := prepare(t, &f)
			defer release()
			if _, err := Migrate(f.commit()); err == nil {
				t.Fatal("unsafe authority was migrated")
			}
			dataEquals(t, f.registry, f.source)
			dataEquals(t, f.worker, []byte("unfinished user work"))
			dataEquals(t, f.session, []byte("private provider history"))
		})
	}
}
func hold(t *testing.T, path string) func() {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	return func() { _ = unix.Flock(int(file.Fd()), unix.LOCK_UN); _ = file.Close() }
}

func TestMigrationRetryPreservesCommittedAuthorityAfterEachDurableBoundary(t *testing.T) {
	for _, boundary := range []string{"backup", "candidate", "renamed"} {
		t.Run(boundary, func(t *testing.T) {
			f := newFixture(t)
			sourceInfo := stat(t, f.registry)
			owner, err := ownership(sourceInfo)
			if err != nil {
				t.Fatal(err)
			}
			// Exercise the same durable production stages, then stop before the next
			// stage. A retry sees only the files already fsynced by those stages.
			if boundary != "renamed" {
				lock, err := acquire(f.registryLock, owner.uid)
				if err != nil {
					t.Fatal(err)
				}
				err = preserveBackup(f.state, filepath.Join(f.state, "migrations/v1-to-v2", core.Digest(f.source)), f.source, owner)
				if err == nil && boundary == "candidate" {
					candidate, convertErr := convert(f.options(), f.legacy)
					if convertErr != nil {
						t.Fatal(convertErr)
					}
					raw, _ := json.Marshal(candidate)
					_, err = writeCandidate(f.state, raw, owner)
				}
				lock.close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Migrate(f.commit()); err != nil {
				t.Fatal(err)
			}
			store, err := workspace.Open(f.options())
			if err != nil {
				t.Fatal(err)
			}
			next := f.observation(uuid.NewString(), currentRef())
			if _, err := store.Observe(next); err != nil {
				t.Fatal(err)
			}
			if _, err := Migrate(f.commit()); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Lookup(next.ID, next.AuthToken, next.WorkspaceID, next.AgentID); err != nil {
				t.Fatal("retry restored old backup over later schema-2 authority", err)
			}
			dataEquals(t, filepath.Join(f.state, "migrations/v1-to-v2", core.Digest(f.source), "registry.v1.json"), f.source)
			dataEquals(t, f.worker, []byte("unfinished user work"))
		})
	}
}

func TestChangedSourceOrDamagedBackupCannotBeOverwritten(t *testing.T) {
	t.Run("source changed after dry-run", func(t *testing.T) {
		f := newFixture(t)
		options := f.commit()
		claim := f.legacy.Claims[f.task]
		claim.TaskEnvKeys = append(claim.TaskEnvKeys, "ZZ_FLAG")
		f.legacy.Claims[f.task] = claim
		f.write(t)
		if _, err := Migrate(options); err == nil {
			t.Fatal("migration overwrote a changed source")
		}
		dataEquals(t, f.registry, f.source)
	})
	t.Run("damaged backup", func(t *testing.T) {
		f := newFixture(t)
		owner, _ := ownership(stat(t, f.registry))
		directory := filepath.Join(f.state, "migrations/v1-to-v2", core.Digest(f.source))
		if err := preserveBackup(f.state, directory, f.source, owner); err != nil {
			t.Fatal(err)
		}
		backup := filepath.Join(directory, "registry.v1.json")
		damaged := []byte("damaged original backup")
		put(t, backup, damaged)
		if _, err := Migrate(f.commit()); err == nil {
			t.Fatal("migration replaced a conflicting backup")
		}
		dataEquals(t, backup, damaged)
		dataEquals(t, f.registry, f.source)
	})
}
