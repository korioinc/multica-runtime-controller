package workspace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCorruptBindingCannotTransferAnotherScopesPrivateWork(t *testing.T) {
	store, options := testStore(t)
	ref := testEnvironment("a")
	a := testObservation(ref)
	rootA := prepareRoot(t, options.WorkspaceRoot, a)
	sessionA := prepareSession(t, options.SessionRoot)
	bindingA, err := store.Bind(approve(t, store, a), rootA, sessionA, ref)
	if err != nil {
		t.Fatal(err)
	}
	b := testObservation(ref)
	b.RepositoryURLs = []string{"https://example.invalid/other-private.git"}
	rootB := prepareRoot(t, options.WorkspaceRoot, b)
	sessionB := prepareSession(t, options.SessionRoot)
	bindingB, err := store.Bind(approve(t, store, b), rootB, sessionB, ref)
	if err != nil {
		t.Fatal(err)
	}
	fileA := filepath.Join(options.WorkspaceRoot, bindingA.WorkerSubPath, "private")
	fileB := filepath.Join(options.WorkspaceRoot, bindingB.WorkerSubPath, "private")
	writeFile(t, fileA, []byte("scope A work"))
	writeFile(t, fileB, []byte("scope B work"))
	path := filepath.Join(options.Directory, "registry.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state registry
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	changed := state.Bindings[rootA]
	changed.WorkerSubPath = bindingB.WorkerSubPath
	state.Bindings[rootA] = changed
	damaged, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, damaged)
	if _, err := Open(options); err == nil {
		t.Fatal("well-formed corrupt binding could authorize another scope's worker data")
	}
	assertData(t, fileA, []byte("scope A work"))
	assertData(t, fileB, []byte("scope B work"))
}
