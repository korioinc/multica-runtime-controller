package workspace

import (
	"path/filepath"
	"testing"
)

func TestArchivedEmptySessionCannotBeReactivated(t *testing.T) {
	store, options := testStore(t)
	ref := testEnvironment("archive")
	task := testObservation(ref)
	claim := approve(t, store, task)
	root := prepareRoot(t, options.WorkspaceRoot, task)
	session := prepareSession(t, options.SessionRoot)
	if _, err := store.Bind(claim, root, session, ref); err != nil {
		t.Fatal(err)
	}
	if err := store.locked(func() error {
		state, err := store.read()
		if err != nil {
			return err
		}
		binding := state.Bindings[root]
		binding.Sessions[session] = SessionRecord{State: "archived", RuntimeRef: nil}
		state.Bindings[root] = binding
		return store.write(state)
	}); err != nil {
		t.Fatal(err)
	}
	task.PriorWorkDir = filepath.Join(root, "workdir")
	task.PriorSession = session
	decision, err := store.Observe(task)
	if err != nil {
		t.Fatal(err)
	}
	if decision.ResetWorkDir || !decision.ResetSession {
		t.Fatal("archived session was offered for continuation")
	}
	claim, err = store.Lookup(task.ID, task.AuthToken, task.WorkspaceID, task.AgentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Bind(claim, root, session, ref); err == nil {
		t.Fatal("an empty archived session acquired new execution authority")
	}
	fresh := prepareSession(t, options.SessionRoot)
	if _, err := store.Bind(claim, root, fresh, ref); err != nil {
		t.Fatal("fresh session could not use the preserved work", err)
	}
}
