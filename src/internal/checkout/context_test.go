package checkout

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func contextFixture(t *testing.T) (string, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root, filepath.Join(t.TempDir(), "context.json")
}

func contextContents(t *testing.T, root, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestContextRefreshCannotOverwriteUnownedFiles(t *testing.T) {
	root, record := contextFixture(t)
	brief := ManagedText{Content: []byte("task instructions"), Begin: "<managed>", End: "</managed>"}
	owned := "workdir/context.md"
	original := []byte("previous task context")
	if err := SeedContext(root, record, map[string][]byte{owned: original}, brief); err != nil {
		t.Fatal(err)
	}
	private := "workdir/notes.md"
	privateContents := []byte("user research that must survive")
	if err := os.WriteFile(filepath.Join(root, private), privateContents, 0600); err != nil {
		t.Fatal(err)
	}
	if err := SeedContext(root, record, map[string][]byte{owned: []byte("updated task context"), private: []byte("replacement")}, brief); err == nil {
		t.Fatal("context refresh adopted an unowned user file")
	}
	if !bytes.Equal(contextContents(t, root, private), privateContents) {
		t.Fatal("rejected context refresh lost user work")
	}
	if !bytes.Equal(contextContents(t, root, owned), original) {
		t.Fatal("rejected context refresh partially changed existing context")
	}
}

func TestContextRefreshPreservesUserInstructionsAndRetractsOwnedData(t *testing.T) {
	root, record := contextFixture(t)
	brief := ManagedText{Content: []byte("<managed>previous task instructions</managed>\n"), Begin: "<managed>", End: "</managed>"}
	withdrawn := "workdir/old-context.md"
	retained := "workdir/current-context.md"
	if err := SeedContext(root, record, map[string][]byte{withdrawn: []byte("withdrawn task input"), retained: []byte("previous input")}, brief); err != nil {
		t.Fatal(err)
	}
	prefix, suffix := []byte("user instructions before\n"), []byte("\nuser instructions after")
	existing := append(append(bytes.Clone(prefix), brief.Content...), suffix...)
	if err := os.WriteFile(filepath.Join(root, "workdir/AGENTS.md"), existing, 0600); err != nil {
		t.Fatal(err)
	}
	updated := []byte("current task input")
	brief.Content = []byte("<managed>current task instructions</managed>\n")
	if err := SeedContext(root, record, map[string][]byte{retained: updated}, brief); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contextContents(t, root, retained), updated) {
		t.Fatal("authorized context was not refreshed")
	}
	if _, err := os.Stat(filepath.Join(root, withdrawn)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("withdrawn owned data remained available to the next task")
	}
	actual := contextContents(t, root, "workdir/AGENTS.md")
	if !bytes.HasPrefix(actual, prefix) || !bytes.HasSuffix(actual, suffix) {
		t.Fatal("context refresh lost user instructions outside its managed region")
	}
	if !bytes.Contains(actual, []byte("current task instructions")) || bytes.Contains(actual, []byte("previous task instructions")) {
		t.Fatal("context refresh did not replace its owned instructions")
	}
}
