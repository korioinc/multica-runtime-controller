package checkout

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func gitFixture(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-c", "user.name=Fixture", "-c", "user.email=fixture@invalid", "-c", "maintenance.auto=false", "-C", directory}, args...)...)
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("fixture git: %v: %s", err, out)
	}
	return strings.TrimSpace(string(out))
}
func ownedArchive(t *testing.T, source string) *bytes.Reader {
	t.Helper()
	var stream bytes.Buffer
	if err := WriteArchive(&stream, source); err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(stream.Bytes())
}

func TestCheckoutKeepsPrivateEditsBranchAndCustomHook(t *testing.T) {
	source := t.TempDir()
	gitFixture(t, source, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(source, "work.txt"), []byte("upstream\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitFixture(t, source, "add", "work.txt")
	gitFixture(t, source, "commit", "-m", "initial")
	hook := filepath.Join(source, ".git/hooks/prepare-commit-msg")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\n# official coauthor A\n"), 0755); err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	task := uuid.NewString()
	publisher, err := New(root, task, os.Environ())
	if err != nil {
		t.Fatal(err)
	}
	plan := wire.Plan{URL: "https://fixture.invalid/source.git", Ref: "main", TaskID: task, WorkDir: root}
	first, err := publisher.Publish(context.Background(), plan, "main", ownedArchive(t, source))
	if err != nil {
		t.Fatal(err)
	}
	gitFixture(t, first.Path, "switch", "-c", "user-branch")
	if err := os.WriteFile(filepath.Join(first.Path, "work.txt"), []byte("private edit\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(context.Background(), plan, "main", ownedArchive(t, source)); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(first.Path, "work.txt"))
	if err != nil || string(actual) != "private edit\n" {
		t.Fatal("repeated checkout lost the user's edit")
	}
	if branch := gitFixture(t, first.Path, "branch", "--show-current"); branch != "user-branch" {
		t.Fatal("repeated checkout switched the user's branch")
	}
	if err := os.WriteFile(filepath.Join(first.Path, ".git/hooks/prepare-commit-msg"), []byte("#!/bin/sh\n# user hook\n"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hook, []byte("#!/bin/sh\n# official coauthor B\n"), 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(context.Background(), plan, "main", ownedArchive(t, source)); err == nil {
		t.Fatal("official hook update overwrote a user-owned hook")
	}
	actual, err = os.ReadFile(filepath.Join(first.Path, ".git/hooks/prepare-commit-msg"))
	if err != nil || !bytes.Contains(actual, []byte("user hook")) {
		t.Fatal("user hook was lost")
	}
	if value := gitFixture(t, source, "show", "HEAD:work.txt"); value != "upstream" {
		t.Fatal("worker edit mutated the source repository")
	}
}

func TestArchiveCannotWriteThroughSymlinkParent(t *testing.T) {
	outside := t.TempDir()
	victim := filepath.Join(outside, "value")
	if err := os.WriteFile(victim, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	writer := tar.NewWriter(&stream)
	if err := writer.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: outside, Mode: 0777}); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteHeader(&tar.Header{Name: "link/value", Typeflag: tar.TypeReg, Mode: 0600, Size: 9}); err != nil {
		t.Fatal(err)
	}
	_, _ = writer.Write([]byte("corrupted"))
	_ = writer.Close()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := extractArchive(bytes.NewReader(stream.Bytes()), root); err == nil {
		t.Fatal("archive wrote through an escaping symlink")
	}
	actual, err := os.ReadFile(victim)
	if err != nil || string(actual) != "private" {
		t.Fatal("archive modified unrelated data")
	}
}

func TestCheckoutRejectsWrongTaskBeforeCreatingDirectories(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := New(root, uuid.NewString(), os.Environ())
	if err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "value")
	if err := os.WriteFile(sentinel, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = publisher.Publish(context.Background(), wire.Plan{TaskID: uuid.NewString(), URL: "https://fixture.invalid/a.git", WorkDir: outside}, "main", strings.NewReader(""))
	if err == nil {
		t.Fatal("a foreign task obtained checkout authority")
	}
	actual, err := os.ReadFile(sentinel)
	if err != nil || string(actual) != "private" {
		t.Fatal("foreign checkout changed unrelated task files")
	}
}
