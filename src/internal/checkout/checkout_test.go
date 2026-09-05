package checkout_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/snapshot"
)

// These tests use actual repositories and Git subprocesses: their oracles are
// repository contents and preserved agent data, not Git command choreography.
func TestSnapshotRevisionAndRepeatPreserveAgentWork(t *testing.T) {
	env := gitEnvironment(t)
	source := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "main content")
	git(t, env, source, "checkout", "-b", "review")
	write(t, filepath.Join(source, "source.txt"), "review content")
	git(t, env, source, "commit", "-am", "review revision")
	root := t.TempDir()
	plan := checkout.Plan{URL: source, Ref: "review", WorkDir: root, TaskID: "task-a"}
	executor := newExecutor(t, root, env)
	result, err := checkoutArchive(t, executor, plan, source)
	if err != nil {
		t.Fatal(err)
	}
	assertContent(t, filepath.Join(result.Path, "source.txt"), "review content")
	write(t, filepath.Join(result.Path, "source.txt"), "agent tracked edit")
	write(t, filepath.Join(result.Path, "notes.txt"), "agent untracked work")

	// A restarted proxy can recover the same checkout without discarding work.
	executor = newExecutor(t, root, env)
	if _, err := checkoutArchive(t, executor, plan, source); err != nil {
		t.Fatal(err)
	}
	assertContent(t, filepath.Join(result.Path, "source.txt"), "agent tracked edit")
	assertContent(t, filepath.Join(result.Path, "notes.txt"), "agent untracked work")

	// The controller may grant a follow-up task the same repository-scoped
	// root. Its new immutable task ID must not strand the conversation's work.
	priorBranch := git(t, env, result.Path, "symbolic-ref", "HEAD")
	continuedEnv := append([]string(nil), env...)
	for i, entry := range continuedEnv {
		if strings.HasPrefix(entry, "MULTICA_TASK_ID=") {
			continuedEnv[i] = "MULTICA_TASK_ID=task-b"
		}
	}
	executor = newExecutor(t, root, continuedEnv)
	plan.TaskID = "task-b"
	continued, err := checkoutArchive(t, executor, plan, source)
	if err != nil {
		t.Fatalf("authorized continuation cannot recover prior work: %v", err)
	}
	assertContent(t, filepath.Join(continued.Path, "source.txt"), "agent tracked edit")
	assertContent(t, filepath.Join(continued.Path, "notes.txt"), "agent untracked work")
	if currentBranch := git(t, continuedEnv, continued.Path, "symbolic-ref", "HEAD"); currentBranch != priorBranch {
		t.Fatal("authorized continuation replaced the branch carrying prior work")
	}
	plan.Ref = "main"
	if _, err := checkoutArchive(t, executor, plan, source); err == nil {
		t.Fatal("checkout silently accepted a ref change on existing agent work")
	}
	assertContent(t, filepath.Join(result.Path, "source.txt"), "agent tracked edit")
	assertContent(t, filepath.Join(result.Path, "notes.txt"), "agent untracked work")
}

func TestSameBasenameRepositoriesKeepSeparateContents(t *testing.T) {
	env := gitEnvironment(t)
	first := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "first repository")
	second := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "second repository")
	root := t.TempDir()
	executor := newExecutor(t, root, env)
	plan := checkout.Plan{URL: first, WorkDir: root, TaskID: "task-a"}
	firstResult, err := checkoutArchive(t, executor, plan, first)
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(firstResult.Path, "source.txt"), "first agent work")
	plan.URL = second
	secondResult, err := checkoutArchive(t, executor, plan, second)
	if err != nil {
		t.Fatal(err)
	}
	assertContent(t, filepath.Join(secondResult.Path, "source.txt"), "second repository")
	assertContent(t, filepath.Join(firstResult.Path, "source.txt"), "first agent work")
}

func TestOfficialHookUpdatesCommitAttributionWithoutResettingWork(t *testing.T) {
	env := gitEnvironment(t)
	source := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "repository content")
	hook := filepath.Join(source, ".git", "hooks", "prepare-commit-msg")
	const attribution = "fixture-author-credit"
	enable := func() {
		write(t, hook, "#!/bin/sh\nprintf '\\n"+attribution+"\\n' >> \"$1\"\n")
		if err := os.Chmod(hook, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	enable()
	root := t.TempDir()
	executor := newExecutor(t, root, env)
	plan := checkout.Plan{URL: source, WorkDir: root, TaskID: "task-a"}
	result, err := checkoutArchive(t, executor, plan, source)
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(result.Path, "source.txt"), "unfinished edit")
	for _, enabled := range []bool{true, false, true} {
		if enabled {
			enable()
		} else if err := os.Remove(hook); err != nil {
			t.Fatal(err)
		}
		if _, err := checkoutArchive(t, executor, plan, source); err != nil {
			t.Fatal(err)
		}
		git(t, env, result.Path, "commit", "--allow-empty", "-m", "worker commit")
		message := git(t, env, result.Path, "log", "-1", "--format=%B")
		if strings.Contains(message, attribution) != enabled {
			t.Fatal("commit did not use the current official attribution setting")
		}
		assertContent(t, filepath.Join(result.Path, "source.txt"), "unfinished edit")
	}
}

func TestCheckoutObjectsSurviveSourceObjectCorruption(t *testing.T) {
	env := gitEnvironment(t)
	source := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "independent repository content")
	root := t.TempDir()
	executor := newExecutor(t, root, env)
	result, err := checkoutArchive(t, executor, checkout.Plan{URL: source, WorkDir: root, TaskID: "task-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the source's actual object bytes. A cache-backed linked worktree,
	// alternate object store, or hardlinked local clone would lose its history.
	objects := filepath.Join(source, ".git", "objects")
	if err := filepath.WalkDir(objects, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			if err := os.Chmod(path, 0o600); err != nil {
				return err
			}
			return os.WriteFile(path, []byte("corrupted source object"), 0o600)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(source); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(result.Path, "source.txt"), "discardable local probe")
	git(t, env, result.Path, "checkout", "HEAD", "--", "source.txt")
	assertContent(t, filepath.Join(result.Path, "source.txt"), "independent repository content")
}

func TestFailedCheckoutPreservesUnownedExistingPath(t *testing.T) {
	env := gitEnvironment(t)
	source := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "repository content")
	root := t.TempDir()
	executor := newExecutor(t, root, env)
	plan := checkout.Plan{URL: source, WorkDir: root, TaskID: "task-a"}
	result, err := checkoutArchive(t, executor, plan, source)
	if err != nil {
		t.Fatal(err)
	}
	// Move the owned checkout aside, then put unrelated user data at that path.
	if err := os.Rename(result.Path, result.Path+"-saved"); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(result.Path, "valuable.txt"), "unrelated user data")
	if _, err := checkoutArchive(t, executor, plan, source); err == nil {
		t.Fatal("checkout adopted an unowned user directory")
	}
	assertContent(t, filepath.Join(result.Path, "valuable.txt"), "unrelated user data")
	assertContent(t, filepath.Join(result.Path+"-saved", "source.txt"), "repository content")
}

func TestCheckoutDoesNotReplaceUserPathCreatedDuringExtraction(t *testing.T) {
	env := gitEnvironment(t)
	source := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "repository content")
	root := t.TempDir()
	executor := newExecutor(t, root, env)
	plan := checkout.Plan{URL: source, WorkDir: root, TaskID: "task-a"}
	result, err := checkoutArchive(t, executor, plan, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(result.Path, result.Path+"-saved"); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := snapshot.Write(&archive, source); err != nil {
		t.Fatal(err)
	}
	var before os.FileInfo
	reader := &onRead{Reader: &archive, action: func() {
		if err := os.Mkdir(result.Path, 0o755); err != nil {
			t.Fatal(err)
		}
		before, err = os.Stat(result.Path)
		if err != nil {
			t.Fatal(err)
		}
	}}
	if _, err := executor.Checkout(context.Background(), plan, "main", reader); err == nil {
		t.Fatal("checkout replaced a directory created before publication")
	}
	after, err := os.Stat(result.Path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("publication replaced the user's existing directory")
	}
	assertContent(t, filepath.Join(result.Path+"-saved", "source.txt"), "repository content")
}

type onRead struct {
	io.Reader
	action func()
}

func (r *onRead) Read(p []byte) (int, error) {
	if r.action != nil {
		action := r.action
		r.action = nil
		action()
	}
	return r.Reader.Read(p)
}

func TestCheckoutCannotWriteThroughAnotherTasksDirectory(t *testing.T) {
	env := gitEnvironment(t)
	source := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "private repository")
	root := t.TempDir()
	outside := t.TempDir()
	executor := newExecutor(t, root, env)
	link := filepath.Join(root, "other-task")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	for _, workDir := range []string{outside, filepath.Join(link, "new-directory")} {
		_, err := checkoutArchive(t, executor, checkout.Plan{URL: source, WorkDir: workDir, TaskID: "task-a"}, source)
		if err == nil {
			t.Fatal("checkout allowed a write outside the task root")
		}
	}
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("rejected checkout wrote into another task directory")
	}
	unauthorized := filepath.Join(root, "unauthorized")
	if _, err := checkoutArchive(t, executor, checkout.Plan{URL: source, WorkDir: unauthorized, TaskID: "task-b"}, source); err == nil {
		t.Fatal("checkout accepted a different task identity")
	}
	if _, err := os.Stat(unauthorized); !os.IsNotExist(err) {
		t.Fatal("another task caused a checkout write")
	}
}

func gitEnvironment(t *testing.T) []string {
	t.Helper()
	home := t.TempDir()
	return []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + home, "XDG_CONFIG_HOME=" + home,
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.test",
		"GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.test",
		"MULTICA_TASK_ID=task-a",
	}
}

func createRepository(t *testing.T, env []string, path, content string) string {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, env, path, "init", "-b", "main")
	write(t, filepath.Join(path, "source.txt"), content)
	git(t, env, path, "add", "source.txt")
	git(t, env, path, "commit", "-m", "initial")
	return path
}

func newExecutor(t *testing.T, root string, env []string) *checkout.Executor {
	t.Helper()
	executor, err := checkout.New(root, env)
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func git(t *testing.T, env []string, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %s: %v", args, out, err)
	}
	return strings.TrimSpace(string(out))
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("agent data changed: got %q, want %q", got, want)
	}
}

func checkoutArchive(t *testing.T, executor *checkout.Executor, plan checkout.Plan, source string) (checkout.Result, error) {
	t.Helper()
	var archive bytes.Buffer
	if err := snapshot.Write(&archive, source); err != nil {
		t.Fatal(err)
	}
	return executor.Checkout(context.Background(), plan, "main", &archive)
}
