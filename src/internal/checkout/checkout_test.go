package checkout_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/checkout"
)

// These tests use actual repositories and Git subprocesses: their oracles are
// repository contents and preserved agent data, not Git command choreography.
func TestCheckoutSelectedRevisionAndRepeatPreservesAgentWork(t *testing.T) {
	env := gitEnvironment(t)
	source := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "main content")
	git(t, env, source, "checkout", "-b", "review")
	write(t, filepath.Join(source, "source.txt"), "review content")
	git(t, env, source, "commit", "-am", "review revision")
	git(t, env, source, "checkout", "main")
	root := t.TempDir()
	plan := checkout.Plan{URL: source, Ref: "review", WorkDir: root, TaskID: "task-a", AgentName: "reviewer"}
	executor := newExecutor(t, root, env)
	result, err := executor.Checkout(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	assertContent(t, filepath.Join(result.Path, "source.txt"), "review content")
	write(t, filepath.Join(result.Path, "source.txt"), "agent tracked edit")
	write(t, filepath.Join(result.Path, "notes.txt"), "agent untracked work")

	// A restarted proxy can recover the same checkout without discarding work.
	executor = newExecutor(t, root, env)
	if _, err := executor.Checkout(context.Background(), plan); err != nil {
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
	continued, err := executor.Checkout(context.Background(), plan)
	if err != nil {
		t.Fatalf("authorized continuation cannot recover prior work: %v", err)
	}
	assertContent(t, filepath.Join(continued.Path, "source.txt"), "agent tracked edit")
	assertContent(t, filepath.Join(continued.Path, "notes.txt"), "agent untracked work")
	if currentBranch := git(t, continuedEnv, continued.Path, "symbolic-ref", "HEAD"); currentBranch != priorBranch {
		t.Fatal("authorized continuation replaced the branch carrying prior work")
	}
	plan.Ref = "main"
	if _, err := executor.Checkout(context.Background(), plan); err == nil {
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
	firstResult, err := executor.Checkout(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(firstResult.Path, "source.txt"), "first agent work")
	plan.URL = second
	secondResult, err := executor.Checkout(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	assertContent(t, filepath.Join(secondResult.Path, "source.txt"), "second repository")
	assertContent(t, filepath.Join(firstResult.Path, "source.txt"), "first agent work")
}

func TestCheckoutObjectsSurviveSourceObjectCorruption(t *testing.T) {
	env := gitEnvironment(t)
	source := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "independent repository content")
	root := t.TempDir()
	executor := newExecutor(t, root, env)
	result, err := executor.Checkout(context.Background(), checkout.Plan{URL: source, WorkDir: root, TaskID: "task-a"})
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
	result, err := executor.Checkout(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	// Move the owned checkout aside, then put unrelated user data at that path.
	if err := os.Rename(result.Path, result.Path+"-saved"); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(result.Path, "valuable.txt"), "unrelated user data")
	if _, err := executor.Checkout(context.Background(), plan); err == nil {
		t.Fatal("checkout adopted an unowned user directory")
	}
	assertContent(t, filepath.Join(result.Path, "valuable.txt"), "unrelated user data")
	assertContent(t, filepath.Join(result.Path+"-saved", "source.txt"), "repository content")
}

func TestCheckoutDoesNotReplaceUserPathCreatedDuringClone(t *testing.T) {
	env := gitEnvironment(t)
	source := createRepository(t, env, filepath.Join(t.TempDir(), "project"), "repository content")
	transport := t.TempDir()
	ssh := filepath.Join(transport, "ssh")
	// This controlled transport runs the real Git server. The second clone is
	// held after it starts so unrelated user work can occupy the final path.
	write(t, ssh, `#!/bin/sh
if [ -f "$FIXTURE_GATE" ]; then
  touch "$FIXTURE_STARTED"
  while [ ! -f "$FIXTURE_RELEASE" ]; do sleep 0.01; done
fi
exec git-upload-pack "$FIXTURE_SOURCE"
`)
	if err := os.Chmod(ssh, 0o755); err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(transport, "gate")
	started := filepath.Join(transport, "started")
	release := filepath.Join(transport, "release")
	env = append(env, "GIT_SSH="+ssh, "GIT_SSH_VARIANT=ssh", "FIXTURE_SOURCE="+source,
		"FIXTURE_GATE="+gate, "FIXTURE_STARTED="+started, "FIXTURE_RELEASE="+release)
	root := t.TempDir()
	executor := newExecutor(t, root, env)
	plan := checkout.Plan{URL: "ssh://fixture/project.git", WorkDir: root, TaskID: "task-a"}
	result, err := executor.Checkout(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(result.Path, result.Path+"-saved"); err != nil {
		t.Fatal(err)
	}
	write(t, gate, "block next clone")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := executor.Checkout(ctx, plan)
		done <- err
	}()
	defer func() {
		// Release the child transport even if the assertion above fails.
		_ = os.WriteFile(release, nil, 0o600)
	}()
	for {
		if _, err := os.Stat(started); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("clone ended before reaching controlled transport: %v", err)
		case <-ctx.Done():
			t.Fatal("clone did not reach controlled transport")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// An empty user directory is significant: ordinary os.Rename can replace
	// it, while the required no-replace publish must preserve even that path.
	if err := os.Mkdir(result.Path, 0o755); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(result.Path)
	if err != nil {
		t.Fatal(err)
	}
	write(t, release, "continue")
	if err := <-done; err == nil {
		t.Fatal("checkout replaced a user directory created during clone")
	}
	after, err := os.Stat(result.Path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("failed publish replaced the concurrently created user directory")
	}
	assertContent(t, filepath.Join(result.Path+"-saved", "source.txt"), "repository content")
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
		_, err := executor.Checkout(context.Background(), checkout.Plan{URL: source, WorkDir: workDir, TaskID: "task-a"})
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
	if _, err := executor.Checkout(context.Background(), checkout.Plan{URL: source, WorkDir: unauthorized, TaskID: "task-b"}); err == nil {
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
