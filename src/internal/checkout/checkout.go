// Package checkout publishes official repository snapshots inside a task Pod.
package checkout

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/snapshot"
)

type Plan struct {
	URL     string `json:"url"`
	Ref     string `json:"ref"`
	WorkDir string `json:"workdir"`
	TaskID  string `json:"task_id"`
}

type Result struct {
	Path       string `json:"path"`
	BranchName string `json:"branch_name"`
}

type Executor struct {
	workDir string
	taskID  string
	environ []string
	guard   chan struct{}
}

// New binds an executor to the immutable task identity and its existing root.
// environ is the task process environment, including its credential settings.
func New(workDir string, environ []string) (*Executor, error) {
	if !filepath.IsAbs(workDir) {
		return nil, errors.New("task workdir must be absolute")
	}
	root, err := filepath.EvalSymlinks(workDir)
	if err != nil {
		return nil, fmt.Errorf("resolve task workdir: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return nil, errors.New("task workdir must be an existing directory")
	}
	e := &Executor{workDir: root, guard: make(chan struct{}, 1)}
	for _, entry := range environ {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if key == "MULTICA_TASK_ID" {
			if e.taskID != "" && e.taskID != value {
				return nil, errors.New("conflicting task identities in environment")
			}
			e.taskID = value
		}
		// A task's auth environment is retained, but Git must own its index and
		// object store inside the new clone instead of inheriting redirections.
		switch key {
		case "GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_TERMINAL_PROMPT":
			continue
		}
		e.environ = append(e.environ, entry)
	}
	if strings.TrimSpace(e.taskID) == "" {
		return nil, errors.New("MULTICA_TASK_ID is required for worker checkout")
	}
	e.environ = append(e.environ, "GIT_TERMINAL_PROMPT=0")
	return e, nil
}

type identity struct {
	URL      string `json:"url"`
	Ref      string `json:"ref"`
	HookHash string `json:"hook_hash,omitempty"`
}

const identityFile = "multica-checkout.json"

// Checkout executes a daemon-approved plan. Approval of URL/workspace ownership
// is the daemon's responsibility; this worker enforces the task and path binding.
// Repeating a plan preserves all agent changes. Selecting a different ref in an
// existing checkout is ordinary agent Git work and is never done implicitly.
func (e *Executor) Checkout(ctx context.Context, plan Plan, branch string, archive io.Reader) (Result, error) {
	select {
	case e.guard <- struct{}{}:
		defer func() { <-e.guard }()
	case <-ctx.Done():
		return Result{}, context.Cause(ctx)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, context.Cause(ctx)
	}
	if plan.TaskID != e.taskID {
		return Result{}, errors.New("checkout plan does not belong to this task")
	}
	plan.URL = strings.TrimSpace(plan.URL)
	plan.Ref = strings.TrimSpace(plan.Ref)
	if plan.URL == "" || strings.HasPrefix(plan.URL, "-") || strings.ContainsAny(plan.URL+plan.Ref, "\x00\r\n") {
		return Result{}, errors.New("invalid checkout URL or ref")
	}
	workDir, err := e.resolveWorkDir(plan.WorkDir)
	if err != nil {
		return Result{}, err
	}
	target := filepath.Join(workDir, repositoryName(plan.URL))
	want := identity{URL: plan.URL, Ref: plan.Ref}
	reuse := false
	if _, err := os.Lstat(target); err == nil {
		reuse = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return Result{}, err
	}

	// A failed or incomplete snapshot never alters the final directory.
	stage, err := os.MkdirTemp(workDir, ".multica-checkout-")
	if err != nil {
		return Result{}, fmt.Errorf("create checkout staging directory: %w", err)
	}
	defer os.RemoveAll(stage)
	if err := snapshot.Extract(archive, stage); err != nil {
		return Result{}, fmt.Errorf("extract approved checkout: %w", err)
	}
	info, err := os.Lstat(filepath.Join(stage, ".git"))
	if err != nil || !info.IsDir() {
		return Result{}, errors.New("snapshot is not a standalone Git checkout")
	}
	_, want.HookHash, err = snapshotHook(stage)
	if err != nil {
		return Result{}, err
	}
	if reuse {
		return e.reuse(ctx, target, want, stage)
	}

	raw, err := json.Marshal(want)
	if err != nil {
		return Result{}, err
	}
	if err := os.WriteFile(filepath.Join(stage, ".git", identityFile), raw, 0o600); err != nil {
		return Result{}, fmt.Errorf("record checkout identity: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, context.Cause(ctx)
	}
	if err := renameNoReplace(stage, target); err != nil {
		return Result{}, fmt.Errorf("publish checkout without replacing existing work at %s: %w; preserve or move the existing path and retry", target, err)
	}
	return Result{Path: target, BranchName: branch}, nil
}

func (e *Executor) reuse(ctx context.Context, target string, want identity, stage string) (Result, error) {
	for _, path := range []string{target, filepath.Join(target, ".git")} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return Result{}, fmt.Errorf("existing path %s is not an owned checkout; preserve or move it and retry", target)
		}
	}
	raw, err := os.ReadFile(filepath.Join(target, ".git", identityFile))
	var have identity
	// The controller authorizes this root's repository grant before mounting
	// it. A later task with that same grant may continue the existing checkout;
	// the current plan is still bound to e.taskID before any filesystem access.
	if err != nil || json.Unmarshal(raw, &have) != nil || have.URL != want.URL {
		return Result{}, fmt.Errorf("existing checkout at %s has a different or unknown repository; preserve or move it and retry", target)
	}
	if have.Ref != want.Ref {
		return Result{}, fmt.Errorf("checkout at %s already uses requested ref %q; changing to %q would modify existing work; use git inside the task to change refs explicitly", target, have.Ref, want.Ref)
	}
	if err := refreshSnapshotHook(target, stage, have, want); err != nil {
		return Result{}, err
	}
	branch, err := e.git(ctx, target, "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil {
		// A task may intentionally detach HEAD. Repeating checkout must retain
		// that state; verify the commit exists without modifying it.
		if _, verifyErr := e.git(ctx, target, "rev-parse", "--verify", "HEAD^{commit}"); verifyErr != nil {
			return Result{}, fmt.Errorf("existing checkout has no valid HEAD: %w", verifyErr)
		}
		branch = ""
	}
	return Result{Path: target, BranchName: branch}, nil
}

func (e *Executor) resolveWorkDir(requested string) (string, error) {
	if !filepath.IsAbs(requested) {
		return "", errors.New("checkout workdir must be absolute")
	}
	// Resolve the existing ancestor first, so a missing descendant below an
	// escaping symlink cannot cause MkdirAll to write outside the task root.
	ancestor := filepath.Clean(requested)
	var missing []string
	for {
		_, err := os.Lstat(ancestor)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) || ancestor == filepath.Dir(ancestor) {
			return "", fmt.Errorf("resolve checkout workdir: %w", err)
		}
		missing = append(missing, filepath.Base(ancestor))
		ancestor = filepath.Dir(ancestor)
	}
	resolved, err := filepath.EvalSymlinks(ancestor)
	if err != nil {
		return "", fmt.Errorf("resolve checkout workdir: %w", err)
	}
	for i := len(missing) - 1; i >= 0; i-- {
		resolved = filepath.Join(resolved, missing[i])
	}
	if !within(e.workDir, resolved) {
		return "", errors.New("checkout workdir is outside this task")
	}
	if err := os.MkdirAll(resolved, 0o755); err != nil {
		return "", fmt.Errorf("create checkout workdir: %w", err)
	}
	resolved, err = filepath.EvalSymlinks(resolved)
	if err != nil || !within(e.workDir, resolved) {
		return "", errors.New("checkout workdir changed outside this task")
	}
	return resolved, nil
}

func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func (e *Executor) git(ctx context.Context, dir string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	// Global credential configuration is useful; global Git hooks are not part
	// of an approved plan and must not execute during checkout preparation.
	full := append([]string{"-c", "core.hooksPath=/dev/null", "-C", dir}, args...)
	cmd := exec.CommandContext(ctx, "git", full...)
	cmd.Env = e.environ
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return "", context.Cause(ctx)
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", fmt.Errorf("git %s: %s: %w", args[0], strings.TrimSpace(string(exitErr.Stderr)), err)
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

var unsafeName = regexp.MustCompile(`[^a-z0-9]+`)

func slug(value, fallback string) string {
	value = strings.Trim(unsafeName.ReplaceAllString(strings.ToLower(value), "-"), "-")
	if len(value) > 40 {
		value = strings.TrimRight(value[:40], "-")
	}
	if value == "" {
		return fallback
	}
	return value
}

func repositoryName(url string) string {
	name := strings.TrimSuffix(filepath.Base(strings.TrimRight(url, "/")), ".git")
	sum := sha256.Sum256([]byte(url))
	return fmt.Sprintf("%s-%x", slug(name, "repo"), sum[:8])
}
