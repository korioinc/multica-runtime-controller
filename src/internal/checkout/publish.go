// Package checkout relays the official isolated checkout and publishes private
// worker repositories without overwriting user changes.
package checkout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type Publisher struct {
	root string
	task string
	env  []string
	mu   sync.Mutex
}

func New(root, task string, env []string) (*Publisher, error) {
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root || !filepath.IsAbs(root) || !wire.UUID(task) {
		return nil, errors.New("canonical task checkout root required")
	}
	clean := []string{}
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "GIT_DIR", "GIT_COMMON_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_TERMINAL_PROMPT":
			continue
		}
		clean = append(clean, entry)
	}
	return &Publisher{root: root, task: task, env: append(clean, "GIT_TERMINAL_PROMPT=0")}, nil
}

type checkoutIdentity struct {
	SchemaVersion int    `json:"schemaVersion"`
	URL           string `json:"url"`
	Ref           string `json:"ref"`
	HookDigest    string `json:"hookDigest"`
}

const checkoutRecord = ".git/multica-checkout-v1.json"

var repositorySlug = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

func (p *Publisher) Publish(ctx context.Context, plan wire.Plan, branch string, archive io.Reader) (wire.Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ctx.Err() != nil {
		return wire.Result{}, ctx.Err()
	}
	if plan.TaskID != p.task || plan.URL == "" || strings.HasPrefix(plan.URL, "-") || strings.ContainsAny(plan.URL+plan.Ref, "\x00\n\r") {
		return wire.Result{}, errors.New("checkout task or URL mismatch")
	}
	rel, err := filepath.Rel(p.root, plan.WorkDir)
	if err != nil || !filepath.IsLocal(rel) {
		return wire.Result{}, errors.New("checkout path escapes task storage")
	}
	base, err := os.OpenRoot(p.root)
	if err != nil {
		return wire.Result{}, err
	}
	defer base.Close()
	if err := plainDirectories(base, filepath.ToSlash(rel), 0755); err != nil {
		return wire.Result{}, err
	}
	parent, err := base.OpenRoot(rel)
	if err != nil {
		return wire.Result{}, err
	}
	defer parent.Close()
	name := strings.Trim(repositorySlug.ReplaceAllString(strings.TrimSuffix(filepath.Base(plan.URL), ".git"), "-"), "-")
	if len(name) > 40 {
		name = name[:40]
	}
	if name == "" {
		name = "repository"
	}
	name += "-" + wire.Digest([]byte(plan.URL))[:16]
	stage := ".checkout-" + uuid.NewString()
	if err := parent.Mkdir(stage, 0700); err != nil {
		return wire.Result{}, err
	}
	defer parent.RemoveAll(stage)
	staging, err := parent.OpenRoot(stage)
	if err != nil {
		return wire.Result{}, err
	}
	defer staging.Close()
	if err := extractArchive(archive, staging); err != nil {
		return wire.Result{}, err
	}
	if err := standalone(staging); err != nil {
		return wire.Result{}, err
	}
	hook, err := readHook(staging)
	if err != nil {
		return wire.Result{}, err
	}
	wanted := checkoutIdentity{SchemaVersion: 1, URL: plan.URL, Ref: plan.Ref}
	if len(hook) > 0 {
		wanted.HookDigest = wire.Digest(hook)
	}
	if info, err := parent.Lstat(name); err == nil {
		if !info.IsDir() {
			return wire.Result{}, errors.New("checkout destination belongs to the user")
		}
		existing, err := parent.OpenRoot(name)
		if err != nil {
			return wire.Result{}, err
		}
		defer existing.Close()
		return p.reuseCheckout(ctx, existing, filepath.Join(plan.WorkDir, name), wanted, hook)
	} else if !errors.Is(err, os.ErrNotExist) {
		return wire.Result{}, err
	}
	if err := saveIdentity(staging, wanted); err != nil {
		return wire.Result{}, err
	}
	if ctx.Err() != nil {
		return wire.Result{}, ctx.Err()
	}
	fd, err := parent.Open(".")
	if err != nil {
		return wire.Result{}, err
	}
	defer fd.Close()
	if err := PublishDirectory(int(fd.Fd()), stage, name); err != nil {
		return wire.Result{}, fmt.Errorf("checkout publication refuses to replace existing work: %w", err)
	}
	if err := fd.Sync(); err != nil {
		return wire.Result{}, err
	}
	return wire.Result{Path: filepath.Join(plan.WorkDir, name), BranchName: branch}, nil
}

func (p *Publisher) reuseCheckout(ctx context.Context, existing *os.Root, path string, wanted checkoutIdentity, hook []byte) (wire.Result, error) {
	if err := standalone(existing); err != nil {
		return wire.Result{}, err
	}
	var have checkoutIdentity
	raw, err := plainRead(existing, checkoutRecord)
	if err != nil || json.Unmarshal(raw, &have) != nil || have.SchemaVersion != 1 || have.URL != wanted.URL || have.Ref != wanted.Ref {
		return wire.Result{}, errors.New("existing repository has no matching checkout authority")
	}
	if err := reconcileCheckoutHook(existing, have, wanted, hook); err != nil {
		return wire.Result{}, err
	}
	branch, err := p.checkoutBranch(ctx, path)
	if err != nil {
		return wire.Result{}, err
	}
	return wire.Result{Path: path, BranchName: branch}, nil
}

func reconcileCheckoutHook(existing *os.Root, have, wanted checkoutIdentity, hook []byte) error {
	if have.HookDigest == wanted.HookDigest {
		return nil
	}
	current, err := readHook(existing)
	if err != nil {
		return err
	}
	currentDigest := ""
	if len(current) > 0 {
		currentDigest = wire.Digest(current)
	}
	if currentDigest != "" && currentDigest != have.HookDigest && currentDigest != wanted.HookDigest {
		return errors.New("official coauthor update conflicts with a user hook")
	}
	if len(hook) == 0 {
		if err := existing.Remove(".git/hooks/prepare-commit-msg"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if err := replace(existing, ".git/hooks/prepare-commit-msg", hook, 0755); err != nil {
		return err
	}
	return saveIdentity(existing, wanted)
}

func (p *Publisher) checkoutBranch(ctx context.Context, path string) (string, error) {
	command := exec.CommandContext(ctx, "git", "-c", "core.hooksPath=/dev/null", "-C", path, "symbolic-ref", "--quiet", "--short", "HEAD")
	command.Env = p.env
	raw, err := command.Output()
	if err == nil {
		return strings.TrimSpace(string(raw)), nil
	}
	command = exec.CommandContext(ctx, "git", "-c", "core.hooksPath=/dev/null", "-C", path, "rev-parse", "--verify", "HEAD^{commit}")
	command.Env = p.env
	if err := command.Run(); err != nil {
		return "", errors.New("existing repository HEAD is invalid")
	}
	return "", nil
}

func saveIdentity(root *os.Root, value checkoutIdentity) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return replace(root, checkoutRecord, raw, 0600)
}
func readHook(root *os.Root) ([]byte, error) {
	raw, err := plainRead(root, ".git/hooks/prepare-commit-msg")
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return raw, err
}
func plainRead(root *os.Root, name string) ([]byte, error) {
	parts := strings.Split(filepath.ToSlash(name), "/")
	for i := range parts {
		st, err := root.Lstat(strings.Join(parts[:i+1], "/"))
		if err != nil {
			return nil, err
		}
		if i < len(parts)-1 && !st.IsDir() || i == len(parts)-1 && !st.Mode().IsRegular() {
			return nil, errors.New("checkout metadata must be regular and confined")
		}
	}
	return root.ReadFile(name)
}
func replace(root *os.Root, name string, data []byte, mode os.FileMode) error {
	if err := plainDirectories(root, filepath.ToSlash(filepath.Dir(name)), 0755); err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(name), ".write-"+uuid.NewString())
	f, err := root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer root.Remove(tmp)
	_, err = f.Write(data)
	if err := errors.Join(err, f.Sync(), f.Close()); err != nil {
		return err
	}
	return root.Rename(tmp, name)
}
