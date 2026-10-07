package repocache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/google/uuid"
)

type checkoutRecord struct {
	Version    int    `json:"version"`
	State      string `json:"state"`
	TreeSHA256 string `json:"tree_sha256"`
}

type canonicalCheckout struct {
	state    string
	verified bool
	pins     int // Manager.mu, including every open Snapshot.Root
}

func (m *Manager) checkout(ctx context.Context, e *entry, requested, origin string) (*Snapshot, error) {
	repo := filepath.Join(e.path, "repository.git")
	if requested == "" {
		requested = "HEAD"
	}
	raw, err := m.resolveCommit(ctx, repo, requested)
	if err != nil {
		return nil, errors.New("requested Git revision does not resolve to a commit")
	}
	commit := strings.TrimSpace(string(raw))
	if !validOID(commit) {
		return nil, errors.New("resolved Git commit is invalid")
	}
	contains, err := m.git(ctx, repo, remote{}, nil, "for-each-ref", "--contains="+commit, "--format=%(refname)", "refs/heads/", "refs/tags/")
	if err != nil {
		return nil, err
	}
	if len(strings.TrimSpace(string(contains))) == 0 {
		if !e.hasHEAD {
			return nil, errors.New("requested Git commit is not reachable from current remote refs")
		}
		if _, err := m.git(ctx, repo, remote{}, nil, "merge-base", "--is-ancestor", commit, "HEAD"); err != nil {
			return nil, errors.New("requested Git commit is not reachable from current remote refs")
		}
	}
	refs, err := m.git(ctx, repo, remote{}, nil, "for-each-ref", "--sort=refname", "--format=%(objectname) %(refname)", "refs/heads/", "refs/tags/")
	if err != nil {
		return nil, err
	}
	var head, branch string
	if e.hasHEAD {
		raw, err := m.git(ctx, repo, remote{}, nil, "rev-parse", "--verify", "HEAD^{commit}")
		if err != nil {
			return nil, err
		}
		head = strings.TrimSpace(string(raw))
		if raw, err := m.git(ctx, repo, remote{}, nil, "symbolic-ref", "--quiet", "HEAD"); err == nil {
			branch = strings.TrimSpace(string(raw))
		}
	}
	// Every advertised branch/tag, default HEAD and selected commit participates
	// in reuse. Origin spelling remains exact for the task's retained checkout.
	sum := sha256.Sum256([]byte("checkout-v1\x00" + e.identity.ObjectFormat + "\x00" + origin + "\x00" + head + "\x00" + branch + "\x00" + commit + "\x00" + string(refs)))
	state := hex.EncodeToString(sum[:])
	parent := filepath.Join(e.path, "checkouts")
	path := filepath.Join(parent, state)
	if e.checkouts == nil {
		e.checkouts = make(map[string]*canonicalCheckout)
	}
	c := e.checkouts[state]
	if c == nil {
		c = &canonicalCheckout{state: state}
		e.checkouts[state] = c
	}
	if info, err := os.Lstat(path); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("canonical checkout path is not a real directory")
		}
		root, err := os.OpenRoot(path)
		if err != nil {
			return nil, err
		}
		valid := c.verified
		if !valid {
			raw, readErr := readMetadata(path+".json", nil)
			var record checkoutRecord
			if readErr == nil && json.Unmarshal(raw, &record) == nil && record.Version == 1 && record.State == state {
				digest, verifyErr := checkoutDigest(ctx, root)
				valid = verifyErr == nil && digest == record.TreeSHA256
			}
		}
		if valid {
			c.verified = true
			e.identity.Checkout = state
			result := m.pin(e, c, root)
			m.pruneCheckouts(e)
			return result, nil
		}
		_ = root.Close()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Existing readers only receive directories verified in this process;
		// an unverified persisted directory has no pins and may be rebuilt.
		if err := m.removeTree(path); err != nil {
			return nil, err
		}
		if err := m.removeFile(path + ".json"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, err
	}
	stage := filepath.Join(parent, ".stage-"+uuid.NewString())
	if err := os.Mkdir(stage, 0700); err != nil {
		return nil, err
	}
	defer m.removeTree(stage)
	if err := m.createCheckout(ctx, e, repo, stage, origin, commit, head, branch); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(stage)
	if err != nil {
		return nil, err
	}
	digest, err := checkoutDigest(ctx, root)
	_ = root.Close()
	if err != nil {
		return nil, err
	}
	if err := os.Rename(stage, path); err != nil {
		return nil, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = m.removeTree(path)
			_ = m.removeFile(path + ".json")
		}
	}()
	record, err := json.Marshal(checkoutRecord{Version: 1, State: state, TreeSHA256: digest})
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path+".json", record, 0600); err != nil {
		return nil, err
	}
	root, err = os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	c.verified = true
	e.identity.Checkout = state
	complete = true
	result := m.pin(e, c, root)
	m.pruneCheckouts(e)
	return result, nil
}

func (m *Manager) pin(e *entry, c *canonicalCheckout, root *os.Root) *Snapshot {
	m.mu.Lock()
	e.active++
	c.pins++
	m.mu.Unlock()
	return &Snapshot{Root: root, manager: m, entry: e, checkout: c}
}

// The repository lock is held by the caller. Old states are disposable once
// their last reader closes; only the most recently requested state is retained.
func (m *Manager) pruneCheckouts(e *entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.unproven || m.entries[cacheKey(e.identity)] != e {
		return
	}
	for state, c := range e.checkouts {
		if state == e.identity.Checkout || c.pins != 0 {
			continue
		}
		path := filepath.Join(e.path, "checkouts", state)
		if err := os.RemoveAll(path); err != nil {
			continue
		}
		if err := os.Remove(path + ".json"); err != nil && !errors.Is(err, os.ErrNotExist) {
			continue
		}
		delete(e.checkouts, state)
	}
}

func (m *Manager) createCheckout(ctx context.Context, e *entry, repo, stage, origin, commit, head, branch string) error {
	if _, err := m.git(ctx, stage, remote{}, nil, "init", "--object-format="+e.identity.ObjectFormat, "."); err != nil {
		return err
	}
	// A real local upload-pack/fetch copies only current approved refs into an
	// empty object store. It never copies the bare directory, old objects,
	// alternates or hardlinks. The file protocol is confined to this trusted path.
	local := remote{config: []string{"protocol.file.allow=always", "protocol.https.allow=never"}}
	args := []string{"fetch", "--atomic", "--no-tags", "--no-write-fetch-head", "--no-recurse-submodules", "--", repo,
		"+refs/heads/*:refs/remotes/origin/*", "+refs/tags/*:refs/tags/*"}
	if head != "" {
		args = append(args, "+HEAD:refs/multica/default-head")
	}
	if _, err := m.git(ctx, stage, local, nil, args...); err != nil {
		return err
	}
	if branch != "" {
		remoteBranch := "refs/remotes/origin/" + strings.TrimPrefix(branch, "refs/heads/")
		if _, err := m.git(ctx, stage, remote{}, nil, "symbolic-ref", "refs/remotes/origin/HEAD", remoteBranch); err != nil {
			return err
		}
		if _, err := m.git(ctx, stage, remote{}, nil, "update-ref", branch, head); err != nil {
			return err
		}
	} else if head != "" {
		if _, err := m.git(ctx, stage, remote{}, nil, "update-ref", "refs/remotes/origin/HEAD", head); err != nil {
			return err
		}
	}
	if _, err := m.git(ctx, stage, remote{}, nil, "update-ref", "-d", "refs/multica/default-head"); err != nil {
		return err
	}
	if _, err := m.git(ctx, stage, remote{}, nil, "config", "remote.origin.url", origin); err != nil {
		return err
	}
	if _, err := m.git(ctx, stage, remote{}, nil, "config", "remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"); err != nil {
		return err
	}
	if _, err := m.git(ctx, stage, remote{}, nil, "checkout", "--detach", commit, "--"); err != nil {
		return err
	}
	_, err := m.git(ctx, stage, remote{}, nil, "fsck", "--full", "--no-dangling")
	return err
}

func (m *Manager) removeTree(path string) error {
	if m.writersUnproven() {
		return ErrWritersUnproven
	}
	return os.RemoveAll(path)
}

// A persisted checksum covers actual checkout bytes, permissions and link
// targets, including Git metadata. It also detects working-file corruption
// that a Git index stat cache or object-only fsck would miss after restart.
func checkoutDigest(ctx context.Context, root *os.Root) (string, error) {
	info, err := root.Lstat(".git")
	if err != nil || !info.IsDir() {
		return "", errors.New("canonical checkout requires its own Git directory")
	}
	hash := sha256.New()
	err = fs.WalkDir(root.FS(), ".", func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := root.Lstat(path)
		if err != nil {
			return err
		}
		if path == ".git/commondir" || strings.HasSuffix(path, "/objects/info/alternates") || strings.HasSuffix(path, "/objects/info/http-alternates") {
			return errors.New("canonical checkout references external Git objects")
		}
		fmt.Fprintf(hash, "%s\x00%d\x00%d\x00", path, info.Mode(), info.Size())
		switch {
		case info.IsDir():
			return nil
		case info.Mode()&os.ModeSymlink != 0:
			for _, part := range strings.Split(path, "/") {
				if part == ".git" {
					return errors.New("canonical Git metadata contains a symbolic link")
				}
			}
			if _, err := root.Stat(path); err != nil {
				return errors.New("canonical checkout link escapes its directory or is unresolved")
			}
			target, err := root.Readlink(path)
			if err != nil {
				return err
			}
			_, err = io.WriteString(hash, target)
			return err
		case info.Mode().IsRegular():
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || stat.Nlink != 1 {
				return errors.New("canonical checkout contains shared files")
			}
			file, err := root.Open(path)
			if err != nil {
				return err
			}
			_, err = io.Copy(hash, contextReader{ctx: ctx, reader: file})
			closeErr := file.Close()
			return errors.Join(err, closeErr)
		default:
			return errors.New("canonical checkout contains a special file")
		}
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
