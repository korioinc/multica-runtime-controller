package repocache

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

func (m *Manager) recover() error {
	children, err := os.ReadDir(m.root)
	if err != nil {
		return err
	}
	for _, child := range children {
		path := filepath.Join(m.root, child.Name())
		if child.Name() == "transfers" {
			// Forward migration only: old transfer files are disposable and are
			// never read or resumed by the canonical checkout path.
			if err := os.RemoveAll(path); err != nil {
				return err
			}
			continue
		}
		workspace, err := uuid.Parse(child.Name())
		if err != nil || workspace.String() != child.Name() || !child.IsDir() {
			return errors.New("repository cache root contains an unrecognized entry")
		}
		repositories, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		for _, repository := range repositories {
			repoPath := filepath.Join(path, repository.Name())
			key, err := hex.DecodeString(repository.Name())
			if err != nil || len(key) != 32 || !repository.IsDir() {
				return errors.New("repository cache workspace contains an unrecognized entry")
			}
			var id identity
			treeErr := validateTree(filepath.Join(repoPath, "repository.git"))
			raw, readErr := readMetadata(filepath.Join(repoPath, "metadata.json"), treeErr)
			valid := readErr == nil && len(raw) <= 16384 && json.Unmarshal(raw, &id) == nil && id.Version == 1 && id.WorkspaceID == child.Name() && id.Authority != ""
			if valid {
				u, urlErr := normalizeURL(id.URL)
				valid = urlErr == nil && u.String() == id.URL && cacheKey(id) == filepath.Join(child.Name(), repository.Name())
			}
			if !valid {
				// Only an entry with our workspace/hash naming can be a recoverable
				// partial cache. Unknown siblings are never silently deleted.
				if err := os.RemoveAll(repoPath); err != nil {
					return err
				}
				continue
			}
			if err := removeInterruptedGitFiles(filepath.Join(repoPath, "repository.git")); err != nil {
				return err
			}
			e := &entry{identity: id, path: repoPath, lastUsed: id.LastUsed, verifyHashes: true}
			if err := recoverCheckouts(e); err != nil {
				return err
			}
			m.entries[cacheKey(id)] = e
		}
	}
	return nil
}

func recoverCheckouts(e *entry) error {
	parent := filepath.Join(e.path, "checkouts")
	info, err := os.Lstat(parent)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("canonical checkouts require a real parent directory")
	}
	children, err := os.ReadDir(parent)
	if err != nil {
		return err
	}
	for _, child := range children {
		name := child.Name()
		stage := false
		if strings.HasPrefix(name, ".stage-") {
			_, err := uuid.Parse(strings.TrimPrefix(name, ".stage-"))
			stage = err == nil
		}
		state := strings.TrimSuffix(name, ".json")
		key, err := hex.DecodeString(state)
		if !stage && (err != nil || len(key) != 32) {
			return errors.New("canonical checkout directory contains an unrecognized entry")
		}
		if !stage && state == e.identity.Checkout {
			continue
		}
		if err := os.RemoveAll(filepath.Join(parent, name)); err != nil {
			return err
		}
	}
	return nil
}

// Git may leave lock files or unfinished packs after SIGKILL. This is used only
// at controller recovery or under an entry lock after all Git writers stopped.
func removeInterruptedGitFiles(repo string) error {
	return filepath.WalkDir(repo, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errCorrupt
		}
		if d.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(repo, path)
		if err != nil {
			return err
		}
		name := d.Name()
		if strings.HasSuffix(name, ".lock") || strings.HasPrefix(filepath.ToSlash(relative), "objects/") && (strings.HasPrefix(name, "tmp_pack_") || strings.HasPrefix(name, "tmp_obj_")) {
			return os.Remove(path)
		}
		return nil
	})
}

func readMetadata(path string, treeErr error) ([]byte, error) {
	if treeErr != nil {
		return nil, treeErr
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 || stat.Uid != uint32(os.Getuid()) {
		return nil, errors.New("cache metadata must be a controller-owned regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(io.LimitReader(file, 16385))
}

func validateTree(root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("repository cache contains a symbolic link")
		}
		if !d.IsDir() {
			if !d.Type().IsRegular() {
				return errors.New("repository cache contains a non-regular file")
			}
			info, err := d.Info()
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || stat.Nlink != 1 || stat.Uid != uint32(os.Getuid()) {
				return errors.New("repository cache file ownership or link count is invalid")
			}
		}
		return nil
	})
}

// checkSpace inspects actual free blocks and inodes; it does not reserve space
// or impose a repository quota on the shared filesystem.
func (m *Manager) checkSpace() error {
	var stat unix.Statfs_t
	if err := unix.Statfs(m.root, &stat); err != nil {
		return err
	}
	if stat.Bavail == 0 || stat.Files > 0 && stat.Ffree == 0 {
		return syscall.ENOSPC
	}
	return nil
}

func (m *Manager) reclaimSpace() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reclaimSpaceLocked()
}

func (m *Manager) reclaimSpaceLocked() error {
	if m.unproven {
		return ErrWritersUnproven
	}
	if err := m.checkSpace(); !errors.Is(err, syscall.ENOSPC) {
		return err
	}
	var inactive []*entry
	for _, e := range m.entries {
		if e.active == 0 {
			inactive = append(inactive, e)
		}
	}
	sort.Slice(inactive, func(i, j int) bool { return inactive[i].lastUsed.Before(inactive[j].lastUsed) })
	for {
		err := m.checkSpace()
		if !errors.Is(err, syscall.ENOSPC) {
			return err
		}
		if len(inactive) == 0 {
			return err
		}
		e := inactive[0]
		inactive = inactive[1:]
		if err := os.RemoveAll(e.path); err != nil {
			return err
		}
		delete(m.entries, cacheKey(e.identity))
	}
}

// operation limits active Git work to available controller execution capacity.
// Actual filesystem exhaustion cancels the child before returning an error.
func (m *Manager) operation(ctx context.Context, fn func(context.Context) error) error {
	select {
	case m.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-m.slots }()
	if err := m.reclaimSpace(); err != nil {
		return err
	}
	opctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopped := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-opctx.Done():
				return
			case <-ticker.C:
				if err := m.checkSpace(); err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	err := fn(opctx)
	close(done)
	<-stopped
	if m.writersUnproven() {
		return ErrWritersUnproven
	}
	if cause := context.Cause(opctx); cause != nil {
		err = cause
	}
	if err == nil {
		err = m.checkSpace()
	}
	return err
}
