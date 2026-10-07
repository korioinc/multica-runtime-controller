package repocache

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

var errCorrupt = errors.New("cached repository is corrupt")

func (m *Manager) update(ctx context.Context, e *entry, r remote) error {
	if err := os.MkdirAll(e.path, 0700); err != nil {
		return err
	}
	repo := filepath.Join(e.path, "repository.git")
	// Reconstruct only our known configuration before interpreting a persisted
	// repository. Credentials and remote URLs never enter the on-disk config.
	if e.identity.ObjectFormat != "" {
		if e.identity.ObjectFormat != "sha1" && e.identity.ObjectFormat != "sha256" {
			return errCorrupt
		}
		if err := cleanConfig(repo, e.identity.ObjectFormat); err != nil {
			return errors.Join(errCorrupt, err)
		}
		if e.verifyHashes {
			if _, err := m.git(ctx, repo, remote{}, nil, "fsck", "--full", "--no-dangling"); err != nil {
				if ctx.Err() != nil || m.writersUnproven() {
					return err
				}
				return errors.Join(errCorrupt, err)
			}
			e.verifyHashes = false
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		advertisement, err := m.git(ctx, m.root, r, nil, "ls-remote", "--symref", "--", r.url, "HEAD", "refs/heads/*", "refs/tags/*")
		if err != nil {
			return err
		}
		var head, branch, format string
		for _, line := range strings.Split(strings.TrimSpace(string(advertisement)), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 3 && fields[0] == "ref:" && fields[2] == "HEAD" {
				if !strings.HasPrefix(fields[1], "refs/heads/") {
					return errors.New("remote HEAD points outside branch refs")
				}
				branch = fields[1]
				continue
			}
			if len(fields) != 2 {
				continue
			}
			if !validOID(fields[0]) {
				return errors.New("Git remote advertised an invalid object")
			}
			current := "sha1"
			if len(fields[0]) == 64 {
				current = "sha256"
			}
			if format != "" && format != current {
				return errors.New("Git remote object formats disagree")
			}
			format = current
			if fields[1] == "HEAD" {
				head = fields[0]
			}
		}
		if format == "" {
			return errors.New("Git remote has no checkoutable commits")
		}
		if e.identity.ObjectFormat == "" {
			if _, err := m.git(ctx, e.path, remote{}, nil, "init", "--bare", "--object-format="+format, "--", repo); err != nil {
				return err
			}
			e.identity.ObjectFormat = format
			if err := cleanConfig(repo, format); err != nil {
				return err
			}
		} else if e.identity.ObjectFormat != format {
			return errCorrupt
		}
		fetchArgs := []string{"fetch", "--atomic", "--prune", "--prune-tags", "--no-tags", "--no-recurse-submodules", "--no-write-fetch-head", "--", r.url,
			"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}
		if head != "" {
			fetchArgs = append(fetchArgs, "+HEAD:refs/multica/remote-head")
		}
		if _, err := m.git(ctx, repo, r, nil, fetchArgs...); err != nil {
			if ctx.Err() == nil && !m.writersUnproven() {
				if _, verifyErr := m.git(ctx, repo, remote{}, nil, "fsck", "--connectivity-only", "--no-dangling"); verifyErr != nil {
					if ctx.Err() != nil || m.writersUnproven() {
						return verifyErr
					}
					return errors.Join(errCorrupt, err)
				}
			}
			return err
		}
		if head != "" {
			actual, err := m.git(ctx, repo, remote{}, nil, "rev-parse", "--verify", "--end-of-options", "refs/multica/remote-head^{commit}")
			if err != nil {
				return err
			}
			if strings.TrimSpace(string(actual)) != head {
				continue
			}
		}
		if head == "" {
			if _, err := m.git(ctx, repo, remote{}, nil, "symbolic-ref", "HEAD", "refs/heads/multica-unborn-"+uuid.NewString()); err != nil {
				return err
			}
		} else if branch != "" {
			actual, err := m.git(ctx, repo, remote{}, nil, "rev-parse", "--verify", "--end-of-options", branch+"^{commit}")
			if err != nil || strings.TrimSpace(string(actual)) != head {
				continue
			}
			if _, err := m.git(ctx, repo, remote{}, nil, "symbolic-ref", "HEAD", branch); err != nil {
				return err
			}
		} else {
			if _, err := m.git(ctx, repo, remote{}, nil, "update-ref", "--no-deref", "HEAD", head); err != nil {
				return err
			}
		}
		if _, err := m.git(ctx, repo, remote{}, nil, "update-ref", "-d", "refs/multica/remote-head"); err != nil {
			return err
		}
		if _, err := m.git(ctx, repo, remote{}, nil, "fsck", "--connectivity-only", "--no-dangling"); err != nil {
			if ctx.Err() != nil || m.writersUnproven() {
				return err
			}
			return errors.Join(errCorrupt, err)
		}
		e.verifyHashes = false
		e.hasHEAD = head != ""
		e.identity.LastUsed = time.Now().UTC()
		m.mu.Lock()
		e.lastUsed = e.identity.LastUsed
		m.mu.Unlock()
		return publishMetadata(e)
	}
	return errors.New("remote HEAD changed during repository refresh")
}

func cleanConfig(repo, format string) error {
	version := 0
	extensions := ""
	if format == "sha256" {
		version = 1
		extensions = "[extensions]\n\tobjectFormat = sha256\n"
	}
	return os.WriteFile(filepath.Join(repo, "config"), []byte(fmt.Sprintf("[core]\n\trepositoryFormatVersion = %d\n\tbare = true\n%s", version, extensions)), 0600)
}

func validOID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

func (m *Manager) resolveCommit(ctx context.Context, repo, requested string) ([]byte, error) {
	resolve := func(ref string) ([]byte, error) {
		return m.git(ctx, repo, remote{}, nil, "rev-parse", "--verify", "--end-of-options", ref+"^{commit}")
	}
	alias := ""
	if strings.HasPrefix(requested, "refs/remotes/origin/") {
		alias = strings.TrimPrefix(requested, "refs/remotes/origin/")
	} else if strings.HasPrefix(requested, "origin/") {
		// A normal clone resolves a tag first, then its sole local default
		// branch, then an origin tracking ref. A bare mirror has all branches
		// locally, so raw resolution can otherwise select origin/main's own
		// branch instead of main's tracking ref.
		if raw, err := resolve("refs/tags/" + requested); err == nil {
			return raw, nil
		}
		branch, err := m.git(ctx, repo, remote{}, nil, "symbolic-ref", "--quiet", "HEAD")
		local := strings.TrimPrefix(strings.TrimSpace(string(branch)), "refs/heads/")
		if err == nil && revisionOf(requested, local) {
			return resolve("refs/heads/" + requested)
		}
		alias = strings.TrimPrefix(requested, "origin/")
	}
	if alias != "" {
		if revisionOf(alias, "HEAD") {
			return resolve(alias)
		}
		return resolve("refs/heads/" + alias)
	}
	return resolve(requested)
}

func revisionOf(revision, ref string) bool {
	if revision == ref {
		return true
	}
	if !strings.HasPrefix(revision, ref) {
		return false
	}
	suffix := strings.TrimPrefix(revision, ref)
	return strings.HasPrefix(suffix, "~") || strings.HasPrefix(suffix, "^") || strings.HasPrefix(suffix, "@{")
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
