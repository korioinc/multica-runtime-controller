// Package repocache owns private, persistent Git objects and immutable checkouts.
package repocache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
)

type Request struct {
	WorkspaceID, URL, Ref, Authority string
	Token                            githubapp.Token
}

type Snapshot struct {
	Root     *os.Root
	once     sync.Once
	manager  *Manager
	entry    *entry
	checkout *canonicalCheckout
	err      error
}

// Close releases this reader's pin. The immutable checkout remains reusable.
func (s *Snapshot) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		s.err = s.Root.Close()
		s.manager.mu.Lock()
		s.entry.active--
		s.checkout.pins--
		s.manager.mu.Unlock()
		if s.entry.mu.TryLock() {
			s.manager.pruneCheckouts(s.entry)
			s.entry.mu.Unlock()
		}
		_ = s.manager.reclaimSpace()
	})
	return s.err
}

type identity struct {
	Version      int       `json:"version"`
	WorkspaceID  string    `json:"workspace_id"`
	URL          string    `json:"url"`
	Authority    string    `json:"authority"`
	LastUsed     time.Time `json:"last_used"`
	ObjectFormat string    `json:"object_format"`
	Checkout     string    `json:"checkout,omitempty"`
}

type entry struct {
	mu           sync.Mutex
	identity     identity
	path         string
	active       int // protected by Manager.mu; includes shared refreshes
	lastUsed     time.Time
	flight       *refresh
	valid        bool // the most recent refresh completed with consistent refs
	hasHEAD      bool
	verifyHashes bool // recovered objects are fully checked once before reuse
	checkouts    map[string]*canonicalCheckout
}

type refresh struct {
	done    chan struct{}
	err     error
	ctx     context.Context
	cancel  context.CancelFunc
	waiters int
}

type Manager struct {
	root     string
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closed   bool
	unproven bool
	entries  map[string]*entry
	slots    chan struct{}
	wg       sync.WaitGroup
	lookupIP func(context.Context, string, string) ([]netip.Addr, error)
}

func New(ctx context.Context, root string) (*Manager, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, errors.New("repository cache root must be an absolute clean path")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("repository cache root must be a real directory")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) {
		return nil, errors.New("repository cache root must belong to the controller user")
	}
	if err := os.Chmod(root, 0700); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	m := &Manager{root: root, ctx: ctx, cancel: cancel, entries: make(map[string]*entry), slots: make(chan struct{}, runtime.GOMAXPROCS(0)), lookupIP: net.DefaultResolver.LookupNetIP}
	if err := m.recover(); err != nil {
		cancel()
		return nil, err
	}
	options, err := m.git(ctx, root, remote{}, nil, "help", "--config")
	if err != nil || !strings.Contains("\n"+string(options), "\nhttp.curloptResolve\n") {
		cancel()
		return nil, errors.New("repository cache requires Git with http.curloptResolve support")
	}
	return m, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	m.closed = true
	m.cancel()
	m.mu.Unlock()
	m.wg.Wait()
	if m.writersUnproven() {
		return ErrWritersUnproven
	}
	return nil
}

func cacheKey(id identity) string {
	digest := sha256.Sum256([]byte(id.URL + "\x00" + id.Authority))
	return filepath.Join(id.WorkspaceID, hex.EncodeToString(digest[:]))
}

// Snapshot authorizes network addresses anew, shares only an in-progress refresh,
// and returns a pinned immutable checkout. Callers must authorize the task
// and repository before every call, including cache hits.
func (m *Manager) Snapshot(ctx context.Context, req Request) (*Snapshot, error) {
	u, err := normalizeURL(req.URL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(m.ctx, cancel)
	defer stop()
	r, err := m.resolveRemote(ctx, u, req.Token)
	if err != nil {
		return nil, err
	}
	return m.snapshot(ctx, req, r)
}

// snapshot consumes only a remote whose address and credentials have already
// been authorized by Snapshot; it owns the cache and shared refresh lifetime.
func (m *Manager) snapshot(ctx context.Context, req Request, r remote) (*Snapshot, error) {
	workspace, err := uuid.Parse(req.WorkspaceID)
	if err != nil || workspace.String() != req.WorkspaceID {
		return nil, errors.New("repository cache requires a canonical workspace UUID")
	}
	if req.Authority == "" || len(req.Authority) > 1024 || strings.ContainsAny(req.Authority, "\x00\r\n") {
		return nil, errors.New("repository cache requires stable authority")
	}
	if len(req.Ref) > 4096 || strings.ContainsAny(req.Ref, "\x00\r\n") || strings.HasPrefix(req.Ref, "-") {
		return nil, errors.New("repository revision is invalid")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(m.ctx, cancel)
	defer stop()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id := identity{Version: 1, WorkspaceID: req.WorkspaceID, URL: r.url, Authority: req.Authority}
	key := cacheKey(id)
	m.mu.Lock()
	if m.closed || m.ctx.Err() != nil {
		m.mu.Unlock()
		return nil, errors.New("repository cache is closed")
	}
	m.wg.Add(1)
	defer m.wg.Done()
	if err := m.reclaimSpaceLocked(); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	e := m.entries[key]
	if e == nil {
		e = &entry{identity: id, path: filepath.Join(m.root, key)}
		m.entries[key] = e
	}
	e.active++
	defer func() { m.mu.Lock(); e.active--; m.mu.Unlock(); _ = m.reclaimSpace() }()
	m.mu.Unlock()
	if err := m.awaitRefresh(ctx, e, r, req.Token.ExpiresAt); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// One repository lock protects mutable refs and canonical checkout creation.
	if err := lockEntry(ctx, e); err != nil {
		return nil, err
	}
	defer e.mu.Unlock()
	if !e.valid {
		return nil, errors.New("repository changed during an unsuccessful refresh; retry checkout")
	}
	var result *Snapshot
	err = m.operation(ctx, func(opctx context.Context) error {
		var err error
		result, err = m.checkout(opctx, e, req.Ref, req.URL)
		return err
	})
	if err != nil {
		if result != nil {
			_ = result.Close()
		}
		return nil, err
	}
	m.mu.Lock()
	e.lastUsed = time.Now().UTC()
	e.identity.LastUsed = e.lastUsed
	m.mu.Unlock()
	if err := publishMetadata(e); err != nil {
		_ = result.Close()
		return nil, err
	}
	return result, nil
}

func lockEntry(ctx context.Context, e *entry) error {
	// TryLock avoids leaving a waiter goroutine behind when its task stops.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.mu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Manager) awaitRefresh(ctx context.Context, e *entry, r remote, expires time.Time) error {
	for {
		m.mu.Lock()
		if m.closed || ctx.Err() != nil {
			m.mu.Unlock()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.New("repository cache is closed")
		}
		f := e.flight
		if f != nil && f.ctx.Err() != nil {
			// Its final waiter cancelled it. Reap the old Git process before
			// allowing a new request to start a fresh update of this entry.
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-f.done:
				continue
			}
		}
		if f == nil {
			var shared context.Context
			var cancel context.CancelFunc
			if expires.IsZero() {
				shared, cancel = context.WithCancel(m.ctx)
			} else {
				shared, cancel = context.WithDeadline(m.ctx, expires)
			}
			f = &refresh{done: make(chan struct{}), ctx: shared, cancel: cancel}
			e.flight = f
			e.active++
			m.wg.Add(1)
			go m.refresh(e, r, f)
		}
		f.waiters++
		m.mu.Unlock()
		var err error
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-f.done:
			err = f.err
		}
		m.mu.Lock()
		f.waiters--
		if f.waiters == 0 {
			f.cancel()
		}
		m.mu.Unlock()
		return err
	}
}

func (m *Manager) refresh(e *entry, r remote, f *refresh) {
	defer m.wg.Done()
	defer f.cancel()
	ctx := f.ctx
	err := lockEntry(ctx, e)
	if err == nil {
		warm := e.identity.ObjectFormat != ""
		e.valid = false
		err = m.operation(ctx, func(opctx context.Context) error { return m.update(opctx, e, r) })
		if errors.Is(err, errCorrupt) && !m.writersUnproven() && ctx.Err() == nil {
			if removeErr := os.RemoveAll(filepath.Join(e.path, "repository.git")); removeErr == nil {
				e.identity.ObjectFormat = ""
				warm = false
				err = m.operation(ctx, func(opctx context.Context) error { return m.update(opctx, e, r) })
			}
		}
		if err != nil && !m.writersUnproven() && (!warm || errors.Is(err, errCorrupt)) {
			// A failed warm network refresh never authorizes stale refs, but its
			// valid object store remains useful for the next successful fetch.
			if removeErr := os.RemoveAll(filepath.Join(e.path, "repository.git")); removeErr == nil {
				e.identity.ObjectFormat = ""
			}
		} else if err != nil && !m.writersUnproven() {
			err = errors.Join(err, removeInterruptedGitFiles(filepath.Join(e.path, "repository.git")))
		}
		e.valid = err == nil
		m.pruneCheckouts(e)
		e.mu.Unlock()
	}
	m.mu.Lock()
	f.err = err
	e.flight = nil
	e.active--
	close(f.done)
	m.mu.Unlock()
	_ = m.reclaimSpace()
}

func (m *Manager) writersUnproven() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.unproven }

func (m *Manager) poison() {
	m.mu.Lock()
	m.unproven = true
	m.closed = true
	m.cancel()
	m.mu.Unlock()
}

func (m *Manager) removeFile(path string) error {
	if m.writersUnproven() {
		return ErrWritersUnproven
	}
	return os.Remove(path)
}

func publishMetadata(e *entry) error {
	raw, err := json.Marshal(e.identity)
	if err != nil {
		return err
	}
	path := filepath.Join(e.path, "metadata.json")
	if err := os.WriteFile(path+".tmp", raw, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
