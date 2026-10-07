package repocache

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This fixture runs the real Git smart-HTTPS server and client, including TLS
// verification, DNS pinning, refs, object negotiation, and on-disk persistence.
type gitFixture struct {
	root, repo, remoteURL string
	server                *httptest.Server
	bytes                 atomic.Int64
	mu                    sync.Mutex
	gate                  func(*http.Request)
	redirect              string
}

type countedWriter struct {
	http.ResponseWriter
	total *atomic.Int64
}

func (w countedWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.total.Add(int64(n))
	return n, err
}

func nativeGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.test", "GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.test")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func newGitFixture(t *testing.T, format string) *gitFixture {
	t.Helper()
	f := &gitFixture{root: t.TempDir()}
	f.repo = filepath.Join(f.root, "repo.git")
	nativeGit(t, f.root, "init", "--bare", "--initial-branch=main", "--object-format="+format, f.repo)
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	backend := &cgi.Handler{Path: gitPath, Args: []string{"http-backend"}, Env: []string{"GIT_PROJECT_ROOT=" + f.root, "GIT_HTTP_EXPORT_ALL=1"}}
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		gate := f.gate
		redirect := f.redirect
		f.mu.Unlock()
		if redirect != "" {
			http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
			return
		}
		if gate != nil {
			gate(r)
		}
		backend.ServeHTTP(countedWriter{w, &f.bytes}, r)
	}))
	t.Cleanup(f.server.Close)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", ca)
	f.remoteURL = f.server.URL + "/repo.git"
	return f
}

func (f *gitFixture) commit(t *testing.T, branch, content string, orphan bool) string {
	t.Helper()
	work := t.TempDir()
	format := nativeGit(t, f.repo, "rev-parse", "--show-object-format")
	nativeGit(t, work, "init", "--initial-branch="+branch, "--object-format="+format)
	if !orphan {
		cmd := exec.Command("git", "--git-dir="+f.repo, "rev-parse", "--verify", "refs/heads/"+branch)
		if cmd.Run() == nil {
			nativeGit(t, work, "fetch", f.repo, branch)
			nativeGit(t, work, "reset", "--hard", "FETCH_HEAD")
		}
	}
	if err := os.WriteFile(filepath.Join(work, "content"), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, work, "add", "content")
	nativeGit(t, work, "commit", "-m", "fixture")
	commit := nativeGit(t, work, "rev-parse", "HEAD")
	nativeGit(t, work, "push", "--force", f.repo, "HEAD:refs/heads/"+branch)
	return commit
}

func cacheRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "repositories")
}

// The cache fixture deliberately enters below the public-address admission
// boundary to exercise real local HTTPS Git storage without a production
// private-network bypass. Public Snapshot denial is tested separately.
type fixtureCache struct {
	*Manager
	config []string
}

func (m *fixtureCache) Snapshot(ctx context.Context, req Request) (*Snapshot, error) {
	u, err := normalizeURL(req.URL)
	if err != nil {
		return nil, err
	}
	return m.snapshot(ctx, req, remote{url: u.String(), config: m.config})
}

func openCache(t *testing.T, root string) *fixtureCache {
	t.Helper()
	m, err := New(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return &fixtureCache{Manager: m}
}

func request(f *gitFixture) Request {
	return Request{WorkspaceID: "1311dc96-11a4-41bf-8103-1ef0694476c5", URL: f.remoteURL, Authority: "anonymous"}
}

func TestPersistentCacheRefreshesObjectsAndReopensIndependentCheckout(t *testing.T) {
	f := newGitFixture(t, "sha1")
	content := make([]byte, 256<<10)
	_, _ = rand.New(rand.NewSource(7)).Read(content)
	f.commit(t, "main", string(content), false)
	root := cacheRoot(t)
	m := openCache(t, root)
	req := request(f)
	started := time.Now()
	first, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	coldDuration := time.Since(started)
	coldBytes := f.bytes.Load()
	first.Close()
	f.mu.Lock()
	f.redirect = "https://denied.invalid/repo.git"
	f.mu.Unlock()
	if s, err := m.Snapshot(context.Background(), req); err == nil {
		s.Close()
		t.Fatal("failed refresh returned stale checkout")
	}
	f.mu.Lock()
	f.redirect = ""
	f.mu.Unlock()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	commit := f.commit(t, "main", string(content)+"new remote content", false)
	m = openCache(t, root)
	before := f.bytes.Load()
	started = time.Now()
	second, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	warmDuration := time.Since(started)
	warmBytes := f.bytes.Load() - before
	if nativeGit(t, second.Root.Name(), "rev-parse", "HEAD") != commit {
		t.Fatal("new remote commit was not checked out")
	}
	if warmBytes >= coldBytes {
		t.Fatalf("warm fetch re-downloaded full history: cold=%d warm=%d", coldBytes, warmBytes)
	}
	second.Close()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m = openCache(t, root)
	third, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	for _, e := range m.entries {
		if err := os.RemoveAll(filepath.Join(e.path, "repository.git")); err != nil {
			t.Fatal(err)
		}
	}
	nativeGit(t, third.Root.Name(), "fsck", "--full")
	got, err := third.Root.ReadFile("content")
	if err != nil || string(got) != string(content)+"new remote content" {
		t.Fatal("canonical checkout depended on the deleted bare cache")
	}
	t.Logf("local HTTPS fixture: cold=%d upstream bytes/%s; warm=%d upstream bytes/%s; independent checkout reopened across restart", coldBytes, coldDuration, warmBytes, warmDuration)
}

func TestRefRefreshPreventsStaleObjectDisclosure(t *testing.T) {
	f := newGitFixture(t, "sha1")
	old := f.commit(t, "main", "private old incarnation", true)
	oldBlob := nativeGit(t, f.repo, "rev-parse", old+":content")
	nativeGit(t, f.repo, "tag", "old", old)
	m := openCache(t, cacheRoot(t))
	req := request(f)
	b, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	newCommit := f.commit(t, "replacement", "new public incarnation", true)
	nativeGit(t, f.repo, "symbolic-ref", "HEAD", "refs/heads/replacement")
	nativeGit(t, f.repo, "update-ref", "-d", "refs/heads/main")
	nativeGit(t, f.repo, "tag", "-d", "old")
	req.Ref = old
	if b, err := m.Snapshot(context.Background(), req); err == nil {
		b.Close()
		t.Fatal("warm cache disclosed unreachable former object")
	}
	req.Ref = "HEAD"
	b, err = m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	work := b.Root.Name()
	if nativeGit(t, b.Root.Name(), "rev-parse", "HEAD") != newCommit {
		t.Fatal("remote default-branch change was not observed")
	}
	cmd := exec.Command("git", "cat-file", "-e", old)
	cmd.Dir = work
	if cmd.Run() == nil {
		t.Fatal("new checkout included unreachable old content")
	}
	cmd = exec.Command("git", "cat-file", "-e", oldBlob)
	cmd.Dir = work
	if cmd.Run() == nil {
		t.Fatal("new checkout disclosed an old unreachable file blob")
	}
}

func TestSHA256AndRevisionExpressions(t *testing.T) {
	f := newGitFixture(t, "sha256")
	old := f.commit(t, "main", "first", false)
	f.commit(t, "main", "second", false)
	m := openCache(t, cacheRoot(t))
	req := request(f)
	for _, ref := range []string{"main~1", "origin/main~1", "refs/remotes/origin/main~1"} {
		req.Ref = ref
		b, err := m.Snapshot(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		work := b.Root.Name()
		b.Close()
		if nativeGit(t, work, "rev-parse", "HEAD") != old {
			t.Fatal("revision did not import the requested SHA256 commit")
		}
	}
}

func TestWorkspaceAndAuthorityCannotReuseRevokedContent(t *testing.T) {
	f := newGitFixture(t, "sha1")
	old := f.commit(t, "main", "old private data", true)
	m := openCache(t, cacheRoot(t))
	req := request(f)
	b, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	f.commit(t, "main", "new authority data", true)
	for _, different := range []Request{
		{WorkspaceID: "2311dc96-11a4-41bf-8103-1ef0694476c5", URL: req.URL, Authority: req.Authority, Ref: old},
		{WorkspaceID: req.WorkspaceID, URL: req.URL, Authority: "new-installation", Ref: old},
	} {
		if b, err := m.Snapshot(context.Background(), different); err == nil {
			b.Close()
			t.Fatal("different authority obtained old cached content")
		}
	}
}

func TestRejectsPrivateNetworkAndUnsafeURLs(t *testing.T) {
	f := newGitFixture(t, "sha1")
	f.commit(t, "main", "private", false)
	root := cacheRoot(t)
	m := openCache(t, root)
	if b, err := m.Manager.Snapshot(context.Background(), request(f)); err == nil {
		b.Close()
		t.Fatal("public Git policy reached loopback")
	}
	for _, raw := range []string{"file:///etc/passwd", "http://github.com/repo", "https://user:secret@github.com/repo", "https://github.com/repo?token=secret", "https://github.com/repo#fragment"} {
		r := request(f)
		r.URL = raw
		if b, err := m.Manager.Snapshot(context.Background(), r); err == nil {
			b.Close()
			t.Fatal("unsafe URL reached cache")
		}
	}
	m.lookupIP = func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("127.0.0.1")}, nil
	}
	r := request(f)
	r.URL = "https://example.com/repo.git"
	if b, err := m.Manager.Snapshot(context.Background(), r); err == nil {
		b.Close()
		t.Fatal("mixed public/private DNS answers reached Git")
	}
}

func TestResolvedAddressIsPinnedAndRedirectCannotEscape(t *testing.T) {
	f := newGitFixture(t, "sha1")
	f.commit(t, "main", "pinned", false)
	u, _ := url.Parse(f.remoteURL)
	u.Host = "example.com:" + u.Port()
	root := cacheRoot(t)
	m := openCache(t, root)
	m.config = []string{"http.curloptResolve=example.com:" + u.Port() + ":127.0.0.1"}
	req := request(f)
	req.URL = u.String()
	b, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	// A real HTTPS redirect must not fetch its target, even when that target
	// is another otherwise reachable Git service.
	hits := atomic.Int64{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer target.Close()
	f.mu.Lock()
	f.redirect = target.URL
	f.mu.Unlock()
	if b, err := m.Snapshot(context.Background(), req); err == nil {
		b.Close()
		t.Fatal("redirected repository was accepted")
	}
	if hits.Load() != 0 {
		t.Fatal("Git followed a redirect outside pinned HTTPS origin")
	}
}

func TestCancellingOneWaiterDoesNotCancelSharedColdRefresh(t *testing.T) {
	f := newGitFixture(t, "sha1")
	commit := f.commit(t, "main", "shared", false)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.mu.Lock()
	f.gate = func(r *http.Request) { once.Do(func() { close(started); <-release }) }
	f.mu.Unlock()
	m := openCache(t, cacheRoot(t))
	req := request(f)
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		b, err := m.Snapshot(ctx, req)
		if b != nil {
			b.Close()
		}
		first <- err
	}()
	<-started
	second := make(chan *Snapshot, 1)
	secondErr := make(chan error, 1)
	go func() { b, err := m.Snapshot(context.Background(), req); second <- b; secondErr <- err }()
	// Stage both waiters before cancelling one; otherwise the new final-waiter
	// cancellation rule correctly aborts the first, still-unshared refresh.
	deadline := time.After(10 * time.Second)
	for {
		m.mu.Lock()
		shared := false
		for _, e := range m.entries {
			if e.flight != nil && e.flight.waiters == 2 {
				shared = true
			}
		}
		m.mu.Unlock()
		if shared {
			break
		}
		select {
		case <-deadline:
			t.Fatal("second request did not join the blocked refresh")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled task kept waiting on shared Git")
	}
	close(release)
	b := <-second
	if err := <-secondErr; err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if nativeGit(t, b.Root.Name(), "rev-parse", "HEAD") != commit {
		t.Fatal("another task's cancellation lost the shared clone")
	}
}

func TestCancellingFinalWaiterStopsSharedGitAndAllowsRetry(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("writer termination and orphan reaping are verified by the Linux runtime")
	}
	f := newGitFixture(t, "sha1")
	commit := f.commit(t, "main", "after cancellation", false)
	started, disconnected := make(chan struct{}), make(chan struct{})
	var first sync.Once
	f.mu.Lock()
	f.gate = func(r *http.Request) { first.Do(func() { close(started); <-r.Context().Done(); close(disconnected) }) }
	f.mu.Unlock()
	m := openCache(t, cacheRoot(t))
	req := request(f)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		b, err := m.Snapshot(ctx, req)
		if b != nil {
			b.Close()
		}
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal("last cancelled task kept waiting for Git")
	}
	select {
	case <-disconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("Git connection survived cancellation of its last waiter")
	}
	b, err := m.Snapshot(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if nativeGit(t, b.Root.Name(), "rev-parse", "HEAD") != commit {
		t.Fatal("cancelled refresh prevented a subsequent valid checkout")
	}
}

func TestRecoveryDoesNotFollowCorruptCacheLinks(t *testing.T) {
	f := newGitFixture(t, "sha1")
	f.commit(t, "main", "safe repository", false)
	root := cacheRoot(t)
	m := openCache(t, root)
	req := request(f)
	b, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "private-data")
	if err := os.WriteFile(outside, []byte("must be preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, e := range m.entries {
		config := filepath.Join(e.path, "repository.git", "config")
		if err := os.Remove(config); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, config); err != nil {
			t.Fatal(err)
		}
	}
	m = openCache(t, root)
	b, err = m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	nativeGit(t, b.Root.Name(), "fsck", "--full")
	got, err := os.ReadFile(outside)
	if err != nil || string(got) != "must be preserved" {
		t.Fatal("cache recovery followed and overwrote an external link")
	}
}

func TestOriginRevisionKeepsCloneMeaningWhenBranchNamesCollide(t *testing.T) {
	f := newGitFixture(t, "sha1")
	f.commit(t, "main", "main branch", false)
	collision := f.commit(t, "origin/main", "branch named origin/main", true)
	m := openCache(t, cacheRoot(t))
	req := request(f)
	check := func(ref string) {
		clone := filepath.Join(t.TempDir(), "clone")
		nativeGit(t, f.root, "clone", "--no-hardlinks", f.repo, clone)
		want := nativeGit(t, clone, "-c", "core.warnAmbiguousRefs=false", "rev-parse", "--verify", ref+"^{commit}")
		req.Ref = ref
		b, err := m.Snapshot(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		if nativeGit(t, b.Root.Name(), "rev-parse", "HEAD") != want {
			t.Fatal("cached revision selected a different branch than an independent clone")
		}
	}
	check("origin/main")
	check("refs/remotes/origin/main")
	nativeGit(t, f.repo, "tag", "origin/main", collision)
	check("origin/main")
	nativeGit(t, f.repo, "tag", "-d", "origin/main")
	nativeGit(t, f.repo, "symbolic-ref", "HEAD", "refs/heads/origin/main")
	check("origin/main")
}

func TestExplicitRevisionWorksWithoutRemoteDefaultBranch(t *testing.T) {
	f := newGitFixture(t, "sha1")
	commit := f.commit(t, "main", "explicit branch", false)
	nativeGit(t, f.repo, "symbolic-ref", "HEAD", "refs/heads/unborn")
	m := openCache(t, cacheRoot(t))
	req := request(f)
	req.Ref = "main"
	b, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	work := b.Root.Name()
	if nativeGit(t, work, "rev-parse", "HEAD") != commit {
		t.Fatal("missing default branch prevented explicit checkout")
	}
	req.Ref = "HEAD"
	if b, err := m.Snapshot(context.Background(), req); err == nil {
		b.Close()
		t.Fatal("unborn remote HEAD resolved to an invented commit")
	}
}

func TestEmptyRemoteCannotPublishCheckout(t *testing.T) {
	f := newGitFixture(t, "sha1")
	m := openCache(t, cacheRoot(t))
	if b, err := m.Snapshot(context.Background(), request(f)); err == nil {
		b.Close()
		t.Fatal("empty repository published a checkout")
	}
}

func TestRestartRecoversAnInterruptedGitRefTransaction(t *testing.T) {
	f := newGitFixture(t, "sha1")
	f.commit(t, "main", "before interruption", false)
	root := cacheRoot(t)
	m := openCache(t, root)
	req := request(f)
	b, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	for _, e := range m.entries {
		lock := filepath.Join(e.path, "repository.git", "refs", "heads", "main.lock")
		if err := os.WriteFile(lock, []byte("interrupted update"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	commit := f.commit(t, "main", "after restart", false)
	m = openCache(t, root)
	b, err = m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	work := b.Root.Name()
	if nativeGit(t, work, "rev-parse", "HEAD") != commit {
		t.Fatal("interrupted Git transaction blocked or reverted the next checkout")
	}
}

func TestRecoveredCacheRebuildsHashCorruptPersistedObjects(t *testing.T) {
	f := newGitFixture(t, "sha1")
	want := "content protected by its Git object hash"
	f.commit(t, "main", want, false)
	root := cacheRoot(t)
	m := openCache(t, root)
	req := request(f)
	b, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	for _, e := range m.entries {
		repo := filepath.Join(e.path, "repository.git")
		blob := nativeGit(t, repo, "rev-parse", "HEAD:content")
		packs, err := filepath.Glob(filepath.Join(repo, "objects", "pack", "*.pack"))
		if err != nil {
			t.Fatal(err)
		}
		// Materialize real persisted loose objects, then corrupt one with a
		// syntactically valid zlib object whose contents no longer match its ID.
		for _, pack := range packs {
			raw, err := os.ReadFile(pack)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(pack); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(strings.TrimSuffix(pack, ".pack") + ".idx"); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("git", "unpack-objects")
			cmd.Dir = repo
			cmd.Stdin = bytes.NewReader(raw)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("unpack fixture: %v: %s", err, out)
			}
		}
		path := filepath.Join(repo, "objects", blob[:2], blob[2:])
		var encoded bytes.Buffer
		writer := zlib.NewWriter(&encoded)
		bad := "silently corrupted persisted content"
		if _, err := fmt.Fprintf(writer, "blob %d%c%s", len(bad), 0, bad); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		// Connectivity alone misses this failure: all named objects still exist.
		nativeGit(t, repo, "fsck", "--connectivity-only", "--no-dangling")
	}
	m = openCache(t, root)
	b, err = m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	work := b.Root.Name()
	got, err := os.ReadFile(filepath.Join(work, "content"))
	if err != nil || string(got) != want {
		t.Fatal("corrupt persisted Git objects survived cache recovery")
	}
}

func TestPinnedCheckoutKeepsItsRefsWhileNewRemoteStateIsPublished(t *testing.T) {
	f := newGitFixture(t, "sha1")
	main := f.commit(t, "main", "main content", false)
	feature := f.commit(t, "feature", "old feature", true)
	nativeGit(t, f.repo, "tag", "release", feature)
	m := openCache(t, cacheRoot(t))
	req := request(f)
	first, err := m.Snapshot(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	updated := f.commit(t, "feature", "updated feature", false)
	nativeGit(t, f.repo, "tag", "-f", "release", updated)
	second, err := m.Snapshot(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if nativeGit(t, second.Root.Name(), "rev-parse", "HEAD") != main {
		t.Fatal("another branch update changed the selected checkout commit")
	}
	if nativeGit(t, second.Root.Name(), "rev-parse", "refs/remotes/origin/feature") != updated || nativeGit(t, second.Root.Name(), "rev-parse", "refs/tags/release") != updated {
		t.Fatal("canonical snapshot missed updated remote refs")
	}
	if nativeGit(t, first.Root.Name(), "rev-parse", "refs/remotes/origin/feature") != feature || nativeGit(t, first.Root.Name(), "rev-parse", "refs/tags/release") != feature {
		t.Fatal("refresh changed a pinned checkout while its caller was reading")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := first.Root.ReadFile("content"); err != nil || string(got) != "main content" {
		t.Fatal("manager shutdown released a live snapshot reader")
	}
	first.Close()
	nativeGit(t, second.Root.Name(), "fsck", "--full")
}

func TestRecoveredCanonicalWorkingFileCorruptionIsRebuilt(t *testing.T) {
	f := newGitFixture(t, "sha1")
	f.commit(t, "main", "original", false)
	root := cacheRoot(t)
	m := openCache(t, root)
	req := request(f)
	first, err := m.Snapshot(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(first.Root.Name(), "content")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered"), info.Mode().Perm()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	m = openCache(t, root)
	second, err := m.Snapshot(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if got, err := second.Root.ReadFile("content"); err != nil || string(got) != "original" {
		t.Fatal("persisted working-file corruption reached the task snapshot")
	}
}
