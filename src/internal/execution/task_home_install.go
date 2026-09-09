package execution

import (
	"archive/tar"
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"golang.org/x/sys/unix"
)

type taskHomeReceipt struct {
	Digest   string           `json:"digest"`
	Identity taskHomeIdentity `json:"identity"`
}

// InstallTaskHome publishes one verified tree and its completion receipt in the
// same rename. A completed attempt owns all later private edits and deletions.
// HOME is Pod-local scratch: close and verify its writes before publication
// without forcing disposable files to durable storage.
func InstallTaskHome(home, artifactPath string, request wire.Request) error {
	if _, err := homeIdentity(request); err != nil {
		return err
	}
	if !core.ValidSHA(request.HomeDigest) {
		return errors.New("task HOME digest is invalid")
	}
	parentPath, name := filepath.Dir(home), filepath.Base(home)
	canonical, err := filepath.EvalSymlinks(parentPath)
	if err != nil || canonical != parentPath || !filepath.IsAbs(home) || filepath.Clean(home) != home || home == "/" {
		return errors.New("task HOME requires a canonical private directory")
	}
	parent, err := os.OpenRoot(parentPath)
	if err != nil {
		return err
	}
	defer parent.Close()
	unlock, err := lockTaskHome(parent)
	if err != nil {
		return err
	}
	defer unlock()
	if err := removeInterruptedHomeStages(parent); err != nil {
		return err
	}
	if complete, err := installedTaskHome(parent, name, request); err != nil || complete {
		return err
	}
	input, err := openTaskHomeArtifact(artifactPath)
	if err != nil {
		return err
	}
	defer input.Close()
	stage := ".task-home-" + uuid.NewString()
	if err := parent.Mkdir(stage, 0700); err != nil {
		return err
	}
	defer parent.RemoveAll(stage)
	staged, err := parent.OpenRoot(stage)
	if err != nil {
		return err
	}
	defer staged.Close()
	if err := extractTaskHomeArchive(staged, input, request); err != nil {
		return err
	}
	if err := writeTaskHomeReceipt(staged, request); err != nil {
		return err
	}
	if complete, err := installedTaskHome(parent, name, request); err != nil || complete {
		return err
	}
	if err := publishInitialTaskHome(parent, stage, name); err != nil {
		// Another initializer may have published the same complete attempt.
		if complete, checkErr := installedTaskHome(parent, name, request); checkErr != nil || !complete {
			return errors.Join(err, checkErr)
		}
	}
	return nil
}

func publishInitialTaskHome(parent *os.Root, stage, name string) error {
	directory, err := parent.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	// os.Root.Rename refuses every existing directory, even an empty one.
	// Native rename atomically replaces the initial empty HOME and still
	// rejects a nonempty destination. Both names are private sibling leaves.
	return unix.Renameat(int(directory.Fd()), stage, int(directory.Fd()), name)
}

// The stable lock inode makes abandoned-stage cleanup safe across concurrent
// retries; process termination releases the lock without deleting its inode.
func lockTaskHome(parent *os.Root) (func(), error) {
	file, err := parent.OpenFile(".task-home.lock", os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if errors.Is(err, os.ErrExist) {
		file, err = parent.OpenFile(".task-home.lock", os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	}
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	linked, err := parent.Lstat(".task-home.lock")
	if err != nil || !linked.Mode().IsRegular() || !os.SameFile(info, linked) {
		file.Close()
		return nil, errors.New("task HOME initialization lock changed identity")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || owner.Uid != uint32(os.Geteuid()) || owner.Nlink != 1 {
		file.Close()
		return nil, errors.New("task HOME initialization lock has a foreign identity")
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		file.Close()
		return nil, err
	}
	return func() { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN); _ = file.Close() }, nil
}

func removeInterruptedHomeStages(parent *os.Root) error {
	entries, err := fs.ReadDir(parent.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		id, stage := strings.CutPrefix(entry.Name(), ".task-home-")
		if stage && wire.UUID(id) && entry.IsDir() {
			if err := parent.RemoveAll(entry.Name()); err != nil {
				return err
			}
		}
	}
	return nil
}

// CheckTaskHome verifies initialization identity without interpreting or
// replacing the provider's subsequent private state.
func CheckTaskHome(home string, request wire.Request) error {
	info, err := os.Lstat(home)
	if err != nil || !info.IsDir() {
		return errors.New("task HOME is not an initialized private directory")
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer root.Close()
	return checkTaskHomeReceipt(root, request)
}

func installedTaskHome(parent *os.Root, name string, request wire.Request) (bool, error) {
	info, err := parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !info.IsDir() {
		return false, errors.New("task HOME destination is not a real directory")
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return false, err
	}
	defer root.Close()
	if _, err := root.Lstat(taskHomeMarker); err == nil {
		return true, checkTaskHomeReceipt(root, request)
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return false, err
	}
	if len(entries) != 0 {
		return false, errors.New("nonempty task HOME has no completed initialization identity")
	}
	return false, nil
}

func checkTaskHomeReceipt(home *os.Root, request wire.Request) error {
	info, err := home.Lstat(taskHomeMarker)
	if err != nil || !info.Mode().IsRegular() || info.Size() > wire.MaxRequestBytes {
		return errors.New("task HOME initialization receipt is unavailable")
	}
	raw, err := home.ReadFile(taskHomeMarker)
	if err != nil {
		return err
	}
	var receipt taskHomeReceipt
	if err := runtimeimage.Decode(raw, &receipt); err != nil {
		return errors.New("task HOME initialization receipt is invalid")
	}
	if !core.ValidSHA(request.HomeDigest) || receipt.Digest != request.HomeDigest || !receipt.Identity.matches(request) {
		return errors.New("task HOME belongs to a different execution identity")
	}
	return nil
}

func writeTaskHomeReceipt(home *os.Root, request wire.Request) error {
	identity, err := homeIdentity(request)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(taskHomeReceipt{Digest: request.HomeDigest, Identity: identity})
	if err != nil {
		return err
	}
	file, err := home.OpenFile(taskHomeMarker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(raw)
	return errors.Join(err, file.Close())
}

func openTaskHomeArtifact(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("task HOME archive must be a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		file.Close()
		return nil, errors.New("task HOME archive changed while opening")
	}
	return file, nil
}

func extractTaskHomeArchive(home *os.Root, input io.Reader, request wire.Request) error {
	hash := sha256.New()
	verified := bufio.NewReaderSize(io.TeeReader(input, hash), 128<<10)
	archive := tar.NewReader(verified)
	header, err := archive.Next()
	if err != nil || header.Name != taskHomeIdentityFile || header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > wire.MaxRequestBytes {
		return errors.New("task HOME archive has no valid identity")
	}
	raw, err := io.ReadAll(archive)
	if err != nil {
		return err
	}
	var identity taskHomeIdentity
	if err := runtimeimage.Decode(raw, &identity); err != nil || !identity.matches(request) {
		return errors.New("task HOME archive belongs to a different execution identity")
	}
	if err := extractTaskHomeEntries(home, archive); err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, verified); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != request.HomeDigest {
		return diagnostics.Wrap("task_home_digest_mismatch", errors.New("task HOME archive differs from its authorized digest"))
	}
	return nil
}

func extractTaskHomeEntries(home *os.Root, archive *tar.Reader) error {
	seen := map[string]bool{}
	extractor := homeArchiveExtractor{
		home:        home,
		directories: newStagedDirectories(home),
		links:       map[string]string{},
		buffer:      make([]byte, 32<<10),
	}
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(header.Name, "/")
		if seen[name] {
			return errors.New("task HOME archive repeats a path")
		}
		if name == "home" && header.Typeflag == tar.TypeDir {
			seen[name] = true
			continue
		}
		path := strings.TrimPrefix(name, "home/")
		if !seen["home"] || name == path || !validTaskHomePath(path, header.Typeflag == tar.TypeDir) {
			return errors.New("task HOME archive contains an unconfined or reserved path")
		}
		seen[name] = true
		if err := extractor.writeEntry(archive, path, header); err != nil {
			return err
		}
	}
	if !seen["home"] {
		return errors.New("task HOME archive is missing its completed tree")
	}
	return extractor.publishLinks()
}

// Paths and file types are validated while writing this exclusively owned tree.
// Only command links need filesystem validation after extraction.
type homeArchiveExtractor struct {
	home        *os.Root
	directories *stagedDirectories
	links       map[string]string
	buffer      []byte
}

func (e *homeArchiveExtractor) writeEntry(archive *tar.Reader, path string, header *tar.Header) error {
	if header.Typeflag == tar.TypeDir {
		return e.directories.mkdirAll(path)
	}
	if err := e.directories.mkdirAll(filepath.Dir(path)); err != nil {
		return err
	}
	if header.Typeflag == tar.TypeSymlink {
		joined := filepath.Clean(filepath.Join(filepath.Dir(path), header.Linkname))
		if !npmCommandPath(path) || filepath.IsAbs(header.Linkname) || !strings.HasPrefix(joined, runtimeimage.PiNPMDirectory+"/") {
			return errors.New("task HOME archive contains an unapproved package link")
		}
		e.links[path] = header.Linkname
		return nil
	}
	if header.Typeflag != tar.TypeReg {
		return errors.New("task HOME archive contains a special file")
	}
	file, err := e.home.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600|os.FileMode(header.Mode)&0100)
	if err != nil {
		return err
	}
	// Hide File.ReadFrom so CopyBuffer reuses this archive's buffer instead of
	// allocating another copy buffer for every file.
	_, err = io.CopyBuffer(struct{ io.Writer }{file}, archive, e.buffer)
	return errors.Join(err, file.Close())
}

func (e *homeArchiveExtractor) publishLinks() error {
	// Links become visible only after all files, so archive paths can never
	// traverse an earlier link while writing another entry.
	for path, target := range e.links {
		if err := e.home.Symlink(target, path); err != nil {
			return err
		}
	}
	root := filepath.Join(e.home.Name(), runtimeimage.PiNPMDirectory)
	for path := range e.links {
		if err := runtimeimage.ValidateNPMCommandLink(root, filepath.Join(e.home.Name(), path)); err != nil {
			return err
		}
	}
	return nil
}
