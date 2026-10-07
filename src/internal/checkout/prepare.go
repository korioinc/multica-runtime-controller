package checkout

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/processgroup"
	"golang.org/x/sys/unix"
)

var ErrCheckoutWritersUnproven = errors.New("checkout process group termination is unproven")

// Retain validates an existing checkout without running its config, hooks, or
// filters. The caller checks current task authority before calling it.
func Retain(ctx context.Context, taskRoot, workdir, repositoryURL string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if err := validateURL(repositoryURL); err != nil {
		return "", false, err
	}
	name, err := DirectoryName(repositoryURL)
	if err != nil {
		return "", false, err
	}
	root, err := openCheckoutRoot(taskRoot, workdir)
	if err != nil {
		return "", false, err
	}
	defer root.Close()
	if _, err := root.Lstat(name); errors.Is(err, os.ErrNotExist) {
		return filepath.Join(workdir, name), false, nil
	} else if err != nil {
		return "", false, err
	}
	if _, err := root.Lstat(filepath.Join(name, ".git", pendingCheckout)); err == nil {
		return "", false, errors.New("repository checkout publication is incomplete")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	if err := verifyCheckout(ctx, root, name); err != nil {
		return "", false, err
	}
	config, err := root.OpenFile(filepath.Join(name, ".git/config"), os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return "", false, err
	}
	defer config.Close()
	current, err := readOrigin(ctx, config)
	if err != nil || strings.TrimSpace(string(current)) != repositoryURL {
		return "", false, errors.Join(errors.New("retained checkout belongs to another repository"), err)
	}
	return filepath.Join(workdir, name), true, nil
}

func validateURL(repositoryURL string) error {
	remote, err := url.Parse(repositoryURL)
	if err != nil || remote.Scheme != "https" || remote.Host == "" || remote.User != nil || remote.Opaque != "" || remote.RawQuery != "" || remote.Fragment != "" || remote.RawPath != "" || remote.ForceQuery || strings.ContainsAny(repositoryURL, "\x00\r\n") {
		return errors.New("unsupported repository URL")
	}
	return nil
}

// DirectoryName is shared with the native checkout route so unrelated URLs
// with the same repository name receive stable, independent directories.
func DirectoryName(repositoryURL string) (string, error) {
	remote, err := url.Parse(repositoryURL)
	if err != nil {
		return "", err
	}
	name := strings.TrimSuffix(filepath.Base(strings.TrimSuffix(remote.Path, "/")), ".git")
	if !fs.ValidPath(name) || strings.ContainsAny(name, "/\\") || name == "." || name == "" || len(name) > 128 {
		return "", errors.New("repository has an invalid checkout name")
	}
	sum := sha256.Sum256([]byte(repositoryURL))
	return name + "-" + hex.EncodeToString(sum[:8]), nil
}

func openCheckoutRoot(taskRoot, workdir string) (*os.Root, error) {
	if !filepath.IsAbs(taskRoot) || filepath.Clean(taskRoot) != taskRoot || workdir != filepath.Join(taskRoot, "workdir") {
		return nil, errors.New("checkout must stay in its task workdir")
	}
	resolved, err := filepath.EvalSymlinks(taskRoot)
	if err != nil || resolved != taskRoot {
		return nil, errors.New("checkout task root is unavailable or linked")
	}
	task, err := os.OpenRoot(taskRoot)
	if err != nil {
		return nil, err
	}
	defer task.Close()
	info, err := task.Lstat("workdir")
	if err != nil || !info.IsDir() {
		return nil, errors.New("checkout workdir is unavailable or linked")
	}
	return task.OpenRoot("workdir")
}

func verifyCheckout(ctx context.Context, root *os.Root, name string) error {
	info, err := root.Lstat(filepath.Join(name, ".git"))
	if err != nil || !info.IsDir() {
		return errors.New("repository requires its own Git directory")
	}
	repository, err := root.OpenRoot(name)
	if err != nil {
		return err
	}
	defer repository.Close()
	return fs.WalkDir(repository.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err != nil {
			return err
		}
		info, err := repository.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return VerifyLink(repository, path)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return errors.New("checkout contains a special file")
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); info.Mode().IsRegular() && (!ok || stat.Nlink != 1) {
			return errors.New("checkout shares Git objects or task files")
		}
		if entry.Name() == ".git" && !info.IsDir() || strings.HasSuffix(path, "/objects/info/alternates") || strings.HasSuffix(path, "/objects/info/http-alternates") || path == ".git/commondir" || strings.HasSuffix(path, "/.git/commondir") {
			return errors.New("checkout references an external Git directory")
		}
		return nil
	})
}

// VerifyLink accepts only resolvable links confined to their repository root.
// Git metadata cannot contain links. The caller must retain its writer gate
// while inspecting the tree; this check does not prevent concurrent mutation.
func VerifyLink(repository *os.Root, relative string) error {
	for _, component := range strings.Split(filepath.ToSlash(relative), "/") {
		if component == ".git" {
			return errors.New("Git metadata contains a symlink")
		}
	}
	info, err := repository.Stat(relative)
	if err != nil || !info.IsDir() && !info.Mode().IsRegular() {
		return errors.New("checkout symlink is unresolved or leaves its repository")
	}
	return nil
}

// Only the explicitly opened config is read. Never discover a repository or
// evaluate includes from the provider-controlled task directory.
func readOrigin(ctx context.Context, config *os.File) ([]byte, error) {
	info, err := config.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("repository config is not a regular file")
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 {
		return nil, errors.New("repository config is shared")
	}
	raw, err := io.ReadAll(io.LimitReader(&checkoutReader{ctx: ctx, reader: config}, 1<<20+1))
	if err != nil || len(raw) > 1<<20 {
		return nil, errors.Join(errors.New("repository config is unavailable or exceeds its parsing limit"), err)
	}
	command := exec.CommandContext(ctx, "git", "config", "--no-includes", "--file", "-", "--get", "remote.origin.url")
	command.Dir = "/"
	command.Stdin = bytes.NewReader(raw)
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=/nonexistent", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0"}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = time.Second
	var output checkoutOutput
	command.Stdout = &output
	if err := command.Start(); err != nil {
		return nil, errors.New("repository config inspection could not start")
	}
	err = command.Wait()
	if stopErr := processgroup.Stop(command.Process.Pid); stopErr != nil {
		return nil, errors.Join(ErrCheckoutWritersUnproven, stopErr)
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("repository origin could not be read")
	}
	return output.Bytes(), nil
}

type checkoutOutput struct{ bytes.Buffer }

func (output *checkoutOutput) Write(raw []byte) (int, error) {
	if output.Len()+len(raw) > 1<<20 {
		return 0, errors.New("Git checkout output exceeds budget")
	}
	return output.Buffer.Write(raw)
}
