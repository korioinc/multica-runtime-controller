package execution

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"syscall"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type configCopies []configuration.Copy

func (c *configCopies) String() string { return "sourceGroup/source/target JSON" }
func (c *configCopies) Set(raw string) error {
	var copy configuration.Copy
	if err := runtimeimage.Decode([]byte(raw), &copy); err != nil {
		return errors.New("home configuration copy requires sourceGroup/source/target JSON")
	}
	*c = append(*c, copy)
	return nil
}

// LayoutHome owns only a Pod-private volume. It commits its immutable source
// capture before publishing any operator file into the writable native HOME.
func LayoutHome(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("home layout", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	private := flags.String("private-root", "", "whole Pod-private volume preparation path")
	requestPath := flags.String("request", "", "worker's read-only request input")
	var copies configCopies
	flags.Var(&copies, "config-copy", "sourceGroup/source/target JSON")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || *private == "" || !filepath.IsAbs(*private) || filepath.Clean(*private) != *private || *private == "/" {
		return errors.New("home layout requires --private-root canonical directory")
	}
	if *requestPath != "" && (*requestPath != wire.RequestPath || len(copies) != 0) {
		return errors.New("worker home requires only its fixed read-only request input")
	}
	info, err := os.Lstat(*private)
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(*private)
	if err != nil || resolved != *private || !info.IsDir() {
		return errors.New("private preparation root must be a real directory")
	}
	volume, err := os.OpenRoot(*private)
	if err != nil {
		return err
	}
	defer volume.Close()
	for _, name := range []string{"agents", "tmp", "run"} {
		if err = volume.Mkdir(name, 0700); err != nil && !os.IsExist(err) {
			return err
		}
		child, err := volume.Lstat(name)
		if err != nil || !child.IsDir() {
			return errors.New("private child is not a real directory")
		}
		owner, ok := child.Sys().(*syscall.Stat_t)
		if !ok || owner.Uid != uint32(os.Geteuid()) {
			return errors.New("private child has a foreign owner")
		}
		if err = volume.Chmod(name, 0700); err != nil {
			return err
		}
	}
	run, home := filepath.Join(*private, "run"), filepath.Join(*private, "agents")
	d, digest, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		return err
	}
	if err = runtimeimage.PublishReceipt(run, d, digest); err != nil {
		return err
	}
	var bundle configuration.Bundle
	if *requestPath == "" {
		bundle, err = configuration.CaptureOrRead(run, copies, configuration.InputRoot)
	} else {
		raw, readErr := os.ReadFile(*requestPath)
		if readErr != nil {
			return readErr
		}
		request, readErr := wire.Decode(raw)
		if readErr != nil {
			return readErr
		}
		if readErr = runtimeimage.Match(d, digest, request.RuntimeRef); readErr != nil {
			return readErr
		}
		if _, readErr = os.Lstat(filepath.Join(run, configuration.BundleName)); readErr == nil {
			bundle, err = configuration.Read(run)
		} else if os.IsNotExist(readErr) {
			bundle, err = configuration.FromSnapshots(request.Snapshots, configuration.InputRoot)
			if err == nil {
				bundle, err = configuration.Commit(run, bundle)
			}
		} else {
			return readErr
		}
		if err == nil && bundle.Digest != request.RuntimeRef.ConfigurationDigest {
			return errors.New("worker configuration capture differs from its selected runtime")
		}
	}
	if err != nil {
		return err
	}
	if err = CopyBundle(home, bundle); err != nil {
		return err
	}
	if d.HomeSeed != "" {
		if err = copyHomeSeed(home, d.HomeSeed); err != nil {
			return err
		}
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, path := range []string{".multica/pi-sessions", ".codex/skills", ".pi/agent"} {
		if err = homeDirectories(root, path); err != nil {
			return err
		}
	}
	return nil
}

func copyHomeSeed(home, seed string) error {
	if err := runtimeimage.ValidateSeedContents(seed); err != nil {
		return err
	}
	entries, err := os.ReadDir(seed)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err = copyHomeConfigExcept(home, filepath.Join(seed, entry.Name()), filepath.Join(wire.Home, entry.Name()), filepath.Join(wire.Home, runtimeimage.PiNPMDirectory)); err != nil {
			return err
		}
	}
	packages := filepath.Join(seed, runtimeimage.PiNPMDirectory)
	if _, err := os.Lstat(packages); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return copyNPMSeed(home, packages)
}

func CopyBundle(home string, bundle configuration.Bundle) error {
	if err := bundle.Validate(); err != nil {
		return err
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, group := range bundle.Groups {
		for _, dir := range group.Directories {
			if err = homeDirectories(root, dir); err != nil {
				return err
			}
		}
		for _, file := range group.Files {
			if err = copyHomeContents(root, bytes.NewReader(file.Content), file.Target, os.FileMode(file.Mode)); err != nil {
				return err
			}
		}
	}
	return nil
}

// CheckPrivate validates the actual paths visible to the running process,
// including every ancestor needed by providers' private IPC implementations.
func CheckPrivate() error {
	for _, path := range []string{wire.Home, "/tmp", wire.ControlRoot} {
		for current := path; ; current = filepath.Dir(current) {
			info, err := os.Lstat(current)
			if err != nil {
				return err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || !info.IsDir() {
				return errors.New("private runtime path contains a link or non-directory")
			}
			if current == path {
				if stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm() != 0700 {
					return errors.New("private runtime path ownership or access mode mismatch")
				}
			} else if stat.Uid != 0 || info.Mode().Perm()&0022 != 0 {
				return errors.New("private runtime ancestor must be root-owned and not group/other writable")
			}
			if current == "/" {
				break
			}
		}
	}
	return nil
}

// BindControllerSessions is called only after image/configuration admission and
// workspace ownership validation. It preserves the daemon's native session path.
func BindControllerSessions() error {
	link := wire.PiSessionsRoot
	target := filepath.Join(wire.WorkspaceRoot, ".multica-runtime/sessions")
	if err := os.MkdirAll(target, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(link)
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		actual, err := os.Readlink(link)
		if err != nil {
			return err
		}
		if actual != target {
			return errors.New("controller session link points outside assigned storage")
		}
		return nil
	}
	if err == nil {
		if !info.IsDir() {
			return errors.New("controller session path is not an empty directory")
		}
		if err = os.Remove(link); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Symlink(target, link)
}
