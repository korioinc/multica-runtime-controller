package execution

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type HomeOptions struct {
	PrivateRoot string
	RequestPath string
	Copies      []configuration.Copy
}

// LayoutHome owns only a Pod-private volume. Controllers commit their operator
// input; workers install an already prepared task HOME without selecting skills.
func LayoutHome(ctx context.Context, options HomeOptions) error {
	private := options.PrivateRoot
	if private == "" || !filepath.IsAbs(private) || filepath.Clean(private) != private || private == "/" {
		return errors.New("home layout requires --private-root canonical directory")
	}
	if options.RequestPath != "" && (options.RequestPath != wire.RequestPath || len(options.Copies) != 0) {
		return errors.New("worker home requires only its fixed read-only request input")
	}
	info, err := os.Lstat(private)
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(private)
	if err != nil || resolved != private || !info.IsDir() {
		return errors.New("private preparation root must be a real directory")
	}
	volume, err := os.OpenRoot(private)
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
	run, home := filepath.Join(private, "run"), filepath.Join(private, "agents")
	if options.RequestPath != "" {
		raw, err := os.ReadFile(options.RequestPath)
		if err != nil {
			return err
		}
		request, err := wire.Decode(raw)
		if err != nil {
			return err
		}
		finish := diagnostics.StartPhase("home_install", diagnostics.TaskAttributes(request.TaskID, request.AttemptID)...)
		err = InstallTaskHome(home, wire.HomeArtifactPath, request)
		finish(err)
		return err
	}
	finish := diagnostics.StartPhase("controller_image_validation")
	d, digest, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	finish(err)
	if err != nil {
		return err
	}
	if err = runtimeimage.PublishReceipt(run, d, digest); err != nil {
		return err
	}
	bundle, err := configuration.CaptureOrRead(run, options.Copies, configuration.InputRoot)
	if err != nil {
		return err
	}
	return layoutBaseHome(home, d.HomeSeed, bundle)
}

func layoutBaseHome(home, seed string, bundle configuration.Bundle) error {
	if err := CopyBundle(home, bundle); err != nil {
		return err
	}
	if seed != "" {
		if err := copyHomeSeed(home, seed); err != nil {
			return err
		}
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, path := range []string{".multica/pi-sessions", ".codex/skills", ".pi/agent"} {
		if err := homeDirectories(root, path); err != nil {
			return err
		}
	}
	return nil
}

func copyHomeSeed(home, seed string) error {
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
