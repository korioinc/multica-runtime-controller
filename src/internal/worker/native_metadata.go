package worker

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func materializeNativeMetadata(ctx context.Context, b wire.Bootstrap, run wire.Run, home *os.Root) error {
	m := run.NativeMetadata
	if m == nil || m.Validate(b.Provider, b.TaskRoot) != nil || m.WorkerSessionID != b.WorkerSessionID || m.TurnSequence != b.TurnSequence || m.Digest() != b.NativeMetadataDigest {
		return errors.New("current native metadata does not match its prepared assignment")
	}
	if err := clearNativeTurnMetadata(); err != nil {
		return err
	}
	control, err := os.OpenRoot(wire.ControlRoot)
	if err != nil {
		return err
	}
	defer control.Close()
	if err := nativePrivateDirectories(control, "native-metadata"); err != nil {
		return err
	}
	metadata, err := workspace.OpenNativeDirectory(control, "native-metadata")
	if err != nil {
		return err
	}
	defer metadata.Close()
	name := m.WorkerSessionID + "-" + strconv.FormatUint(m.TurnSequence, 10)
	if err := metadata.Mkdir(name, 0700); err != nil {
		return errors.New("private native view already exists")
	}
	view, err := workspace.OpenNativeDirectory(metadata, name)
	if err != nil {
		return err
	}
	defer view.Close()
	task, err := os.OpenRoot(b.TaskRoot)
	if err != nil {
		return err
	}
	defer task.Close()
	for path, target := range b.AllowedLinks {
		if workspace.NativeProjectionLinks(b.Provider)[path] != target {
			continue
		}
		parent, base := filepath.Split(path)
		directory, err := workspace.OpenNativeDirectory(task, strings.TrimSuffix(parent, "/"))
		if err != nil {
			return err
		}
		actual, err := directory.Readlink(base)
		directory.Close()
		if err != nil || actual != target {
			return errors.New("fixed native projection changed before materialization")
		}
	}
	relative, err := filepath.Rel(b.TaskRoot, m.ArtifactRoot)
	if err != nil {
		return err
	}
	artifacts, err := workspace.OpenNativeDirectory(task, relative)
	if err != nil {
		return err
	}
	defer artifacts.Close()
	providerHome := map[string]string{"codex": ".codex", "pi": ".pi/agent", "claude": workspace.ClaudeConfigDir}[b.Provider]
	if b.Provider == "codex" {
		if err := nativePrivateDirectories(view, "codex/skills"); err != nil {
			return err
		}
		if err := copyNativeOperatorSkills(home, view, providerHome+"/skills", "codex/skills"); err != nil {
			return err
		}
	}
	for _, artifact := range m.Artifacts {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw, err := workspace.ReadNativeArtifact(artifacts, artifact)
		if err != nil {
			return err
		}
		path := artifact.Path
		if b.Provider == "codex" {
			path = filepath.Join("codex", path)
		}
		if err := nativePrivateDirectories(view, filepath.Dir(path)); err != nil {
			return err
		}
		if err := configuration.WriteFile(view, path, raw, 0600); err != nil {
			return err
		}
		if b.Provider != "codex" {
			target := filepath.Join(providerHome, artifact.Path)
			if err := nativePrivateDirectories(home, filepath.Dir(target)); err != nil {
				return err
			}
			if err := configuration.WriteFile(home, target, raw, 0600); err != nil {
				return err
			}
		}
	}
	for _, file := range []struct {
		path string
		raw  []byte
	}{
		{"codex/config.toml", m.CodexConfig}, {"claude-runtime-skill-settings.json", m.ClaudeSettings},
		{"pi-openai-service-tier.json", m.ServiceTier}, {"resources.json", m.ProjectResources},
	} {
		if len(file.raw) == 0 {
			continue
		}
		if err := nativePrivateDirectories(view, filepath.Dir(file.path)); err != nil {
			return err
		}
		if err := configuration.WriteFile(view, file.path, file.raw, 0600); err != nil {
			return err
		}
	}
	if err := configuration.WriteFile(control, filepath.Base(workspace.NativeTaskMarker), m.TaskMarker, 0600); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Select only the complete private view built from exact manifest bytes.
	return metadata.Symlink(name, "current")
}

func nativePrivateDirectories(root *os.Root, path string) error {
	if path == "." {
		return nil
	}
	if !fs.ValidPath(path) {
		return errors.New("invalid private native path")
	}
	parent := root
	var opened []*os.Root
	defer func() {
		for _, directory := range opened {
			directory.Close()
		}
	}()
	for _, name := range strings.Split(path, "/") {
		if err := parent.Mkdir(name, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		child, err := workspace.OpenNativeDirectory(parent, name)
		if err != nil {
			return err
		}
		opened = append(opened, child)
		parent = child
	}
	return nil
}

func readNativeHomeFile(home *os.Root, path string) ([]byte, error) {
	info, err := home.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("private HOME skill is indirect or unavailable")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || info.Size() > workspace.MaxNativeArtifactBytes {
		return nil, errors.New("private HOME skill is shared or too large")
	}
	file, err := home.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, workspace.MaxNativeArtifactBytes+1))
	err = errors.Join(readErr, file.Close())
	if err != nil || len(raw) > workspace.MaxNativeArtifactBytes {
		return nil, errors.New("private HOME skill exceeds its copy budget")
	}
	return raw, nil
}

func copyNativeOperatorSkills(home, view *os.Root, source, target string) error {
	if _, err := home.Lstat(source); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	directory, err := workspace.OpenNativeDirectory(home, source)
	if err != nil {
		return err
	}
	defer directory.Close()
	return fs.WalkDir(directory.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nativePrivateDirectories(view, filepath.Join(target, path))
		}
		raw, err := readNativeHomeFile(directory, path)
		if err != nil {
			return errors.New("operator skill is not a bounded regular file")
		}
		return configuration.WriteFile(view, filepath.Join(target, path), raw, 0600)
	})
}

func clearNativeTurnMetadata() error {
	control, err := os.OpenRoot(wire.ControlRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer control.Close()
	// Cleanup paths are fixed; never trust a provider-authored cleanup record.
	// The noncredential guard remains valid throughout idle.
	return control.RemoveAll("native-metadata")
}
