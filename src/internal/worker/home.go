package worker

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/githubauth"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type homeFile struct {
	content []byte
	mode    fs.FileMode
	present bool
}

// managedHome holds the image's original managed files in supervisor memory.
// Each turn overlays its captured configuration on this baseline, so SDK edits
// and generated provider credentials cannot become another turn's defaults.
type managedHome struct {
	path  string
	files map[string]homeFile
}

func captureManagedHome(path string, bundle configuration.Bundle) (*managedHome, error) {
	if err := configuration.ValidateHomeConfiguration(bundle); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	names := []string{".claude/settings.json", ".claude/settings.local.json", ".claude/.credentials.json", ".claude.json", ".codex/auth.json", ".codex/config.toml", ".multica/config.json", ".pi/agent/auth.json", ".pi/agent/models.json", ".pi/agent/settings.json", piMCPConfigPath, piAdapterMCPPath, ".pi/agent/trust.json", ".npmrc", ".cargo/config.toml", ".cargo/config", ".config/pip/pip.conf", ".config/composer/config.json", ".composer/config.json"}
	for _, group := range bundle.Groups {
		for _, file := range group.Files {
			names = append(names, file.Target)
		}
	}
	for _, path := range nativeHomeSkillRoots {
		if _, err := root.Lstat(path); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := fs.WalkDir(root.FS(), path, func(name string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !entry.IsDir() {
				names = append(names, name)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	home := &managedHome{path: path, files: make(map[string]homeFile)}
	for _, name := range names {
		info, err := root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			home.files[name] = homeFile{}
			continue
		}
		if err != nil || !info.Mode().IsRegular() || info.Size() > configuration.MaxGroupBytes {
			return nil, errors.New("managed HOME baseline is not a bounded regular file")
		}
		file, err := root.Open(name)
		if err != nil {
			return nil, err
		}
		content, readErr := io.ReadAll(io.LimitReader(file, configuration.MaxGroupBytes+1))
		err = errors.Join(readErr, file.Close())
		if err != nil || len(content) > configuration.MaxGroupBytes {
			return nil, errors.New("managed HOME baseline could not be captured")
		}
		home.files[name] = homeFile{content: content, mode: info.Mode().Perm(), present: true}
	}
	return home, nil
}

func (h *managedHome) restore() error {
	if h == nil {
		return nil
	}
	root, err := os.OpenRoot(h.path)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, path := range nativeHomeSkillRoots {
		if err := root.RemoveAll(path); err != nil {
			return err
		}
	}
	names := make([]string, 0, len(h.files))
	for name := range h.files {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		file := h.files[name]
		if !file.present {
			if err := root.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			continue
		}
		if err := root.MkdirAll(filepath.Dir(name), 0700); err != nil {
			return err
		}
		if err := configuration.WriteFile(root, name, file.content, file.mode); err != nil {
			return err
		}
	}
	return nil
}

var nativeHomeSkillRoots = []string{".codex/skills", ".pi/agent/skills", ".claude/skills"}

func clearTurnPrivateFiles() error {
	if err := clearNativeTurnMetadata(); err != nil {
		return err
	}
	root, err := os.OpenRoot(wire.ControlRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	for _, name := range []string{"ready", githubauth.TaskAuthorizationName, "task-config", "bin/gh"} {
		if err := root.RemoveAll(name); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(wire.ControlRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "runner-") {
			if err := root.RemoveAll(entry.Name()); err != nil {
				return err
			}
		}
	}
	return nil
}
