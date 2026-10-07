package configuration

import (
	"errors"
	"github.com/google/uuid"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
)

func chromeProfileConflict(target string, file bool) bool {
	path := filepath.Join(Home, target)
	return path == ChromeProfileRoot || strings.HasPrefix(path, ChromeProfileRoot+"/") ||
		file && strings.HasPrefix(ChromeProfileRoot, path+"/")
}

// ValidateHomeConfiguration protects the image profile before applying operator files.
// Structural Bundle validation remains unchanged for historical signed records.
func ValidateHomeConfiguration(bundle Bundle) error {
	if err := bundle.Validate(); err != nil {
		return err
	}
	for _, group := range bundle.Groups {
		for _, file := range group.Files {
			if chromeProfileConflict(file.Target, true) {
				return diagnostics.ForGroup("configuration_chrome_profile_conflict", group.Name, errors.New("configuration overlaps the retained Chrome profile"))
			}
		}
		for _, directory := range group.Directories {
			if chromeProfileConflict(directory, false) {
				return diagnostics.ForGroup("configuration_chrome_profile_conflict", group.Name, errors.New("configuration overlaps the retained Chrome profile"))
			}
		}
	}
	return nil
}

// ApplyHomeConfiguration overlays operator files on HOME supplied by the image.
func ApplyHomeConfiguration(path string, bundle Bundle) error {
	if err := ValidateHomeConfiguration(bundle); err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return errors.New("HOME must be a real directory")
	}
	home, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer home.Close()
	for _, group := range bundle.Groups {
		for _, directory := range group.Directories {
			if err := home.MkdirAll(directory, 0700); err != nil {
				return err
			}
		}
		for _, file := range group.Files {
			if err := home.MkdirAll(filepath.Dir(file.Target), 0700); err != nil {
				return err
			}
			if err := WriteFile(home, file.Target, file.Content, fs.FileMode(file.Mode)); err != nil {
				return err
			}
		}
	}
	return nil
}

func WriteFile(root *os.Root, name string, raw []byte, mode fs.FileMode) error {
	stage := filepath.Join(filepath.Dir(name), ".write-"+uuid.NewString())
	file, err := root.OpenFile(stage, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer root.Remove(stage)
	_, err = file.Write(raw)
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err := root.Rename(stage, name); err != nil {
		return err
	}
	dir, err := root.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
