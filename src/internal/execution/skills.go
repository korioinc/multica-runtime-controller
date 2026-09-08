package execution

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

const skillHome = ".codex/skills"

var skillNameSeparators = regexp.MustCompile(`[^a-z0-9]+`)

// Match the official daemon's user/workspace skill collision rule.
func canonicalSkillName(name string) string {
	name = skillNameSeparators.ReplaceAllString(strings.ToLower(strings.TrimSpace(name)), "-")
	name = strings.Trim(name, "-")
	if name == "" {
		return "skill"
	}
	return name
}

// hydrateSkills rebuilds managed skills from the selected image, committed
// operator configuration and current assignment before starting the provider.
// Current HOME is never used as a base: removing an assignment restores the
// operator defaults rather than retaining instructions from a previous task.
func hydrateSkills(source, home, imageSeed string, bundle configuration.Bundle) error {
	if err := bundle.Validate(); err != nil {
		return err
	}
	info, err := os.Lstat(home)
	if err != nil || !info.IsDir() {
		return errors.New("skill HOME must be a real directory")
	}
	private, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer private.Close()
	if err := homeDirectories(private, ".codex"); err != nil {
		return err
	}
	stage := ".codex/.skills-stage-" + uuid.NewString()
	if err := private.Mkdir(stage, 0700); err != nil {
		return err
	}
	defer private.RemoveAll(stage)
	staged, err := private.OpenRoot(stage)
	if err != nil {
		return err
	}
	defer staged.Close()
	if err := copyOperatorSkills(staged, bundle); err != nil {
		return err
	}
	// Copy operator files first; the image copy fills only missing files.
	if imageSeed != "" {
		seedSkills := filepath.Join(imageSeed, skillHome)
		if _, err := os.Lstat(seedSkills); err == nil {
			if err := copyHomeConfig(filepath.Join(home, stage), seedSkills, wire.Home+"/"+skillHome); err != nil {
				return err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := copyAssignedSkills(staged, source); err != nil {
		return err
	}
	return publishSkills(private, filepath.Join(stage, skillHome))
}

func copyOperatorSkills(target *os.Root, bundle configuration.Bundle) error {
	if err := homeDirectories(target, skillHome); err != nil {
		return err
	}
	for _, group := range bundle.Groups {
		for _, path := range group.Directories {
			if strings.HasPrefix(path, skillHome+"/") {
				if err := homeDirectories(target, path); err != nil {
					return err
				}
			}
		}
		for _, file := range group.Files {
			if file.Target == skillHome {
				return errors.New("operator skills must be a directory")
			}
			if strings.HasPrefix(file.Target, skillHome+"/") {
				if err := copyHomeContents(target, bytes.NewReader(file.Content), file.Target, os.FileMode(file.Mode)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func copyAssignedSkills(target *os.Root, source string) error {
	info, err := os.Lstat(source)
	if err != nil || !info.IsDir() {
		return errors.New("assigned skills must be a real directory")
	}
	prepared, err := os.OpenRoot(source)
	if err != nil {
		return err
	}
	defer prepared.Close()
	entries, err := fs.ReadDir(prepared.FS(), ".")
	if err != nil {
		return err
	}
	claimed := make(map[string]bool, len(entries))
	for _, unit := range entries {
		claimed[canonicalSkillName(unit.Name())] = true
	}
	defaults, err := fs.ReadDir(target.FS(), skillHome)
	if err != nil {
		return err
	}
	for _, unit := range defaults {
		if unit.IsDir() && claimed[canonicalSkillName(unit.Name())] {
			if err := target.RemoveAll(filepath.Join(skillHome, unit.Name())); err != nil {
				return err
			}
		}
	}
	for _, unit := range entries {
		// The assignment owns a whole top-level skill, including its references.
		if err := target.RemoveAll(filepath.Join(skillHome, unit.Name())); err != nil {
			return err
		}
		if err := fs.WalkDir(prepared.FS(), unit.Name(), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := prepared.Lstat(path)
			if err != nil {
				return err
			}
			destination := filepath.Join(skillHome, path)
			if info.IsDir() {
				return homeDirectories(target, destination)
			}
			if !info.Mode().IsRegular() {
				return errors.New("assigned skill contains a link or special file")
			}
			file, err := prepared.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			return copyHomeContents(target, file, destination, info.Mode())
		}); err != nil {
			return err
		}
	}
	return nil
}

func publishSkills(home *os.Root, stage string) error {
	backup := ".codex/.skills-previous-" + uuid.NewString()
	previous := false
	if info, err := home.Lstat(skillHome); err == nil {
		if !info.IsDir() {
			return errors.New("private skills destination must be a real directory")
		}
		if err := home.Rename(skillHome, backup); err != nil {
			return err
		}
		previous = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := home.Rename(stage, skillHome); err != nil {
		if previous {
			if rollbackErr := home.Rename(backup, skillHome); rollbackErr != nil {
				return errors.Join(err, fmt.Errorf("restore previous skills: %w", rollbackErr))
			}
		}
		return err
	}
	if previous {
		return home.RemoveAll(backup)
	}
	return nil
}
