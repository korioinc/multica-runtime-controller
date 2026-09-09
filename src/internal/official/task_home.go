package official

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

// HomeDirectory is a confined provider input copied to a relative HOME target.
type HomeDirectory struct {
	Root   *os.Root
	Target string
}

// HomeOverrides describes the task-owned changes to an already composed base
// HOME. The caller owns all directory handles and must close the result.
type HomeOverrides struct {
	Remove      []string
	Directories []HomeDirectory
}

func (overrides HomeOverrides) Close() error {
	var failures []error
	for _, directory := range overrides.Directories {
		if directory.Root != nil {
			failures = append(failures, directory.Root.Close())
		}
	}
	return errors.Join(failures...)
}

const codexSkillHome = ".codex/skills"

var skillNameSeparators = regexp.MustCompile(`[^a-z0-9]+`)

// canonicalSkillName matches the pinned daemon's user/workspace collision rule.
func canonicalSkillName(name string) string {
	name = skillNameSeparators.ReplaceAllString(strings.ToLower(strings.TrimSpace(name)), "-")
	name = strings.Trim(name, "-")
	if name == "" {
		return "skill"
	}
	return name
}

// ReadTaskHomeOverrides owns the pinned daemon's provider-specific HOME policy.
// Pi assignments already belong to workdir/.pi/skills and travel with sidecars;
// the other supported providers have no task HOME overlay in this adapter.
func ReadTaskHomeOverrides(provider, taskRoot, globalHome string, baseHome *os.Root) (overrides HomeOverrides, returnErr error) {
	if !runtimeimage.SupportedProvider(provider) {
		return overrides, errors.New("unsupported provider task HOME")
	}
	if provider != "codex" {
		return overrides, nil
	}
	if baseHome == nil {
		return overrides, errors.New("task HOME overrides require the composed base")
	}
	directories, err := readCodexHomeDirectories(taskRoot, filepath.Join(globalHome, codexSkillHome))
	if err != nil {
		return overrides, err
	}
	overrides.Directories = directories
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, overrides.Close())
			overrides = HomeOverrides{}
		}
	}()
	claimed := make(map[string]bool, len(directories))
	for _, directory := range directories {
		claimed[canonicalSkillName(filepath.Base(directory.Target))] = true
		// Exact targets also replace a file collision, as well as directories.
		overrides.Remove = append(overrides.Remove, directory.Target)
	}
	defaults, err := fs.ReadDir(baseHome.FS(), codexSkillHome)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return overrides, err
	}
	for _, unit := range defaults {
		if unit.IsDir() && claimed[canonicalSkillName(unit.Name())] {
			overrides.Remove = append(overrides.Remove, filepath.Join(codexSkillHome, unit.Name()))
		}
	}
	slices.Sort(overrides.Remove)
	overrides.Remove = slices.Compact(overrides.Remove)
	return overrides, nil
}

// readCodexHomeDirectories interprets only the daemon's assignment tree. Its auth links
// and other provider state are not a complete or independently owned HOME.
// Exact global links are inherited defaults, supplied by committed configuration.
func readCodexHomeDirectories(taskRoot, globalSkills string) (directories []HomeDirectory, returnErr error) {
	canonical, err := filepath.EvalSymlinks(taskRoot)
	if err != nil || canonical != taskRoot {
		return nil, errors.New("official task HOME requires a canonical task directory")
	}
	task, err := os.OpenRoot(taskRoot)
	if err != nil {
		return nil, err
	}
	defer task.Close()
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, (HomeOverrides{Directories: directories}).Close())
			directories = nil
		}
	}()
	for _, path := range []string{"codex-home", "codex-home/skills"} {
		info, err := task.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		if err != nil || !info.IsDir() {
			return nil, errors.New("official task skill path must be a real directory")
		}
	}
	entries, err := fs.ReadDir(task.FS(), "codex-home/skills")
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		path := filepath.Join("codex-home/skills", entry.Name())
		info, err := task.Lstat(path)
		if err != nil {
			return directories, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			inherited, err := inheritedCodexSkill(task, path, globalSkills)
			if err != nil {
				return directories, err
			}
			if inherited {
				continue
			}
			return directories, errors.New("assigned skill contains an unapproved link")
		}
		if !info.IsDir() {
			return directories, errors.New("assigned skill must be a directory")
		}
		root, err := task.OpenRoot(path)
		if err != nil {
			return directories, err
		}
		directories = append(directories, HomeDirectory{Root: root, Target: filepath.Join(codexSkillHome, entry.Name())})
		opened, err := root.Stat(".")
		if err != nil || !os.SameFile(info, opened) {
			return directories, errors.New("assigned skill changed while opening its directory")
		}
		if err := validateAssignedSkill(root); err != nil {
			return directories, err
		}
	}
	return directories, nil
}

func validateAssignedSkill(root *os.Root) error {
	return fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := root.Lstat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return errors.New("assigned skill contains a link or special file")
		}
		return nil
	})
}

func inheritedCodexSkill(task *os.Root, name, globalSkills string) (bool, error) {
	if !filepath.IsAbs(globalSkills) || filepath.Clean(globalSkills) != globalSkills {
		return false, nil
	}
	target, err := task.Readlink(name)
	if err != nil {
		return false, err
	}
	expected := filepath.Join(globalSkills, filepath.Base(name))
	if target != expected {
		return false, nil
	}
	info, err := os.Lstat(expected)
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, nil
	}
	canonical, err := filepath.EvalSymlinks(expected)
	if err != nil {
		return false, err
	}
	return canonical == expected, nil
}
