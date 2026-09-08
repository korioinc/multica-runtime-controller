package checkout

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// SeedContext refreshes only the official sidecar files owned by this binding.
// The durable ownership union is published before writes so retry after a crash
// cannot mistake a partial refresh for user-owned files.
func SeedContext(source, destination, record, provider, globalSkills string) error {
	prepared, err := os.OpenRoot(source)
	if err != nil {
		return err
	}
	defer prepared.Close()
	canonical, err := filepath.EvalSymlinks(destination)
	if err != nil || canonical != destination {
		return errors.New("worker destination must be a canonical bound directory")
	}
	worker, err := os.OpenRoot(destination)
	if err != nil {
		return err
	}
	defer worker.Close()
	for _, directory := range []string{"workdir", "multica-config", "output", "logs", "codex-skills"} {
		if err := plainDirectories(worker, directory, 0700); err != nil {
			return err
		}
	}
	var previous []string
	if raw, err := os.ReadFile(record); err == nil {
		if json.Unmarshal(raw, &previous) != nil {
			return errors.New("invalid context ownership record")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, name := range previous {
		if !fs.ValidPath(name) || !strings.HasPrefix(name, "workdir/") || name == "workdir/AGENTS.md" {
			return errors.New("unconfined context ownership record")
		}
	}
	var manifest struct {
		Files []string `json:"files"`
	}
	raw, err := plainRead(prepared, ".multica_sidecar_manifest.json")
	if err != nil || json.Unmarshal(raw, &manifest) != nil {
		return errors.New("official task context is unavailable")
	}
	files := map[string][]byte{}
	var current []string
	for _, name := range manifest.Files {
		rel, err := filepath.Rel(source, name)
		if err != nil || !filepath.IsLocal(rel) || !strings.HasPrefix(rel, "workdir/") {
			return errors.New("official sidecar is outside task context")
		}
		if rel == "workdir/AGENTS.md" {
			continue
		}
		data, err := plainRead(prepared, rel)
		if err != nil {
			return err
		}
		if _, err := worker.Lstat(rel); err == nil && !slices.Contains(previous, rel) {
			return errors.New("official context conflicts with a user file")
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		files[rel] = data
		current = append(current, rel)
	}
	brief, err := plainRead(prepared, "workdir/AGENTS.md")
	if err != nil {
		return err
	}
	existing, err := plainRead(worker, "workdir/AGENTS.md")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	union := append(slices.Clone(previous), current...)
	slices.Sort(union)
	union = slices.Compact(union)
	if err := writeContextRecord(record, union); err != nil {
		return err
	}
	for _, name := range union {
		if err := plainDirectories(worker, filepath.ToSlash(filepath.Dir(name)), 0700); err != nil {
			return err
		}
		if err := worker.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for name, data := range files {
		if err := replace(worker, name, data, 0600); err != nil {
			return err
		}
	}
	if err := replace(worker, "workdir/AGENTS.md", mergeBrief(existing, brief), 0600); err != nil {
		return err
	}
	if provider == "codex" {
		if err := worker.RemoveAll("codex-skills"); err != nil {
			return err
		}
		if err := worker.Mkdir("codex-skills", 0700); err != nil {
			return err
		}
		if _, err := prepared.Lstat("codex-home/skills"); err == nil {
			if err := fs.WalkDir(prepared.FS(), "codex-home/skills", func(name string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				if entry.Type()&os.ModeSymlink != 0 {
					inherited, err := inheritedCodexSkill(prepared, name, globalSkills)
					if err != nil || inherited {
						return err
					}
				}
				data, err := plainRead(prepared, name)
				if err != nil {
					return err
				}
				return replace(worker, "codex-skills/"+strings.TrimPrefix(name, "codex-home/skills/"), data, 0600)
			}); err != nil {
				return err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return writeContextRecord(record, current)
}

// The official daemon links global skills into its per-task home. They are
// already supplied by the worker's committed configuration, so exporting them
// as assigned skills would give mutable controller HOME unintended precedence.
// Only an exact, same-name link to a real canonical global directory is skipped.
func inheritedCodexSkill(prepared *os.Root, name, globalSkills string) (bool, error) {
	if filepath.Dir(name) != "codex-home/skills" || !filepath.IsAbs(globalSkills) || filepath.Clean(globalSkills) != globalSkills {
		return false, nil
	}
	target, err := prepared.Readlink(name)
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

func writeContextRecord(path string, files []string) error {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer root.Close()
	raw, err := json.Marshal(files)
	if err != nil {
		return err
	}
	if err := replace(root, filepath.Base(path), raw, 0600); err != nil {
		return err
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func mergeBrief(existing, prepared []byte) []byte {
	begin := []byte("<!-- BEGIN MULTICA-RUNTIME (auto-managed; do not edit) -->")
	end := []byte("<!-- END MULTICA-RUNTIME -->")
	if start := bytes.Index(existing, begin); start >= 0 {
		if finish := bytes.Index(existing[start+len(begin):], end); finish >= 0 {
			finish += start + len(begin) + len(end)
			result := append([]byte{}, existing[:start]...)
			result = append(result, bytes.TrimRight(prepared, "\n")...)
			return append(result, existing[finish:]...)
		}
	}
	if len(existing) == 0 {
		return prepared
	}
	result := append([]byte{}, existing...)
	result = append(result, '\n', '\n')
	return append(result, prepared...)
}
