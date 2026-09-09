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

// ManagedText replaces the first complete delimited region while preserving
// surrounding user text. The input adapter owns the delimiter format.
type ManagedText struct {
	Content    []byte
	Begin, End string
}

// SeedContext refreshes only the context files owned by this binding.
// The durable ownership union is published before writes so retry after a crash
// cannot mistake a partial refresh for user-owned files.
func SeedContext(destination, record string, files map[string][]byte, brief ManagedText) error {
	if brief.Begin == "" || brief.End == "" || brief.Begin == brief.End {
		return errors.New("task brief requires distinct, nonempty delimiters")
	}
	current := make([]string, 0, len(files))
	for name := range files {
		if !contextPath(name) {
			return errors.New("unconfined task context file")
		}
		current = append(current, name)
	}
	slices.Sort(current)
	canonical, err := filepath.EvalSymlinks(destination)
	if err != nil || canonical != destination {
		return errors.New("worker destination must be a canonical bound directory")
	}
	worker, err := os.OpenRoot(destination)
	if err != nil {
		return err
	}
	defer worker.Close()
	for _, directory := range []string{"workdir", "multica-config", "output", "logs"} {
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
		if !contextPath(name) {
			return errors.New("unconfined context ownership record")
		}
	}
	for _, name := range current {
		if _, err := worker.Lstat(name); err == nil && !slices.Contains(previous, name) {
			return errors.New("official context conflicts with a user file")
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
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

	return writeContextRecord(record, current)
}

func contextPath(name string) bool {
	return fs.ValidPath(name) && strings.HasPrefix(name, "workdir/") && name != "workdir/AGENTS.md"
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
func mergeBrief(existing []byte, brief ManagedText) []byte {
	prepared := brief.Content
	begin, end := []byte(brief.Begin), []byte(brief.End)
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
