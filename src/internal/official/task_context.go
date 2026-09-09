package official

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// TaskContext contains the daemon's prepared sidecars and managed task brief.
// Destination ownership and publication belong to the checkout package.
type TaskContext struct {
	Files                map[string][]byte
	Brief                []byte
	BriefBegin, BriefEnd string
}

func ReadTaskContext(source string) (TaskContext, error) {
	prepared, err := os.OpenRoot(source)
	if err != nil {
		return TaskContext{}, err
	}
	defer prepared.Close()
	var manifest struct {
		Files []string `json:"files"`
	}
	raw, err := readContextFile(prepared, ".multica_sidecar_manifest.json")
	if err != nil || json.Unmarshal(raw, &manifest) != nil {
		return TaskContext{}, errors.New("official task context is unavailable")
	}
	result := TaskContext{
		Files:      make(map[string][]byte, len(manifest.Files)),
		BriefBegin: "<!-- BEGIN MULTICA-RUNTIME (auto-managed; do not edit) -->",
		BriefEnd:   "<!-- END MULTICA-RUNTIME -->",
	}
	for _, name := range manifest.Files {
		rel, err := filepath.Rel(source, name)
		if err != nil || !filepath.IsLocal(rel) || !strings.HasPrefix(rel, "workdir/") {
			return TaskContext{}, errors.New("official sidecar is outside task context")
		}
		if rel == "workdir/AGENTS.md" {
			continue
		}
		data, err := readContextFile(prepared, rel)
		if err != nil {
			return TaskContext{}, err
		}
		result.Files[rel] = data
	}
	result.Brief, err = readContextFile(prepared, "workdir/AGENTS.md")
	if err != nil {
		return TaskContext{}, err
	}
	return result, nil
}

func readContextFile(root *os.Root, name string) ([]byte, error) {
	parts := strings.Split(filepath.ToSlash(name), "/")
	for i := range parts {
		info, err := root.Lstat(strings.Join(parts[:i+1], "/"))
		if err != nil {
			return nil, err
		}
		if i < len(parts)-1 && !info.IsDir() || i == len(parts)-1 && !info.Mode().IsRegular() {
			return nil, errors.New("official task context must contain regular, confined files")
		}
	}
	return root.ReadFile(name)
}
