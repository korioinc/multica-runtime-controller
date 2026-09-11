// Package execution coordinates authorized attempts and provider processes.
package execution

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

const SelectionSchemaVersion = 3

type Selection struct {
	SchemaVersion int               `json:"schemaVersion"`
	OwnerID       string            `json:"ownerID"`
	Controller    kubernetes.Owner  `json:"controller"`
	Namespace     string            `json:"namespace"`
	Gateway       string            `json:"gateway"`
	Backend       string            `json:"backend"`
	RuntimeRef    runtimeimage.Ref  `json:"runtimeRef"`
	Worker        kubernetes.Config `json:"worker"`
	OperatorKeys  []string          `json:"operatorKeys"`
	GitHubApp     bool              `json:"githubApp"`
}

func (s Selection) Validate() error {
	if s.SchemaVersion != SelectionSchemaVersion || !wire.UUID(s.OwnerID) || s.Namespace == "" || s.Controller.Name == "" || s.Controller.UID == "" || s.RuntimeRef.Platform != core.HostPlatform() {
		return errors.New("invalid runtime selection; schema 3 required")
	}
	if err := s.Worker.Validate(); err != nil {
		return err
	}
	if s.Worker.Platform != s.RuntimeRef.Platform {
		return errors.New("runtime selection platform mismatch")
	}
	if err := s.RuntimeRef.Validate(); err != nil {
		return err
	}
	return nil
}
func SaveSelection(s Selection) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(wire.ControlRoot, 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if len(raw) > wire.MaxRequestBytes {
		return errors.New("runtime selection exceeds supported configuration metadata size")
	}
	return durableWrite(wire.SelectionPath, raw)
}
func LoadSelection() (Selection, error) {
	var s Selection
	st, err := os.Lstat(wire.SelectionPath)
	if err != nil {
		return s, err
	}
	if !st.Mode().IsRegular() || st.Size() > wire.MaxRequestBytes {
		return s, errors.New("invalid selection file")
	}
	raw, err := os.ReadFile(wire.SelectionPath)
	if err != nil {
		return s, err
	}
	if err := runtimeimage.Decode(raw, &s); err != nil {
		return s, err
	}
	return s, s.Validate()
}
func OpenWorkspace(owner string) (*workspace.Store, error) {
	return workspace.Open(workspace.Options{Directory: workspace.DefaultDirectory, WorkspaceRoot: wire.WorkspaceRoot, SessionRoot: wire.PiSessionsRoot, OwnerID: owner})
}
func LayoutWorkspace(owner string) error {
	if _, err := OpenWorkspace(owner); err != nil {
		return err
	}
	for _, name := range []string{"workers", "sessions", "attempts", "context"} {
		if err := os.MkdirAll(filepath.Join(wire.WorkspaceRoot, ".multica-runtime", name), 0700); err != nil {
			return err
		}
	}
	return syncDir(filepath.Join(wire.WorkspaceRoot, ".multica-runtime"))
}

const OperatorPrefix = "MULTICA_OPERATOR_"

func OperatorNames(env []string) []string {
	names := []string{}
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if ok && strings.HasPrefix(key, OperatorPrefix) {
			names = append(names, strings.TrimPrefix(key, OperatorPrefix))
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// SelectedEnvironment reads operator provenance from the actual Pod environment,
// rather than rereading mutable Secret/ConfigMap objects after Pod startup.
func SelectedEnvironment(manifest runtimeimage.Descriptor, base []string, locations runtimeimage.Locations) ([]string, error) {
	image := []string{}
	operator := map[string]string{}
	for _, entry := range base {
		key, val, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if strings.HasPrefix(key, OperatorPrefix) {
			key = strings.TrimPrefix(key, OperatorPrefix)
			if runtimeimage.Reserved(key) || strings.HasPrefix(key, "POD_") {
				return nil, errors.New("operator overrides reserved environment key")
			}
			operator[key] = val
		} else {
			image = append(image, entry)
		}
	}
	vars, err := runtimeimage.Vars(manifest, image, locations)
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, entry := range vars {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	for key, value := range operator {
		values[key] = value
	}
	values["PATH"] = wire.ControllerRoot + ":" + values["PATH"]
	return wire.Environment(values), nil
}
