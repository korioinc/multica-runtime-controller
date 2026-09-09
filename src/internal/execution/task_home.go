package execution

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/official"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

const (
	taskHomeArtifacts    = ".runtime-home"
	taskHomeIdentityFile = "identity.json"
	taskHomeMarker       = ".multica-runtime-home.json"
)

type taskHomeIdentity struct {
	SchemaVersion int              `json:"schemaVersion"`
	TaskID        string           `json:"taskID"`
	AttemptID     string           `json:"attemptID"`
	OwnerID       string           `json:"ownerID"`
	WorkerSubPath string           `json:"workerSubPath"`
	Provider      string           `json:"provider"`
	RuntimeRef    runtimeimage.Ref `json:"runtimeRef"`
}

func homeIdentity(request wire.Request) (taskHomeIdentity, error) {
	storage := strings.TrimPrefix(request.WorkerSubPath, ".multica-runtime/workers/")
	if !wire.UUID(request.TaskID) || !wire.UUID(request.AttemptID) || !wire.UUID(request.OwnerID) || !wire.UUID(storage) || request.WorkerSubPath != ".multica-runtime/workers/"+storage || wire.Alias(request.Provider) == "" {
		return taskHomeIdentity{}, errors.New("task HOME identity is invalid")
	}
	if err := request.RuntimeRef.Validate(); err != nil {
		return taskHomeIdentity{}, err
	}
	if _, enabled := request.RuntimeRef.Providers[request.Provider]; !enabled {
		return taskHomeIdentity{}, errors.New("task HOME provider is not selected")
	}
	return taskHomeIdentity{SchemaVersion: 1, TaskID: request.TaskID, AttemptID: request.AttemptID, OwnerID: request.OwnerID, WorkerSubPath: request.WorkerSubPath, Provider: request.Provider, RuntimeRef: request.RuntimeRef}, nil
}

func (identity taskHomeIdentity) matches(request wire.Request) bool {
	expected, err := homeIdentity(request)
	return err == nil && identity.SchemaVersion == expected.SchemaVersion && identity.TaskID == expected.TaskID && identity.AttemptID == expected.AttemptID && identity.OwnerID == expected.OwnerID && identity.WorkerSubPath == expected.WorkerSubPath && identity.Provider == expected.Provider && identity.RuntimeRef.Equal(expected.RuntimeRef)
}

// PrepareTaskHome runs only after the caller holds the storage lease and has
// reconciled prior consumers. The completed archive belongs to this attempt.
func PrepareTaskHome(request wire.Request, workerRoot string, manifest runtimeimage.Descriptor, bundle configuration.Bundle) (string, error) {
	if _, err := homeIdentity(request); err != nil {
		return "", err
	}
	if workerRoot != filepath.Join(wire.WorkspaceRoot, request.WorkerSubPath) {
		return "", errors.New("task HOME storage differs from its authorized worker binding")
	}
	taskRoot, err := wire.StorageRoot(request)
	if err != nil {
		return "", err
	}
	return prepareTaskHome(request, workerRoot, taskRoot, wire.Home, manifest, bundle)
}

// Path authorization belongs to PrepareTaskHome; this boundary owns composition
// and publication from already selected task, image and configuration inputs.
func prepareTaskHome(request wire.Request, workerRoot, taskRoot, globalHome string, manifest runtimeimage.Descriptor, bundle configuration.Bundle) (string, error) {
	identity, err := homeIdentity(request)
	if err != nil {
		return "", err
	}
	if err := runtimeimage.Match(manifest, request.RuntimeRef.DescriptorDigest, request.RuntimeRef); err != nil {
		return "", err
	}
	if err := bundle.Validate(); err != nil {
		return "", err
	}
	if bundle.Digest != request.RuntimeRef.ConfigurationDigest {
		return "", errors.New("task HOME configuration differs from the selected runtime")
	}
	artifacts, err := resetTaskHomeArtifacts(workerRoot)
	if err != nil {
		return "", err
	}
	defer artifacts.Close()
	stage := "home-" + uuid.NewString()
	if err := artifacts.Mkdir(stage, 0700); err != nil {
		return "", err
	}
	defer artifacts.RemoveAll(stage)
	home := filepath.Join(workerRoot, taskHomeArtifacts, stage)
	if err := composeTaskHome(home, manifest.HomeSeed, bundle, request.Provider, taskRoot, globalHome); err != nil {
		return "", err
	}
	return publishTaskHomeArchive(artifacts, stage, identity)
}

func resetTaskHomeArtifacts(workerRoot string) (*os.Root, error) {
	canonical, err := filepath.EvalSymlinks(workerRoot)
	if err != nil || canonical != workerRoot {
		return nil, errors.New("task HOME storage must be a canonical directory")
	}
	worker, err := os.OpenRoot(workerRoot)
	if err != nil {
		return nil, err
	}
	defer worker.Close()
	if err := worker.RemoveAll(taskHomeArtifacts); err != nil {
		return nil, err
	}
	if err := worker.Mkdir(taskHomeArtifacts, 0700); err != nil {
		return nil, err
	}
	if err := syncDir(workerRoot); err != nil {
		return nil, err
	}
	return worker.OpenRoot(taskHomeArtifacts)
}

func composeTaskHome(home, seed string, bundle configuration.Bundle, provider, taskRoot, globalHome string) (returnErr error) {
	if err := layoutBaseHome(home, seed, bundle); err != nil {
		return err
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer root.Close()
	overrides, err := official.ReadTaskHomeOverrides(provider, taskRoot, globalHome, root)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, overrides.Close()) }()
	if err := applyHomeOverrides(root, overrides); err != nil {
		return err
	}
	return validateTaskHomeTree(home)
}

func applyHomeOverrides(home *os.Root, overrides official.HomeOverrides) error {
	for _, path := range overrides.Remove {
		if !validHomeOverridePath(path) {
			return errors.New("task HOME override removes an unconfined or protected path")
		}
	}
	for _, directory := range overrides.Directories {
		if directory.Root == nil || !validHomeOverridePath(directory.Target) {
			return errors.New("task HOME override has an invalid source or protected target")
		}
	}
	for _, path := range overrides.Remove {
		if err := home.RemoveAll(path); err != nil {
			return err
		}
	}
	for _, directory := range overrides.Directories {
		if err := copyHomeOverride(home, directory); err != nil {
			return err
		}
	}
	return nil
}

func validHomeOverridePath(path string) bool {
	// A whole-directory replacement must also reject ancestors of protected
	// state, even where ordinary per-file HOME configuration is allowed.
	return validTaskHomePath(path, true) && configuration.HomePath(wire.Home+"/"+path, false)
}

func copyHomeOverride(home *os.Root, directory official.HomeDirectory) error {
	return fs.WalkDir(directory.Root.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := directory.Root.Lstat(path)
		if err != nil {
			return err
		}
		destination := filepath.Join(directory.Target, path)
		if !validTaskHomePath(destination, info.IsDir()) {
			return errors.New("task HOME override contains an unconfined or protected path")
		}
		if info.IsDir() {
			return homeDirectories(home, destination)
		}
		if !info.Mode().IsRegular() {
			return errors.New("task HOME override changed to a link or special file")
		}
		file, err := directory.Root.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		return copyHomeContents(home, file, destination, info.Mode())
	})
}

func validTaskHomePath(path string, directory bool) bool {
	if !fs.ValidPath(path) || path == "." || strings.Contains(path, "\\") || path == taskHomeMarker || strings.HasPrefix(path, taskHomeMarker+"/") {
		return false
	}
	return directory && path == ".multica/pi-sessions" || configuration.HomePath(wire.Home+"/"+path, directory)
}

func validateTaskHomeTree(home string) error {
	root, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer root.Close()
	return fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || path == "." {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !validTaskHomePath(path, info.IsDir()) {
			return errors.New("task HOME contains reserved authority or session state")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if !npmCommandPath(path) {
				return errors.New("task HOME contains an unapproved link")
			}
			return runtimeimage.ValidateNPMCommandLink(filepath.Join(home, runtimeimage.PiNPMDirectory), filepath.Join(home, path))
		} else if !info.IsDir() && !info.Mode().IsRegular() {
			return errors.New("task HOME contains a special file")
		}
		return nil
	})
}

func npmCommandPath(path string) bool {
	parent := filepath.Dir(path)
	return strings.HasPrefix(path, runtimeimage.PiNPMDirectory+"/") && filepath.Base(parent) == ".bin" && filepath.Base(filepath.Dir(parent)) == "node_modules"
}
