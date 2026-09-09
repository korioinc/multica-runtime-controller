package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func verifyProviderPrompt(report *providerResult) error {
	prompt, err := io.ReadAll(io.LimitReader(os.Stdin, 8<<20))
	if err != nil {
		return err
	}
	if !bytes.Contains(prompt, []byte("Verify the local runtime repository")) {
		return errors.New("actual official natural-language prompt did not reach the provider")
	}
	report.ContinuityNotice = strings.Contains(strings.ToLower(string(prompt)), "could not be restored")
	return nil
}

func verifyProviderEnvironment(report *providerResult) error {
	if err := verifyMounts(report.WorkDir); err != nil {
		return err
	}
	report.ROChecked, report.WritableChecked, report.IsolationChecked = true, true, true
	cache := os.Getenv("FIXTURE_CACHE")
	expectedCache := filepath.Join(report.WorkDir, ".cache")
	if override := os.Getenv("VERIFYRUNTIME_EXPECT_CACHE"); override != "" {
		expectedCache = override
	}
	if cache != expectedCache {
		return fmt.Errorf("manifest/task environment expansion selected wrong cache: got %q expected %q", cache, expectedCache)
	}
	if err := os.MkdirAll(cache, 0700); err != nil {
		return err
	}
	cacheFile, err := os.CreateTemp(cache, "fixture-cache-")
	if err != nil {
		return err
	}
	_, err = cacheFile.Write([]byte("task-local cache"))
	cacheFile.Close()
	os.Remove(cacheFile.Name())
	if err != nil {
		return err
	}
	report.CachePath, report.CacheChecked = cache, true
	actualHash, err := core.HashFile(wire.ControllerRoot + "/runtime")
	if err != nil {
		return err
	}
	report.CoreHash = actualHash
	if actualHash != report.RuntimeRef.Controller.RuntimeSHA256 {
		return errors.New("worker core differs from its immutable request")
	}
	return nil
}

func verifyMounts(cwd string) error {
	for _, root := range []string{wire.ControllerRoot, runtimeimage.Root} {
		file, err := os.CreateTemp(root, "fixture-write-probe-")
		if err == nil {
			file.Close()
			os.Remove(file.Name())
			return errors.New("worker can modify immutable core or tools")
		}
		if !errors.Is(err, os.ErrPermission) && !strings.Contains(strings.ToLower(err.Error()), "read-only") {
			return fmt.Errorf("unexpected immutable mount failure: %w", err)
		}
	}
	for _, root := range []string{wire.Home, "/tmp", cwd} {
		file, err := os.CreateTemp(root, "fixture-writable-")
		if err != nil {
			return err
		}
		_, err = file.Write([]byte("private writable data"))
		file.Close()
		os.Remove(file.Name())
		if err != nil {
			return err
		}
	}
	protected := []string{"/workspace/.multica-runtime/state", "/workspace/.multica-runtime/attempts", "/workspace/.multica-runtime/workers", "/workspace/.repos", "/var/run/secrets/kubernetes.io/serviceaccount/token"}
	if root := os.Getenv("VERIFYRUNTIME_FORBIDDEN_ROOT"); root != "" {
		protected = append(protected, root)
	}
	for _, path := range protected {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("worker can reach unassigned controller or task storage: %s", path)
		}
	}
	return nil
}
