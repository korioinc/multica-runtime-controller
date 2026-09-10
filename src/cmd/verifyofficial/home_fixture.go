package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	configurationbundle "github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

const (
	nativeHomeTaskToken      = "mat_disposable_home_task"
	homeConfiguration        = "model = \"fixture-model\"\n"
	homeBaseInstructions     = "# Committed review instructions\n"
	homeBaseReference        = "Committed reference\n"
	homeGlobalInstructions   = "# Unrelated global instructions\n"
	homeAssignedInstructions = "# Assigned review instructions\n"
	homeAssignedReference    = "Assigned reference\n"
)

type nativeHomeFixture struct {
	directory string
	bundle    configurationbundle.Bundle
	ref       runtimeimage.Ref
	forbidden [][]byte
}

func (v *verifier) prepareNativeHomeFixture() (*nativeHomeFixture, func() error, error) {
	directory, err := os.MkdirTemp("/tmp", "official-home-conversion-")
	if err != nil {
		return nil, nil, err
	}
	canonical, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return nil, nil, errors.Join(err, os.RemoveAll(directory))
	}
	directory = canonical
	fixture := &nativeHomeFixture{directory: directory}
	var restore func() error
	cleanup := func() error {
		var err error
		if restore != nil {
			err = restore()
		}
		return errors.Join(err, os.RemoveAll(directory))
	}
	inputs := map[string]string{
		"config.toml":                           homeConfiguration,
		"skills/Code Review/SKILL.md":           homeBaseInstructions,
		"skills/Code Review/references/base.md": homeBaseReference,
		"skills/global/SKILL.md":                homeGlobalInstructions,
	}
	operator := filepath.Join(directory, "provider")
	for name, content := range inputs {
		if err := writeHomeFixtureFile(filepath.Join(operator, name), []byte(content)); err != nil {
			return nil, nil, errors.Join(err, cleanup())
		}
	}
	selected := filepath.Join(directory, "selected")
	if err := os.Mkdir(selected, 0700); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	fixture.bundle, err = configurationbundle.CaptureOrRead(selected, []configurationbundle.Copy{{SourceGroup: "provider", Source: operator, Target: wire.Home + "/.codex"}}, directory)
	if err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	fixture.ref, err = v.descriptor.Reference(v.selection.RuntimeRef.Image, v.descriptorDigest, fixture.bundle.Digest)
	if err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	restore, err = isolateNativeHome()
	if err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	for name, content := range inputs {
		if err := writeHomeFixtureFile(filepath.Join(wire.Home, ".codex", name), []byte(content)); err != nil {
			return nil, nil, errors.Join(err, cleanup())
		}
	}
	// These live controller changes are outside the committed configuration.
	// Actual daemon auth/global links must not turn them into prepared HOME data.
	marker := uuid.NewString()
	privateAuth := "sk-disposable-controller-home-" + marker
	privateSession := "private controller session " + marker
	sessionData, err := json.Marshal(map[string]any{"type": "message", "message": map[string]any{"role": "assistant", "content": []any{map[string]string{"type": "text", "text": privateSession}}}})
	if err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	mutatedReview := "# Mutable controller review " + marker + "\n"
	mutatedGlobal := "# Mutable controller global " + marker + "\n"
	mutatedConfiguration := "model = \"unselected-" + marker + "\"\n"
	for name, content := range map[string]string{
		".codex/auth.json":                              `{"OPENAI_API_KEY":"` + privateAuth + `"}`,
		".codex/sessions/controller-private.jsonl":      string(sessionData) + "\n",
		".multica/pi-sessions/controller-private.jsonl": string(sessionData) + "\n",
		".codex/config.toml":                            mutatedConfiguration,
		".codex/skills/Code Review/SKILL.md":            mutatedReview,
		".codex/skills/global/SKILL.md":                 mutatedGlobal,
	} {
		if err := writeHomeFixtureFile(filepath.Join(wire.Home, name), []byte(content)); err != nil {
			return nil, nil, errors.Join(err, cleanup())
		}
	}
	for _, content := range []string{privateAuth, privateSession, mutatedReview, mutatedGlobal, mutatedConfiguration} {
		fixture.forbidden = append(fixture.forbidden, []byte(content))
	}
	return fixture, cleanup, nil
}

// The suite already uses wire.Home. Temporarily isolate only the native trees
// needed by this final probe, then restore the prior suite data without copying
// mutable controller content into the selected operator bundle.
func isolateNativeHome() (func() error, error) {
	root, err := os.OpenRoot(wire.Home)
	if err != nil {
		return nil, err
	}
	backup := ".home-proof-backup-" + uuid.NewString()
	if err := root.Mkdir(backup, 0700); err != nil {
		root.Close()
		return nil, err
	}
	var owned []string
	saved := map[string]bool{}
	restore := func() error {
		var failures []error
		for _, name := range owned {
			if err := root.RemoveAll(name); err != nil {
				failures = append(failures, err)
				continue
			}
			if saved[name] {
				if err := root.Rename(filepath.Join(backup, name), name); err != nil {
					failures = append(failures, err)
				}
			}
		}
		// Retain the backup if restoration failed; never erase prior suite data.
		if len(failures) == 0 {
			failures = append(failures, root.Remove(backup))
		}
		failures = append(failures, root.Close())
		return errors.Join(failures...)
	}
	for _, name := range []string{".codex", ".multica"} {
		if _, err := root.Lstat(name); err == nil {
			if err := root.Rename(name, filepath.Join(backup, name)); err != nil {
				return nil, errors.Join(err, restore())
			}
			saved[name] = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, errors.Join(err, restore())
		}
		owned = append(owned, name)
		if err := root.Mkdir(name, 0700); err != nil {
			return nil, errors.Join(err, restore())
		}
	}
	return restore, nil
}

func writeHomeFixtureFile(path string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return os.WriteFile(path, content, 0600)
}

func writeNativeHomeUpdate(observed homeObservation) error {
	if observed.Home == wire.Home || !strings.HasPrefix(observed.CodexHome, observed.Home+"/") {
		return errors.New("private update requires the converted HOME")
	}
	auth, err := json.Marshal(map[string]string{"fixture": "private provider credential " + observed.TaskID})
	if err != nil {
		return err
	}
	if err := writeHomeFixtureFile(filepath.Join(observed.CodexHome, "auth.json"), auth); err != nil {
		return err
	}
	return writeHomeFixtureFile(filepath.Join(observed.CodexHome, "sessions/provider-private.jsonl"), []byte("private provider session "+observed.TaskID+"\n"))
}
