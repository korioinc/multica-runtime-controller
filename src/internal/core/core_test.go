package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func artifactFixture(t *testing.T) string {
	t.Helper()
	source := t.TempDir()
	for _, name := range []string{"runtime"} {
		if err := os.WriteFile(filepath.Join(source, name), []byte("#!/bin/sh\nexit 0\n"), 0555); err != nil {
			t.Fatal(err)
		}
	}
	hash, err := HashFile(filepath.Join(source, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(source, "shims"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(source, "disabled"), 0555); err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{}
	for _, alias := range []string{"pi", "codex", "copilot", "agy"} {
		paths[alias] = filepath.Join(source, "shims", alias)
		if err := os.Link(filepath.Join(source, "runtime"), paths[alias]); err != nil {
			t.Fatal(err)
		}
	}
	contract := Contract{SchemaVersion: Version, ControllerABI: ABI, BuildID: hash, Platform: "linux/amd64", RuntimePath: filepath.Join(source, "runtime"), RuntimeSHA256: hash, ShimPaths: paths, GoVersion: "go1.26.1"}
	b, _ := json.Marshal(contract)
	if err = os.WriteFile(filepath.Join(source, "build.json"), b, 0444); err != nil {
		t.Fatal(err)
	}
	return source
}

func TestExecutableMutationInvalidatesEveryConsumer(t *testing.T) {
	target := artifactFixture(t)
	var err error
	if _, err = Check(target, "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(target, "shims", "pi")
	if err = os.Chmod(alias, 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(alias, []byte("#!/bin/sh\necho replaced\n"), 0755); err != nil {
		t.Fatal(err)
	}
	if _, err = Check(target, "linux/amd64"); err == nil {
		t.Fatal("consumer accepted mutation through a shared executable alias")
	}
}
