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
	for _, name := range []string{"runtime", "multica"} {
		if err := os.WriteFile(filepath.Join(source, name), []byte("#!/bin/sh\nexit 0\n"), 0555); err != nil {
			t.Fatal(err)
		}
	}
	hash, err := HashFile(filepath.Join(source, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	contract := Contract{ContractVersion: 1, BuildID: "fixture", Platform: "linux/amd64", OfficialVersion: "0.4.40", OfficialSHA256: hash, Files: map[string]string{"runtime": hash, "multica": hash}}
	b, _ := json.Marshal(contract)
	if err = os.WriteFile(filepath.Join(source, "contract.json"), b, 0444); err != nil {
		t.Fatal(err)
	}
	return source
}

func TestExecutableMutationInvalidatesEveryConsumer(t *testing.T) {
	source := artifactFixture(t)
	var err error
	target := filepath.Join(t.TempDir(), "core")
	if _, err = Materialize(source, target, "linux/amd64"); err != nil {
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
	if _, err = Materialize(source, target, "linux/amd64"); err == nil {
		t.Fatal("materializer silently replaced a changed core already available to consumers")
	}
}

func TestInterruptedMaterializationResumesWithoutReplacingTheMountedRoot(t *testing.T) {
	source := artifactFixture(t)
	target := t.TempDir()
	before, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	// An interrupted copy can leave a truncated executable and only part of the
	// shim directory. No completion contract has been published yet.
	if err = os.WriteFile(filepath.Join(target, "runtime"), []byte("partial executable"), 0555); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(target, "shims"), 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.Link(filepath.Join(target, "runtime"), filepath.Join(target, "shims", "pi")); err != nil {
		t.Fatal(err)
	}
	if _, err = Materialize(source, target, "linux/amd64"); err != nil {
		t.Fatal("interrupted init could not resume", err)
	}
	if _, err = Check(target, "linux/amd64"); err != nil {
		t.Fatal("resumed materialization is not consumable", err)
	}
	after, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("retry replaced the mounted root")
	}
}
