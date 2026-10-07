package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestExecutableMutationRejected(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "runtime")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}
	hash, err := HashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contract := Contract{BuildID: hash, Platform: "linux/amd64", RuntimePath: path, RuntimeSHA256: hash, GoVersion: "go1.26.1"}
	raw, err := json.Marshal(contract)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "build.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Check(root, "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho replaced\n"), 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := Check(root, "linux/amd64"); err == nil {
		t.Fatal("modified executable was admitted")
	}
}
