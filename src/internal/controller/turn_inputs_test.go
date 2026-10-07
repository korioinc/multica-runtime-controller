package controller

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeSelectorsNeverFollowTaskDirectoryRedirection(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	project, private := filepath.Join(base, "workdir"), filepath.Join(base, "private")
	for _, path := range []string{project, filepath.Join(private, ".codex")} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	secret := filepath.Join(private, ".codex/config.toml")
	privateConfig := []byte("model = 'private-selector'")
	if err := os.WriteFile(secret, privateConfig, 0600); err != nil {
		t.Fatal(err)
	}
	if _, known := nativeSelectorFiles(project, []string{".codex/config.toml"}); !known {
		t.Fatal("an observed empty directory was treated as missing evidence")
	}
	if err := os.Symlink(private, filepath.Join(base, "redirected-workdir")); err != nil {
		t.Fatal(err)
	}
	if files, known := nativeSelectorFiles(filepath.Join(base, "redirected-workdir"), []string{".codex/config.toml"}); known || bytes.Equal(files[".codex/config.toml"], privateConfig) {
		t.Fatal("redirected workdir disclosed another root's selectors")
	}
	if err := os.Symlink(filepath.Join(private, ".codex"), filepath.Join(project, ".codex")); err != nil {
		t.Fatal(err)
	}
	if files, known := nativeSelectorFiles(project, []string{".codex/config.toml"}); known || bytes.Equal(files[".codex/config.toml"], privateConfig) {
		t.Fatal("redirected selector directory was read")
	}
	if err := os.Remove(filepath.Join(project, ".codex")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(project, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(secret, filepath.Join(project, ".codex/config.toml")); err != nil {
		t.Fatal(err)
	}
	if files, known := nativeSelectorFiles(project, []string{".codex/config.toml"}); known || bytes.Equal(files[".codex/config.toml"], privateConfig) {
		t.Fatal("hardlinked selector crossed workspace ownership")
	}
}
