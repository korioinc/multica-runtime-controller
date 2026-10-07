package worker

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
)

func TestPiMCPRefreshesNativeConsumersAndRestoresHome(t *testing.T) {
	for _, originalPresent := range []bool{false, true} {
		name := "absent_baseline"
		if originalPresent {
			name = "operator_baseline"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			if err := os.MkdirAll(filepath.Join(directory, ".pi/agent"), 0700); err != nil {
				t.Fatal(err)
			}
			home, err := os.OpenRoot(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer home.Close()
			original := []byte(`{"mcpServers":{"operator":{"url":"https://operator.example/mcp"}}}`)
			paths := []string{piMCPConfigPath, piAdapterMCPPath}
			if originalPresent {
				for _, path := range paths {
					if err := configuration.WriteFile(home, path, original, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			baseline, err := captureManagedHome(directory, configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)})
			if err != nil {
				t.Fatal(err)
			}
			for _, payload := range [][]byte{[]byte(`{"mcpServers":{"current":{"url":"https://current.example/mcp","headers":{"Authorization":"Bearer task-only"}}}}`), nil} {
				if err := configurePiMCP(home, payload); err != nil {
					t.Fatal(err)
				}
				expected := payload
				if len(expected) == 0 {
					expected = []byte(`{"mcpServers":{}}`)
				}
				for _, path := range paths {
					actual, err := home.ReadFile(path)
					if err != nil || !bytes.Equal(actual, expected) {
						t.Fatal("a native Pi consumer retained another task's MCP configuration", path, err)
					}
					info, err := home.Stat(path)
					if err != nil || info.Mode().Perm() != 0600 {
						t.Fatal("task MCP configuration is not private", path, err)
					}
				}
				if err := baseline.restore(); err != nil {
					t.Fatal(err)
				}
				for _, path := range paths {
					actual, err := home.ReadFile(path)
					if originalPresent && (err != nil || !bytes.Equal(actual, original)) {
						t.Fatal("turn cleanup did not restore the original operator MCP file", path, err)
					}
					if !originalPresent && !os.IsNotExist(err) {
						t.Fatal("turn cleanup retained a task-created MCP credential file", path, err)
					}
				}
			}
		})
	}
}

func TestPiMCPRejectsIncompletePublication(t *testing.T) {
	directory := t.TempDir()
	if err := os.MkdirAll(filepath.Join(directory, piAdapterMCPPath), 0700); err != nil {
		t.Fatal(err)
	}
	home, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer home.Close()
	if err := configurePiMCP(home, nil); err == nil {
		t.Fatal("one configured native consumer hid another consumer's publication failure")
	}
}
