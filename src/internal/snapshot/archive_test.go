package snapshot_test

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/snapshot"
)

func TestExtractionCannotModifyFilesOutsideItsDestination(t *testing.T) {
	for _, attempt := range []string{"parent traversal", "absolute file", "symlink parent", "hardlink"} {
		t.Run(attempt, func(t *testing.T) {
			outside := t.TempDir()
			protected := filepath.Join(outside, "protected")
			if err := os.WriteFile(protected, []byte("other task's data"), 0o600); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(outside, "stage")
			if err := os.Mkdir(destination, 0o700); err != nil {
				t.Fatal(err)
			}
			var archive bytes.Buffer
			tw := tar.NewWriter(&archive)
			name := "../protected"
			switch attempt {
			case "absolute file":
				name = protected
			case "symlink parent":
				if err := tw.WriteHeader(&tar.Header{Name: "parent", Typeflag: tar.TypeSymlink, Linkname: ".."}); err != nil {
					t.Fatal(err)
				}
				name = "parent/protected"
			case "hardlink":
				if err := tw.WriteHeader(&tar.Header{Name: "linked", Typeflag: tar.TypeLink, Linkname: protected}); err != nil {
					t.Fatal(err)
				}
				name = "linked"
			}
			if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Size: 9, Mode: 0o600}); err != nil {
				t.Fatal(err)
			}
			if _, err := tw.Write([]byte("overwrite")); err != nil {
				t.Fatal(err)
			}
			if err := tw.Close(); err != nil {
				t.Fatal(err)
			}
			_ = snapshot.Extract(&archive, destination)
			raw, err := os.ReadFile(protected)
			if err != nil || string(raw) != "other task's data" {
				t.Fatal("extraction changed another task's data")
			}
		})
	}
}
