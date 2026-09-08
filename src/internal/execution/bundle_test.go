package execution

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func TestCommittedBundleSurvivesProjectionChangeAndPartialHomeCopy(t *testing.T) {
	input, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	run, home, seed := t.TempDir(), t.TempDir(), t.TempDir()
	group := filepath.Join(input, "provider")
	for _, generation := range []string{"..a", "..b"} {
		configFile(t, filepath.Join(group, generation, "a.txt"), generation+" first")
		configFile(t, filepath.Join(group, generation, "b.txt"), generation+" second")
	}
	projection := filepath.Join(group, "..data")
	if err := os.Symlink("..a", projection); err != nil {
		t.Fatal(err)
	}
	copies := []configuration.Copy{{SourceGroup: "provider", Source: group, Target: wire.Home + "/.fixture"}}
	committed, err := configuration.CaptureOrRead(run, copies, input)
	if err != nil {
		t.Fatal(err)
	}
	// The first HOME file is written; a real destination conflict interrupts
	// copying the second. The selected input has already committed atomically.
	blocker := filepath.Join(home, ".fixture/b.txt")
	if err := os.MkdirAll(blocker, 0700); err != nil {
		t.Fatal(err)
	}
	if err := CopyBundle(home, committed); err == nil {
		t.Fatal("copy fixture did not reach its partial failure")
	}
	if err := os.Remove(projection); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..b", projection); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	retried, err := configuration.CaptureOrRead(run, copies, input)
	if err != nil {
		t.Fatal(err)
	}
	if err := CopyBundle(home, retried); err != nil {
		t.Fatal(err)
	}
	configFile(t, filepath.Join(seed, ".fixture/a.txt"), "image default")
	configFile(t, filepath.Join(seed, ".fixture/default.txt"), "missing-file default")
	if err := copyHomeSeed(home, seed); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{"a.txt": "..a first", "b.txt": "..a second", "default.txt": "missing-file default"} {
		got, err := os.ReadFile(filepath.Join(home, ".fixture", path))
		if err != nil || string(got) != want {
			t.Fatalf("retry mixed configuration or replaced operator file %s: %v", path, err)
		}
	}
}
