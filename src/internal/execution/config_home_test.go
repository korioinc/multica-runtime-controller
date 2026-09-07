package execution

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func configFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCopiedConfigurationKeepsTaskChangesPrivate(t *testing.T) {
	source, first, second := t.TempDir(), t.TempDir(), t.TempDir()
	payload := filepath.Join(source, "..generation")
	settings := filepath.Join(payload, "settings.json")
	configFile(t, settings, "operator settings")
	if err := os.Symlink("..generation", filepath.Join(source, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..data/settings.json", filepath.Join(source, "settings.json")); err != nil {
		t.Fatal(err)
	}
	for _, home := range []string{first, second} {
		if err := copyHomeConfig(home, source, wire.Home+"/.pi/agent"); err != nil {
			t.Fatal(err)
		}
	}
	destination := filepath.Join(first, ".pi/agent/settings.json")
	configFile(t, destination, "task customization")
	if err := copyHomeConfig(first, source, wire.Home+"/.pi/agent"); err != nil {
		t.Fatal(err)
	}
	changed, err := os.ReadFile(destination)
	if err != nil || string(changed) != "task customization" {
		t.Fatal("reinitialization replaced the task's customization", err)
	}
	original, err := os.ReadFile(settings)
	if err != nil || string(original) != "operator settings" {
		t.Fatal("task customization changed the operator input", err)
	}
	other, err := os.ReadFile(filepath.Join(second, ".pi/agent/settings.json"))
	if err != nil || string(other) != "operator settings" {
		t.Fatal("task customization changed another task's configuration", err)
	}
}

func TestConfigurationCopyCannotReplaceAssignedSkills(t *testing.T) {
	source, home := t.TempDir(), t.TempDir()
	assigned := filepath.Join(home, ".codex/skills/approved.md")
	configFile(t, assigned, "assigned task instructions")
	configFile(t, filepath.Join(source, "skills/approved.md"), "operator replacement")
	if err := copyHomeConfig(home, source, wire.Home+"/.codex"); err == nil {
		t.Fatal("configuration gained authority over assigned task skills")
	}
	got, err := os.ReadFile(assigned)
	if err != nil || string(got) != "assigned task instructions" {
		t.Fatal("configuration replaced the assigned task instructions", err)
	}
}

func TestConfigurationCopyDoesNotFollowNativeHomeLinks(t *testing.T) {
	source, home, outside := t.TempDir(), t.TempDir(), t.TempDir()
	input := filepath.Join(source, "config.toml")
	configFile(t, input, "operator input")
	protected := filepath.Join(outside, "config.toml")
	configFile(t, protected, "unrelated private file")
	if err := os.Symlink(outside, filepath.Join(home, ".codex")); err != nil {
		t.Fatal(err)
	}
	if err := copyHomeConfig(home, input, wire.Home+"/.codex/config.toml"); err == nil {
		t.Fatal("configuration copy followed a native HOME directory link")
	}
	got, err := os.ReadFile(protected)
	if err != nil || string(got) != "unrelated private file" {
		t.Fatal("configuration changed data outside its HOME", err)
	}
}

func TestConfigurationProjectionCannotReadOutsideItsInput(t *testing.T) {
	source, home, outside := t.TempDir(), t.TempDir(), t.TempDir()
	configFile(t, filepath.Join(outside, "private.txt"), "ungranted credential")
	if err := os.Symlink(outside, filepath.Join(source, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := copyHomeConfig(home, source, wire.Home+"/.codex"); err == nil {
		t.Fatal("projection obtained access outside the operator input")
	}
	if _, err := os.Stat(filepath.Join(home, ".codex/private.txt")); !os.IsNotExist(err) {
		t.Fatal("projection disclosed ungranted data to the task", err)
	}
}
