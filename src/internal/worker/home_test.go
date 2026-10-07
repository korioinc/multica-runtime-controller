package worker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func TestManagedHomeDoesNotCarryAnEarlierTurnCredential(t *testing.T) {
	home := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, ".pi/agent"), 0700); err != nil {
		t.Fatal(err)
	}
	authPath := filepath.Join(home, ".pi/agent/auth.json")
	writeCredential := func(key string) {
		t.Helper()
		raw, err := json.Marshal(map[string]any{"openai": map[string]string{"type": "api_key", "key": key}})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(authPath, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	original := uuid.NewString()
	writeCredential(original)
	baseline, err := captureManagedHome(home, configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)})
	if err != nil {
		t.Fatal(err)
	}
	writeCredential(uuid.NewString())
	trust := filepath.Join(home, ".pi/agent/trust.json")
	if err := os.WriteFile(trust, []byte(`{"trusted":["/workspace"]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := baseline.restore(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(authPath)
	var credentials map[string]struct {
		Key string `json:"key"`
	}
	if err != nil || json.Unmarshal(raw, &credentials) != nil || credentials["openai"].Key != original {
		t.Fatal("the next turn inherited a credential written by the previous provider", err)
	}
	if _, err := os.Lstat(trust); !os.IsNotExist(err) {
		t.Fatal("the next turn inherited provider-written project trust", err)
	}
}

func TestManagedHomeRestoresClaudeConfigurationAndRemovesGeneratedCredentials(t *testing.T) {
	home := t.TempDir()
	config := filepath.Join(home, ".claude")
	if err := os.Mkdir(config, 0700); err != nil {
		t.Fatal(err)
	}
	settings := []byte(`{"model":"claude-sonnet-4-6","effortLevel":"high"}`)
	if err := os.WriteFile(filepath.Join(config, "settings.json"), settings, 0600); err != nil {
		t.Fatal(err)
	}
	baseline, err := captureManagedHome(home, configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".claude/settings.json", ".claude/settings.local.json", ".claude/.credentials.json", ".claude.json"} {
		if err := os.WriteFile(filepath.Join(home, name), []byte(`{"previousTurn":"private"}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := baseline.restore(); err != nil {
		t.Fatal(err)
	}
	if actual, err := os.ReadFile(filepath.Join(config, "settings.json")); err != nil || string(actual) != string(settings) {
		t.Fatal("Claude changed the next turn's captured defaults", err)
	}
	for _, name := range []string{".claude/settings.local.json", ".claude/.credentials.json", ".claude.json"} {
		if _, err := os.Lstat(filepath.Join(home, name)); !os.IsNotExist(err) {
			t.Fatal("a previous turn's private Claude state survived restoration", name, err)
		}
	}
}

func TestManagedHomeRestoresSkillRootsAndClearsTaskPackageCredentials(t *testing.T) {
	home := t.TempDir()
	operator := filepath.Join(home, ".pi/agent/skills/operator/SKILL.md")
	if err := os.MkdirAll(filepath.Dir(operator), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(operator, []byte("original operator skill"), 0600); err != nil {
		t.Fatal(err)
	}
	profile := filepath.Join(home, strings.TrimPrefix(wire.ChromeProfileRoot, wire.Home+"/"), "Default", "Preferences")
	if err := os.MkdirAll(filepath.Dir(profile), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(profile, []byte("image browser preferences"), 0600); err != nil {
		t.Fatal(err)
	}
	baseline, err := captureManagedHome(home, configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(operator, []byte("earlier task instructions"), 0600); err != nil {
		t.Fatal(err)
	}
	retained := []byte("live browser preferences")
	if err := os.WriteFile(profile, retained, 0600); err != nil {
		t.Fatal(err)
	}
	paths := []string{".pi/agent/skills/task/SKILL.md", ".claude/skills/task/SKILL.md", ".npmrc", ".cargo/config.toml", ".cargo/config", ".config/pip/pip.conf", ".config/composer/config.json", ".composer/config.json"}
	for _, path := range paths {
		full := filepath.Join(home, path)
		if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("synthetic old task credential"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	desktop := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, ".cache"), 0700); err != nil {
		t.Fatal(err)
	}
	discovery := filepath.Join(home, ".cache/cua-driver")
	if err := os.Symlink(desktop, discovery); err != nil {
		t.Fatal(err)
	}
	if err := baseline.restore(); err != nil {
		t.Fatal(err)
	}
	if current, err := os.ReadFile(operator); err != nil || string(current) != "original operator skill" {
		t.Fatal("task edits changed the next turn's original skill baseline", err)
	}
	for _, path := range paths {
		if _, err := os.Lstat(filepath.Join(home, path)); !os.IsNotExist(err) {
			t.Fatal("earlier task private data survived HOME reset", path, err)
		}
	}
	if target, err := os.Readlink(discovery); err != nil || target != desktop {
		t.Fatal("task HOME reset changed resident Cua discovery", err)
	}
	if current, err := os.ReadFile(profile); err != nil || string(current) != string(retained) {
		t.Fatal("task HOME reset overwrote the live Chrome profile", err)
	}
}
