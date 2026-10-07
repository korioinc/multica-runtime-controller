package configuration

import (
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"os"
	"path/filepath"
	"testing"
)

func TestHomeConfigurationPreservesExistingFiles(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "settings")
	if err := os.WriteFile(path, []byte("image default"), 0600); err != nil {
		t.Fatal(err)
	}
	managed := File{Target: "operator", Mode: 0600, Content: []byte("current operator configuration")}
	managed.SHA256 = core.Digest(managed.Content)
	if err := os.WriteFile(filepath.Join(root, managed.Target), []byte("image default configuration"), 0600); err != nil {
		t.Fatal(err)
	}
	groups := []Group{{Name: "operator", Directories: []string{}, Files: []File{managed}}}
	bundle := Bundle{Groups: groups, Digest: Digest(groups)}
	if err := ApplyHomeConfiguration(root, bundle); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "image default" {
		t.Fatal("configuration changed image-provided data", err)
	}
	got, err = os.ReadFile(filepath.Join(root, managed.Target))
	if err != nil || string(got) != string(managed.Content) {
		t.Fatal("operator configuration did not replace the image default", err)
	}
	if err := os.WriteFile(path, []byte("current local state"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ApplyHomeConfiguration(root, bundle); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(path)
	if err != nil || string(got) != "current local state" {
		t.Fatal("main initialization replaced current data", err)
	}
}

func TestHomeConfigurationCannotWriteOutsideItsRoot(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "settings")
	if err := os.WriteFile(outside, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Dir(outside), filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	file := File{Target: "escape/settings", Mode: 0600, Content: []byte("operator credential")}
	file.SHA256 = core.Digest(file.Content)
	groups := []Group{{Name: "operator", Directories: []string{}, Files: []File{file}}}
	if err := ApplyHomeConfiguration(root, Bundle{Groups: groups, Digest: Digest(groups)}); err == nil {
		t.Fatal("configuration escaped HOME")
	}
	got, err := os.ReadFile(outside)
	if err != nil || string(got) != "private" {
		t.Fatal("configuration overwrote data outside HOME", err)
	}
}

func TestHomeConfigurationCannotReplaceImageChromeProfile(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := filepath.Rel(Home, ChromeProfileRoot)
	if err != nil {
		t.Fatal(err)
	}
	preferences := filepath.Join(profile, "Default", "Preferences")
	if err := os.MkdirAll(filepath.Join(home, filepath.Dir(preferences)), 0700); err != nil {
		t.Fatal(err)
	}
	original := []byte("retain the image profile")
	if err := os.WriteFile(filepath.Join(home, preferences), original, 0600); err != nil {
		t.Fatal(err)
	}
	operator := File{Target: "operator", Mode: 0600, Content: []byte("new operator input")}
	operator.SHA256 = core.Digest(operator.Content)
	protected := File{Target: preferences, Mode: 0600, Content: []byte("replace the image profile")}
	protected.SHA256 = core.Digest(protected.Content)
	groups := []Group{
		{Name: "operator", Directories: []string{}, Files: []File{operator}},
		{Name: "profile", Directories: []string{filepath.Dir(profile)}, Files: []File{protected}},
	}
	bundle := Bundle{Groups: groups, Digest: Digest(groups)}
	_ = ApplyHomeConfiguration(home, bundle)
	got, err := os.ReadFile(filepath.Join(home, preferences))
	if err != nil || string(got) != string(original) {
		t.Fatal("operator input replaced the original image profile", err)
	}
	if _, err := os.Lstat(filepath.Join(home, operator.Target)); !os.IsNotExist(err) {
		t.Fatal("rejected configuration partially changed HOME", err)
	}
	// Retained serialized bundles still validate for historical recovery.
	if err := bundle.Validate(); err != nil {
		t.Fatal(err)
	}
	input := t.TempDir()
	source := filepath.Join(input, "operator")
	if err := os.MkdirAll(filepath.Join(source, filepath.Base(profile), "Default"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, filepath.Base(profile), "Default", "Preferences"), protected.Content, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture([]Copy{{SourceGroup: "operator", Source: source, Target: filepath.Dir(ChromeProfileRoot)}}, input); err == nil {
		t.Fatal("parent mapping captured configuration for the retained image profile")
	}
	if err := os.RemoveAll(filepath.Join(source, filepath.Base(profile))); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "harmless-settings"), operator.Content, 0600); err != nil {
		t.Fatal(err)
	}
	allowed, err := Capture([]Copy{{SourceGroup: "operator", Source: source, Target: filepath.Dir(ChromeProfileRoot)}}, input)
	if err != nil {
		t.Fatal(err)
	}
	if err := ApplyHomeConfiguration(home, allowed); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(filepath.Join(home, filepath.Dir(profile), "harmless-settings"))
	if err != nil || string(got) != string(operator.Content) {
		t.Fatal("harmless parent configuration was not applied", err)
	}
}
