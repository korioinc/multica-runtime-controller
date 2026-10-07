package runtimeimage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
)

func TestInstalledCodexHelperBindingRejectsChangedImageInputs(t *testing.T) {
	if runtime.GOOS != "linux" || os.Getenv("MULTICA_NATIVE_CODEX_REUSE") != "1" {
		t.Skip("requires an installed Linux runtime")
	}
	d, _, err := ReadInstalled(Root, core.Root, core.HostPlatform())
	if err != nil {
		t.Fatal(err)
	}
	native, err := CodexHelperExecutable(d)
	if err != nil {
		t.Fatal("installed native helper binding failed", err)
	}
	for _, boundary := range []string{"launcher hash", "version", "platform"} {
		t.Run(boundary, func(t *testing.T) {
			changed := d
			provider := d.Providers["codex"]
			switch boundary {
			case "launcher hash":
				provider.SHA256 = strings.Repeat("0", 64)
			case "version":
				provider.Version = "0.0.0"
			case "platform":
				changed.Platform = "linux/unsupported"
			}
			changed.Providers = map[string]Executable{"codex": provider}
			if _, err := CodexHelperExecutable(changed); err == nil {
				t.Fatal("changed descriptor acquired native helper authority")
			}
		})
	}
	if os.Geteuid() != 0 {
		return
	}
	launcher, err := filepath.EvalSymlinks(d.Providers["codex"].Path)
	if err != nil {
		t.Fatal(err)
	}
	launcherBytes, err := os.ReadFile(launcher)
	if err != nil {
		t.Fatal(err)
	}
	nativeBytes, err := os.ReadFile(native.Path)
	if err != nil {
		t.Fatal(err)
	}
	var cpu string
	if d.Platform == "linux/arm64" {
		cpu = "arm64"
	} else {
		cpu = "x64"
	}
	for _, boundary := range []string{"native version", "native platform", "writable image parent", "indirect image parent"} {
		t.Run(boundary, func(t *testing.T) {
			fixture := filepath.Join("/opt", "multica-codex-binding-proof-"+uuid.NewString())
			t.Cleanup(func() { _ = os.RemoveAll(fixture) })
			packageRoot := filepath.Join(fixture, "node_modules/@openai/codex")
			nativeRoot := filepath.Join(filepath.Dir(packageRoot), "codex-linux-"+cpu)
			for _, path := range []string{filepath.Join(packageRoot, "bin"), nativeRoot} {
				if err := os.MkdirAll(path, 0755); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(packageRoot, "bin/codex.js")
			if err := os.WriteFile(path, launcherBytes, 0555); err != nil {
				t.Fatal(err)
			}
			version := d.Providers["codex"].Version
			metadata, _ := json.Marshal(map[string]any{"name": "@openai/codex", "version": version,
				"optionalDependencies": map[string]string{"@openai/codex-linux-" + cpu: "npm:@openai/codex@" + version + "-linux-" + cpu}})
			if err := os.WriteFile(filepath.Join(packageRoot, "package.json"), metadata, 0644); err != nil {
				t.Fatal(err)
			}
			nativeVersion, nativeCPU := version+"-linux-"+cpu, cpu
			metadata, _ = json.Marshal(map[string]any{"name": "@openai/codex", "version": nativeVersion, "os": []string{"linux"}, "cpu": []string{nativeCPU}})
			if err := os.WriteFile(filepath.Join(nativeRoot, "package.json"), metadata, 0644); err != nil {
				t.Fatal(err)
			}
			triple := map[string]string{"arm64": "aarch64-unknown-linux-musl", "x64": "x86_64-unknown-linux-musl"}[cpu]
			binary := filepath.Join(nativeRoot, "vendor", triple, "bin", "codex")
			if err := os.MkdirAll(filepath.Dir(binary), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(binary, nativeBytes, 0555); err != nil {
				t.Fatal(err)
			}
			provider := d.Providers["codex"]
			provider.Path = path
			changed := d
			changed.Providers = map[string]Executable{"codex": provider}
			if _, err := CodexHelperExecutable(changed); err != nil {
				t.Fatal("complete immutable package fixture was rejected before mutation", err)
			}
			if boundary == "native version" || boundary == "native platform" {
				if boundary == "native version" {
					nativeVersion = "wrong-version"
				} else {
					nativeCPU = "wrong-cpu"
				}
				metadata, _ = json.Marshal(map[string]any{"name": "@openai/codex", "version": nativeVersion, "os": []string{"linux"}, "cpu": []string{nativeCPU}})
				if err := os.WriteFile(filepath.Join(nativeRoot, "package.json"), metadata, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "writable image parent" {
				if err := os.Chmod(packageRoot, 0777); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "indirect image parent" {
				if err := os.Rename(nativeRoot, nativeRoot+"-retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(nativeRoot+"-retained", nativeRoot); err != nil {
					t.Fatal(err)
				}
			}
			_, err := CodexHelperExecutable(changed)
			reason := "native package differs"
			if boundary == "writable image parent" {
				reason = "indirect or writable"
			}
			if err == nil || !strings.Contains(err.Error(), reason) {
				t.Fatal("changed native installation did not reject the intended boundary", err)
			}
		})
	}
}

func TestMountTargetCannotBeResolvedThroughAnotherVolume(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Main's mounted directory hides the image's aliases/seed symlink. Init
	// could resolve that same target into installed files in its original view.
	alias := filepath.Join(root, "aliases")
	if err := os.Mkdir(alias, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveMounts([]string{alias, filepath.Join(alias, "seed")}); err == nil {
		t.Fatal("a target hidden by another container's volume acquired image authority")
	}
	if _, err := resolveMounts([]string{alias, alias, filepath.Join(root, "unrelated")}); err != nil {
		t.Fatal("independent or repeated mount destinations were rejected", err)
	}
}

func TestImageMountCannotReplaceIntermediateSelector(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "provider")
	if err := os.WriteFile(file, []byte("provider bytes"), 0500); err != nil {
		t.Fatal(err)
	}
	middle := filepath.Join(root, "selector")
	entry := filepath.Join(root, "entrypoint")
	for path, target := range map[string]string{middle: "provider", entry: "selector"} {
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
	}
	paths, err := resolveImagePath(imagePath{entry, false})
	if err != nil {
		t.Fatal(err)
	}
	// The middle name is neither the descriptor's entrypoint nor its final
	// resolved file. Replacing it would make the worker choose different bytes.
	for _, mount := range []string{entry, middle, file} {
		rejected := false
		for _, path := range paths {
			if path.checkMounts([]string{mount}) != nil {
				rejected = true
			}
		}
		if !rejected {
			t.Fatalf("mounted selector %s acquired image authority", mount)
		}
	}
	for _, path := range paths {
		if err := path.checkMounts([]string{filepath.Join(root, "task-data")}); err != nil {
			t.Fatal("unrelated task data could not coexist with the image", err)
		}
	}
	if err := os.Remove(middle); err != nil {
		t.Fatal(err)
	}
	if _, err = resolveImagePath(imagePath{entry, false}); err == nil {
		t.Fatal("unresolved selector acquired image authority")
	}
}

func TestImageDirectoryProtectsDescendantsThroughLinks(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "seed")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "seed-link")
	if err := os.Symlink(directory, link); err != nil {
		t.Fatal(err)
	}
	paths, err := resolveImagePath(imagePath{link, true})
	if err != nil {
		t.Fatal(err)
	}
	for _, mount := range []string{filepath.Join(link, "config"), filepath.Join(directory, "config")} {
		rejected := false
		for _, path := range paths {
			if path.checkMounts([]string{mount}) != nil {
				rejected = true
			}
		}
		if !rejected {
			t.Fatal("mounted seed descendant acquired image authority")
		}
	}
}

func TestMountAliasCannotReplaceImageBytes(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	installed := filepath.Join(root, "installed")
	if err := os.Mkdir(installed, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(installed, alias); err != nil {
		t.Fatal(err)
	}
	// The future destination is absent in the controller but kubelet would
	// create it beneath the existing symlink target in the worker.
	mounts, err := resolveMounts([]string{filepath.Join(alias, "injected")})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(installed)
	if err != nil {
		t.Fatal(err)
	}
	if err := (imagePath{resolved, true}).checkMounts(mounts); err == nil {
		t.Fatal("aliased mount acquired image authority")
	}
}

func TestImageResolutionFollowsSymlinkBeforeParentTraversal(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{"location", "actual/child"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0700); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(root, "actual/provider")
	if err := os.WriteFile(file, []byte("provider"), 0500); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../actual/child", filepath.Join(root, "location/selector")); err != nil {
		t.Fatal(err)
	}
	entry := filepath.Join(root, "entry")
	if err := os.Symlink("location/selector/../provider", entry); err != nil {
		t.Fatal(err)
	}
	paths, err := resolveImagePath(imagePath{entry, false})
	if err != nil {
		t.Fatal("valid installed selection was rejected", err)
	}
	rejected := false
	for _, path := range paths {
		if path.checkMounts([]string{filepath.Join(root, "location/selector")}) != nil {
			rejected = true
		}
	}
	if !rejected {
		t.Fatal("mounted selector before parent traversal acquired image authority")
	}
}
