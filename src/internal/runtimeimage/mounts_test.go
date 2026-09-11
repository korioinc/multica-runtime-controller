package runtimeimage

import (
	"os"
	"path/filepath"
	"testing"
)

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
