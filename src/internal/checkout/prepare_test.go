package checkout

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

const fixtureRemote = "https://example.invalid/repo.git"

func TestCopiedCheckoutOwnsObjectsAndRetainsEdits(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			source := checkoutSource(t, format)
			gitRun(t, source, "switch", "-c", "feature/requested")
			writeFixture(t, filepath.Join(source, "work.txt"), "requested branch content")
			gitRun(t, source, "commit", "-am", "requested branch")
			gitRun(t, source, "tag", "-a", "release", "-m", "release")
			gitRun(t, source, "switch", "main")
			snapshot, commit := checkoutSnapshot(t, source, "feature/requested")
			base, workdir := checkoutTask(t)
			destination, err := Copy(context.Background(), base, workdir, fixtureRemote, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			otherTask, otherWorkdir := checkoutTask(t)
			other, err := Copy(context.Background(), otherTask, otherWorkdir, fixtureRemote, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			for _, ref := range []string{"HEAD", "origin/feature/requested", "release^{commit}"} {
				if gitRun(t, destination, "rev-parse", ref) != commit {
					t.Fatalf("selected commit unavailable through %s", ref)
				}
			}
			if gitRun(t, destination, "show", "origin/HEAD:work.txt") != "committed source" {
				t.Fatal("default branch content changed")
			}
			writeFixture(t, filepath.Join(destination, "work.txt"), "uncommitted user change")
			writeFixture(t, filepath.Join(destination, "notes.txt"), "untracked user note")
			gitRun(t, destination, "config", "--local", "user.name", "Custom task author")
			_, kept, err := Retain(context.Background(), base, workdir, fixtureRemote)
			if err != nil || !kept {
				t.Fatal("existing checkout was not retained", err)
			}
			for name, wanted := range map[string]string{"work.txt": "uncommitted user change", "notes.txt": "untracked user note"} {
				content, err := os.ReadFile(filepath.Join(destination, name))
				if err != nil || string(content) != wanted {
					t.Fatal("reuse changed task work", err)
				}
			}
			if gitRun(t, destination, "config", "--local", "user.name") != "Custom task author" {
				t.Fatal("reuse changed task Git identity")
			}
			snapshotPath := snapshot.Name()
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(snapshotPath); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(source); err != nil {
				t.Fatal(err)
			}
			gitRun(t, destination, "fsck", "--strict")
			if err := os.RemoveAll(filepath.Join(destination, ".git", "objects")); err != nil {
				t.Fatal(err)
			}
			gitRun(t, other, "fsck", "--strict")
			if gitRun(t, other, "show", "HEAD:work.txt") != "requested branch content" {
				t.Fatal("another task lost independent repository objects")
			}
		})
	}
}

func TestCheckoutPreservesContainedLinksAndRejectsExternalLinks(t *testing.T) {
	for _, external := range []bool{false, true} {
		label := "repository internal"
		if external {
			label = "outside repository"
		}
		t.Run(label, func(t *testing.T) {
			source := checkoutSource(t, "sha1")
			if err := os.MkdirAll(filepath.Join(source, ".agents", "skills"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(source, ".claude"), 0700); err != nil {
				t.Fatal(err)
			}
			protected := filepath.Join(source, ".agents", "skills", "guide.md")
			target := "../.agents/skills"
			if external {
				target = t.TempDir()
				protected = filepath.Join(target, "guide.md")
			}
			writeFixture(t, protected, "repository skill content")
			if err := os.Symlink(target, filepath.Join(source, ".claude", "skills")); err != nil {
				t.Fatal(err)
			}
			gitRun(t, source, "add", ".")
			gitRun(t, source, "commit", "-m", "skills")
			snapshot, _ := checkoutSnapshot(t, source, "main")
			base, workdir := checkoutTask(t)
			destination, err := Copy(context.Background(), base, workdir, fixtureRemote, snapshot)
			if external {
				if err == nil {
					t.Fatal("checkout accepted a link to protected external content")
				}
				after, readErr := os.ReadFile(protected)
				if readErr != nil || string(after) != "repository skill content" {
					t.Fatal("checkout changed external content", readErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			content, err := os.ReadFile(filepath.Join(destination, ".claude", "skills", "guide.md"))
			if err != nil || string(content) != "repository skill content" {
				t.Fatal("repository link cannot read copied content", err)
			}
		})
	}
}

func TestCheckoutRejectsWrongSourceAndCancelledCopy(t *testing.T) {
	source := checkoutSource(t, "sha1")
	snapshot, _ := checkoutSnapshot(t, source, "HEAD")
	for _, mismatch := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		remote := fixtureRemote
		if mismatch {
			remote = "https://example.invalid/another.git"
		} else {
			cancel()
		}
		base, workdir := checkoutTask(t)
		if _, err := Copy(ctx, base, workdir, remote, snapshot); err == nil {
			t.Fatal("unauthorized source or cancelled copy succeeded")
		}
		cancel()
		_, kept, err := Retain(context.Background(), base, workdir, remote)
		if err != nil || kept {
			t.Fatal("failed copy published a repository", err)
		}
	}
}

func TestCheckoutPublicationDoesNotReplaceConcurrentTaskWork(t *testing.T) {
	base, workdir := checkoutTask(t)
	root, err := openCheckoutRoot(base, workdir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := root.Mkdir("stage", 0700); err != nil {
		t.Fatal(err)
	}
	stage, err := root.OpenRoot("stage")
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	if err := root.Mkdir("destination", 0700); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(workdir, "destination", "notes.txt"), "concurrent task data")
	if err := publishCheckout(context.Background(), root, stage, "stage", "destination"); err == nil {
		t.Fatal("publication replaced concurrent task work")
	}
	content, err := root.ReadFile("destination/notes.txt")
	if err != nil || string(content) != "concurrent task data" {
		t.Fatal("publication damaged concurrent task work", err)
	}
}

func TestNFSFallbackPublishesIndependentRepository(t *testing.T) {
	source := checkoutSource(t, "sha1")
	snapshot, _ := checkoutSnapshot(t, source, "main")
	base, workdir := checkoutTask(t)
	path, err := Copy(context.Background(), base, workdir, fixtureRemote, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	root, err := openCheckoutRoot(base, workdir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	name := filepath.Base(path)
	if err := root.Rename(name, "stage"); err != nil {
		t.Fatal(err)
	}
	stage, err := root.OpenRoot("stage")
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	if err := copyCheckout(context.Background(), root, stage, "stage", name); err != nil {
		t.Fatal(err)
	}
	_, kept, err := Retain(context.Background(), base, workdir, fixtureRemote)
	if err != nil || !kept {
		t.Fatal("completed copied repository cannot be retained", err)
	}
	gitRun(t, path, "fsck", "--strict")
	if gitRun(t, path, "show", "HEAD:work.txt") != "committed source" {
		t.Fatal("copy lost independent repository content")
	}
}

func TestNFSFallbackFailureCannotAuthorizePartialRepository(t *testing.T) {
	source := checkoutSource(t, "sha1")
	snapshot, _ := checkoutSnapshot(t, source, "main")
	base, workdir := checkoutTask(t)
	path, err := Copy(context.Background(), base, workdir, fixtureRemote, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	root, err := openCheckoutRoot(base, workdir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	name := filepath.Base(path)
	if err := root.Rename(name, "stage"); err != nil {
		t.Fatal(err)
	}
	stage, err := root.OpenRoot("stage")
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	if err := unix.Mkfifo(filepath.Join(workdir, "stage", "incomplete"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := copyCheckout(context.Background(), root, stage, "stage", name); err == nil {
		t.Fatal("copy accepted incomplete data")
	}
	if _, kept, err := Retain(context.Background(), base, workdir, fixtureRemote); err == nil || kept {
		t.Fatal("partial copy received reuse authority")
	}
	protected := filepath.Join(path, "notes.txt")
	writeFixture(t, protected, "task edits after failed checkout")
	if err := copyCheckout(context.Background(), root, stage, "stage", name); err == nil {
		t.Fatal("retry adopted occupied task destination")
	}
	content, err := os.ReadFile(protected)
	if err != nil || string(content) != "task edits after failed checkout" {
		t.Fatal("retry damaged retained task work", err)
	}
}

func TestCheckoutCopyCannotOverwriteProviderFiles(t *testing.T) {
	source, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	destination, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	writeFixture(t, filepath.Join(source.Name(), "work.txt"), "repository content")
	writeFixture(t, filepath.Join(destination.Name(), "work.txt"), "concurrent user edit")
	if err := copyCheckoutTree(context.Background(), source, destination, nil); err == nil {
		t.Fatal("copy overwrote provider-owned file")
	}
	content, err := destination.ReadFile("work.txt")
	if err != nil || string(content) != "concurrent user edit" {
		t.Fatal("copy damaged provider edit", err)
	}
}

func TestRetainedOriginDoesNotReadIncludedProviderConfig(t *testing.T) {
	source := checkoutSource(t, "sha1")
	snapshot, _ := checkoutSnapshot(t, source, "main")
	base, workdir := checkoutTask(t)
	path, err := Copy(context.Background(), base, workdir, fixtureRemote, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	untrusted := filepath.Join(t.TempDir(), "included-config")
	writeFixture(t, untrusted, "[remote \"origin\"]\nurl=https://example.invalid/not-authorized.git\n")
	gitRun(t, path, "config", "include.path", untrusted)
	_, kept, err := Retain(context.Background(), base, workdir, fixtureRemote)
	if err != nil || !kept {
		t.Fatal("provider includes overrode bound origin", err)
	}
}

func checkoutSource(t *testing.T, format string) string {
	t.Helper()
	source := t.TempDir()
	gitRun(t, source, "init", "--object-format="+format, "-b", "main")
	writeFixture(t, filepath.Join(source, "work.txt"), "committed source")
	gitRun(t, source, "add", ".")
	gitRun(t, source, "commit", "-m", "fixture")
	return source
}

func checkoutTask(t *testing.T) (string, string) {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	workdir := filepath.Join(base, "workdir")
	if err := os.Mkdir(workdir, 0700); err != nil {
		t.Fatal(err)
	}
	return base, workdir
}

func checkoutSnapshot(t *testing.T, source, ref string) (*os.Root, string) {
	t.Helper()
	commit := gitRun(t, source, "rev-parse", ref+"^{commit}")
	path := t.TempDir()
	gitRun(t, source, "clone", "--no-hardlinks", "--no-checkout", "--", source, path)
	gitRun(t, path, "remote", "set-url", "origin", fixtureRemote)
	gitRun(t, path, "checkout", "--detach", commit, "--")
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root, commit
}

func writeFixture(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func gitRun(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-c", "user.name=Fixture", "-c", "user.email=fixture@invalid", "-c", "core.hooksPath=/dev/null", "-c", "maintenance.auto=false", "-C", directory}, args...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("native Git command failed: %v: %s", err, output)
	}
	return strings.TrimSpace(string(output))
}
