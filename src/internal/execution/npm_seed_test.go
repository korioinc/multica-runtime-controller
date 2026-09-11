package execution

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

func packageSeed(t *testing.T, seed string) string {
	t.Helper()
	root := filepath.Join(seed, runtimeimage.PiNPMDirectory)
	configFile(t, filepath.Join(root, "package.json"), `{"private":true,"dependencies":{"fixture":"1.0.0"}}`)
	command := filepath.Join(root, "node_modules/fixture/token/command")
	configFile(t, command, "#!/bin/sh\nprintf 'image package'\n")
	if err := os.Chmod(command, 0755); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "node_modules/.bin")
	if err := os.MkdirAll(bin, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../fixture/token/command", filepath.Join(bin, "fixture")); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestHomeSeedPreparesExecutablePackagesAndPreservesProviderUpdates(t *testing.T) {
	seed, home := t.TempDir(), t.TempDir()
	source := packageSeed(t, seed)
	configFile(t, filepath.Join(source, "node_modules/fixture/obsolete.txt"), "old package data")
	configFile(t, filepath.Join(seed, ".pi/agent/settings.json"), "image settings")
	settings := filepath.Join(home, ".pi/agent/settings.json")
	configFile(t, settings, "operator settings")
	if err := copyHomeSeed(home, seed); err != nil {
		t.Fatal(err)
	}
	npm := filepath.Join(home, runtimeimage.PiNPMDirectory)
	command := filepath.Join(npm, "node_modules/.bin/fixture")
	if output, err := exec.Command(command).Output(); err != nil || string(output) != "image package" {
		t.Fatalf("provider could not execute the installed package: %q, %v", output, err)
	}
	configFile(t, filepath.Join(npm, "node_modules/fixture/token/command"), "#!/bin/sh\nprintf 'user update'\n")
	if err := os.Remove(filepath.Join(npm, "node_modules/fixture/obsolete.txt")); err != nil {
		t.Fatal(err)
	}
	if err := copyHomeSeed(home, seed); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command(command).Output(); err != nil || string(output) != "user update" {
		t.Fatalf("reinitialization replaced the user's package: %q, %v", output, err)
	}
	if _, err := os.Stat(filepath.Join(npm, "node_modules/fixture/obsolete.txt")); !os.IsNotExist(err) {
		t.Fatal("reinitialization resurrected data removed by the user's update", err)
	}
	if got, err := os.ReadFile(settings); err != nil || string(got) != "operator settings" {
		t.Fatal("image defaults replaced operator settings", err)
	}
	if got, err := os.ReadFile(filepath.Join(source, "node_modules/fixture/token/command")); err != nil || string(got) != "#!/bin/sh\nprintf 'image package'\n" {
		t.Fatal("private provider changes escaped into the image seed", err)
	}
}

func TestHomeSeedAdmissionRejectsCredentialStateWithPackages(t *testing.T) {
	seed := t.TempDir()
	packageSeed(t, seed)
	configFile(t, filepath.Join(seed, ".pi/agent/auth.json"), "image credential")
	if err := runtimeimage.ValidateSeedContents(seed); err == nil {
		t.Fatal("image credential state was admitted as a HOME default")
	}
}

func TestHomeSeedRejectsPackageLinksToUnrelatedPrivateFiles(t *testing.T) {
	seed, home, outside := t.TempDir(), t.TempDir(), t.TempDir()
	root := packageSeed(t, seed)
	private := filepath.Join(outside, "private-command")
	configFile(t, private, "#!/bin/sh\nprintf 'ungranted data'\n")
	if err := os.Chmod(private, 0755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "node_modules/.bin/fixture")
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	target, err := filepath.Rel(filepath.Dir(link), private)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := copyHomeSeed(home, seed); err == nil {
		t.Fatal("package link granted access outside its installed tree")
	}
	if _, err := os.Stat(filepath.Join(home, runtimeimage.PiNPMDirectory)); !os.IsNotExist(err) {
		t.Fatal("rejected package link was published into HOME", err)
	}
}

func TestConcurrentHomeSeedPublicationExposesOnlyCompletePackageData(t *testing.T) {
	seed, home := t.TempDir(), t.TempDir()
	root := packageSeed(t, seed)
	want := bytes.Repeat([]byte("package payload\n"), 1<<16)
	relative := "node_modules/fixture/z-payload"
	if err := os.WriteFile(filepath.Join(root, relative), want, 0644); err != nil {
		t.Fatal(err)
	}
	npm := filepath.Join(home, runtimeimage.PiNPMDirectory)
	finished := make(chan struct{})
	errors := make(chan error, 2)
	var writers sync.WaitGroup
	for range 2 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			errors <- copyHomeSeed(home, seed)
		}()
	}
	go func() { writers.Wait(); close(finished) }()
	for {
		if _, err := os.Stat(npm); err == nil {
			got, err := os.ReadFile(filepath.Join(npm, relative))
			if err != nil || !bytes.Equal(got, want) {
				<-finished
				t.Fatal("provider observed an incomplete package publication", err)
			}
		}
		select {
		case <-finished:
			for range 2 {
				if err := <-errors; err != nil {
					t.Fatal(err)
				}
			}
			if got, err := os.ReadFile(filepath.Join(npm, relative)); err != nil || !bytes.Equal(got, want) {
				t.Fatal("concurrent initialization did not publish the package payload", err)
			}
			return
		default:
			runtime.Gosched()
		}
	}
}
