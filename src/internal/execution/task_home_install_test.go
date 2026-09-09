package execution

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

func TestTaskHomeRetryPreservesNativeCredentialsSettingsAndPackages(t *testing.T) {
	operator, seed := canonicalHomeDirectory(t), canonicalHomeDirectory(t)
	configFile(t, filepath.Join(operator, "auth.json"), "committed credential")
	configFile(t, filepath.Join(operator, "config.toml"), "operator setting")
	npmSeed := packageSeed(t, seed)
	configFile(t, filepath.Join(npmSeed, "node_modules/fixture/obsolete.txt"), "old package data")
	fixture := newTaskHomeFixture(t, taskHomeBundle(t, operator), seed)
	fixture.request.Provider = "pi"
	artifact := fixture.prepare(t)
	home := canonicalHomeDirectory(t)
	if err := InstallTaskHome(home, artifact, fixture.request); err != nil {
		t.Fatal(err)
	}
	npm := filepath.Join(home, runtimeimage.PiNPMDirectory)
	command := filepath.Join(npm, "node_modules/.bin/fixture")
	if output, err := exec.Command(command).Output(); err != nil || string(output) != "image package" {
		t.Fatalf("prepared task HOME could not execute its package: %q, %v", output, err)
	}
	configFile(t, filepath.Join(home, ".codex/auth.json"), "native refreshed credential")
	configFile(t, filepath.Join(npm, "node_modules/fixture/token/command"), "#!/bin/sh\nprintf 'native package update'\n")
	configFile(t, filepath.Join(npm, "node_modules/fixture/user-added.txt"), "new package state")
	for _, path := range []string{filepath.Join(home, ".codex/config.toml"), filepath.Join(npm, "node_modules/fixture/obsolete.txt")} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	if err := InstallTaskHome(home, artifact, fixture.request); err != nil {
		t.Fatal(err)
	}
	taskHomeContents(t, filepath.Join(home, ".codex/auth.json"), "native refreshed credential")
	taskHomeContents(t, filepath.Join(npm, "node_modules/fixture/user-added.txt"), "new package state")
	if output, err := exec.Command(command).Output(); err != nil || string(output) != "native package update" {
		t.Fatalf("same-attempt retry replaced its native package update: %q, %v", output, err)
	}
	for _, path := range []string{filepath.Join(home, ".codex/config.toml"), filepath.Join(npm, "node_modules/fixture/obsolete.txt")} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("same-attempt retry resurrected native state that the provider deleted", err)
		}
	}
	foreign := fixture.request
	foreign.AttemptID = uuid.NewString()
	if err := CheckTaskHome(home, foreign); err == nil {
		t.Fatal("a new attempt adopted another attempt's private HOME")
	}
}

func TestCorruptHomeArchiveCannotPublishPartialPrivateState(t *testing.T) {
	operator := canonicalHomeDirectory(t)
	configFile(t, filepath.Join(operator, "auth.json"), "authorized credential")
	fixture := newTaskHomeFixture(t, taskHomeBundle(t, operator), "")
	artifact := fixture.prepare(t)
	original, err := os.ReadFile(artifact)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(original)
	position := bytes.Index(corrupt, []byte("authorized credential"))
	if position < 0 {
		t.Fatal("archive fixture did not carry its operator credential")
	}
	copy(corrupt[position:], []byte("unapproved credential"))
	if err := os.WriteFile(artifact, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	home := canonicalHomeDirectory(t)
	if err := InstallTaskHome(home, artifact, fixture.request); err == nil {
		t.Fatal("modified archive acquired private HOME authority")
	}
	if err := CheckTaskHome(home, fixture.request); err == nil {
		t.Fatal("a rejected archive exposed an initialized HOME")
	}
	if _, err := os.ReadFile(filepath.Join(home, ".codex/auth.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("rejected archive exposed its unapproved credential", err)
	}
	if err := os.WriteFile(artifact, original, 0600); err != nil {
		t.Fatal(err)
	}
	if err := InstallTaskHome(home, artifact, fixture.request); err != nil {
		t.Fatal("failed initialization prevented a valid retry", err)
	}
	taskHomeContents(t, filepath.Join(home, ".codex/auth.json"), "authorized credential")
}

func TestHomeArchiveCannotGrantAnotherTaskIdentityOrEscapeItsDestination(t *testing.T) {
	operator, outside := canonicalHomeDirectory(t), canonicalHomeDirectory(t)
	configFile(t, filepath.Join(operator, "auth.json"), "authorized credential")
	fixture := newTaskHomeFixture(t, taskHomeBundle(t, operator), "")
	artifact := fixture.prepare(t)
	foreign := fixture.request
	foreign.TaskID = uuid.NewString()
	if err := InstallTaskHome(canonicalHomeDirectory(t), artifact, foreign); err == nil {
		t.Fatal("another task adopted the original task's authorized HOME")
	}
	protected := filepath.Join(outside, "private.txt")
	configFile(t, protected, "unrelated private content")
	identity, err := homeIdentity(fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	var malicious bytes.Buffer
	archive := tar.NewWriter(&malicious)
	for _, entry := range []struct {
		header tar.Header
		body   []byte
	}{
		{tar.Header{Name: taskHomeIdentityFile, Typeflag: tar.TypeReg, Mode: 0600, Size: int64(len(raw))}, raw},
		{tar.Header{Name: "home", Typeflag: tar.TypeDir, Mode: 0700}, nil},
		{tar.Header{Name: "home/" + protected, Typeflag: tar.TypeReg, Mode: 0600, Size: 9}, []byte("overwrite")},
	} {
		if err := archive.WriteHeader(&entry.header); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.Write(entry.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.request.HomeDigest = core.Digest(malicious.Bytes())
	if err := os.WriteFile(artifact, malicious.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := InstallTaskHome(canonicalHomeDirectory(t), artifact, fixture.request); err == nil {
		t.Fatal("archive path escaped its private HOME")
	}
	taskHomeContents(t, protected, "unrelated private content")
}

func TestHomePublicationRefusesUnmarkedNativeDirectoryLinks(t *testing.T) {
	operator, outside := canonicalHomeDirectory(t), canonicalHomeDirectory(t)
	configFile(t, filepath.Join(operator, "config.toml"), "operator configuration")
	protected := filepath.Join(outside, "config.toml")
	configFile(t, protected, "unrelated private configuration")
	fixture := newTaskHomeFixture(t, taskHomeBundle(t, operator), "")
	artifact := fixture.prepare(t)
	home := canonicalHomeDirectory(t)
	if err := os.Symlink(outside, filepath.Join(home, ".codex")); err != nil {
		t.Fatal(err)
	}
	if err := InstallTaskHome(home, artifact, fixture.request); err == nil {
		t.Fatal("initialization adopted unmarked native state through a HOME link")
	}
	taskHomeContents(t, protected, "unrelated private configuration")
}

func TestConcurrentTaskHomeInitializationPublishesOneCompletePrivateTree(t *testing.T) {
	operator, seed := canonicalHomeDirectory(t), canonicalHomeDirectory(t)
	configFile(t, filepath.Join(operator, "auth.json"), "authorized credential")
	want := bytes.Repeat([]byte("complete task HOME data\n"), 1<<16)
	configFile(t, filepath.Join(seed, ".fixture/payload"), string(want))
	fixture := newTaskHomeFixture(t, taskHomeBundle(t, operator), seed)
	artifact := fixture.prepare(t)
	home := canonicalHomeDirectory(t)
	results := make(chan error, 2)
	finished := make(chan struct{})
	var writers sync.WaitGroup
	for range 2 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			results <- InstallTaskHome(home, artifact, fixture.request)
		}()
	}
	go func() { writers.Wait(); close(finished) }()
	for {
		if CheckTaskHome(home, fixture.request) == nil {
			got, err := os.ReadFile(filepath.Join(home, ".fixture/payload"))
			if err != nil || !bytes.Equal(got, want) {
				<-finished
				t.Fatal("authorized HOME exposed an incomplete publication", err)
			}
		}
		select {
		case <-finished:
			for range 2 {
				if err := <-results; err != nil {
					t.Fatal(err)
				}
			}
			taskHomeContents(t, filepath.Join(home, ".codex/auth.json"), "authorized credential")
			return
		default:
			runtime.Gosched()
		}
	}
}
