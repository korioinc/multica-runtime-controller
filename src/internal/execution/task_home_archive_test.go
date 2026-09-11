package execution

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestTaskHomeArchiveRejectsInvalidStagedStateBeforePublication(t *testing.T) {
	for _, failure := range []string{"absolute command", "escaping command", "broken command", "non-executable command", "redirected command", "reserved authority", "special file"} {
		t.Run(failure, func(t *testing.T) {
			fixture := newTaskHomeFixture(t, taskHomeBundle(t, canonicalHomeDirectory(t)), "")
			identity, err := homeIdentity(fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			artifacts, err := resetTaskHomeArtifacts(fixture.worker)
			if err != nil {
				t.Fatal(err)
			}
			defer artifacts.Close()
			if err := artifacts.Mkdir("stage", 0700); err != nil {
				t.Fatal(err)
			}
			home := filepath.Join(artifacts.Name(), "stage")
			npm := packageSeed(t, home)
			command := filepath.Join(npm, "node_modules/fixture/token/command")
			link := filepath.Join(npm, "node_modules/.bin/fixture")
			target := ""
			switch failure {
			case "absolute command":
				target = command
			case "escaping command":
				outside := filepath.Join(canonicalHomeDirectory(t), "command")
				configFile(t, outside, "#!/bin/sh\nexit 0\n")
				if err := os.Chmod(outside, 0700); err != nil {
					t.Fatal(err)
				}
				target, err = filepath.Rel(filepath.Dir(link), outside)
			case "broken command":
				err = os.Remove(command)
			case "non-executable command":
				err = os.Chmod(command, 0600)
			case "redirected command":
				outside := canonicalHomeDirectory(t)
				configFile(t, filepath.Join(outside, "command"), "#!/bin/sh\nexit 0\n")
				if err := os.Chmod(filepath.Join(outside, "command"), 0700); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink(outside, filepath.Join(npm, "node_modules/fixture/redirect"))
				target = "../fixture/redirect/command"
			case "reserved authority":
				configFile(t, filepath.Join(home, taskHomeMarker), "unapproved identity")
			case "special file":
				err = syscall.Mkfifo(filepath.Join(npm, "node_modules/fixture/pipe"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if target != "" {
				if err := os.Remove(link); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, link); err != nil {
					t.Fatal(err)
				}
			}
			digest, err := publishTaskHomeArchive(artifacts, "stage", identity)
			if err == nil || digest != "" {
				t.Fatal("invalid staged HOME acquired a published artifact digest", err)
			}
			if _, err := os.Stat(filepath.Join(artifacts.Name(), identity.AttemptID+".tar")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid staged HOME became available to a worker", err)
			}
		})
	}
}
