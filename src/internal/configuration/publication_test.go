package configuration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
)

func TestUnpublishableGroupCannotPublishPartialBundle(t *testing.T) {
	input, run := t.TempDir(), t.TempDir()
	content := bytes.Repeat([]byte("x"), configuration.MaxGroupBytes*3/4)
	// Each file fits the source limit; the combined group exceeds it.
	if err := os.Mkdir(filepath.Join(input, "combined"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "second"} {
		if err := os.WriteFile(filepath.Join(input, "combined", name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	copies := []configuration.Copy{{SourceGroup: "combined", Source: filepath.Join(input, "combined"), Target: configuration.Home + "/.provider"}}
	_, err := configuration.CaptureOrRead(run, copies, input)
	if err == nil {
		t.Fatal("an unpublishable source group acquired committed configuration authority")
	}
	if _, err = os.Stat(filepath.Join(run, configuration.BundleName)); !os.IsNotExist(err) {
		t.Fatal("rejected capture published a partial committed bundle", err)
	}

	// Separate source objects retain separate capacity. Recovery may commit both
	// complete inputs without dropping files merely to fit one merged object.
	for _, name := range []string{"first", "second"} {
		if err = os.Mkdir(filepath.Join(input, name), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.Rename(filepath.Join(input, "combined", name), filepath.Join(input, name, "settings")); err != nil {
			t.Fatal(err)
		}
	}
	copies = []configuration.Copy{{SourceGroup: "first", Source: filepath.Join(input, "first"), Target: configuration.Home + "/.first"}, {SourceGroup: "second", Source: filepath.Join(input, "second"), Target: configuration.Home + "/.second"}}
	_, err = configuration.CaptureOrRead(run, copies, input)
	if err != nil {
		t.Fatal("independent complete source groups could not be published", err)
	}
	recovered, err := configuration.Read(run)
	if err != nil {
		t.Fatal("published bundle cannot be recovered", err)
	}
	var actual []byte
	for _, group := range recovered.Groups {
		for _, file := range group.Files {
			actual = append(actual, file.Content...)
		}
	}
	if !bytes.Equal(actual, append(bytes.Clone(content), content...)) {
		t.Fatal("committed source content was lost on recovery")
	}
}
