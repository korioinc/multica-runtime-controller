package configuration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
)

func TestUnpublishableGroupCannotPublishPartialBundleOrHome(t *testing.T) {
	input, run, home := t.TempDir(), t.TempDir(), t.TempDir()
	content := bytes.Repeat([]byte("x"), configuration.MaxGroupBytes*3/4)
	// Each file fits one source object; together they cannot form one snapshot.
	if err := os.Mkdir(filepath.Join(input, "combined"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "second"} {
		if err := os.WriteFile(filepath.Join(input, "combined", name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	copies := []configuration.Copy{{SourceGroup: "combined", Source: filepath.Join(input, "combined"), Target: configuration.Home + "/.provider"}}
	rejected, err := configuration.CaptureOrRead(run, copies, input)
	if err == nil {
		t.Fatal("an unpublishable source group acquired committed configuration authority")
	}
	if _, err = os.Stat(filepath.Join(run, configuration.BundleName)); !os.IsNotExist(err) {
		t.Fatal("rejected capture published a partial committed bundle", err)
	}
	if err = execution.CopyBundle(home, rejected); err == nil {
		t.Fatal("rejected capture was accepted for HOME publication")
	}
	entries, err := os.ReadDir(home)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("rejected bundle published operator files into HOME")
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
	accepted, err := configuration.CaptureOrRead(run, copies, input)
	if err != nil {
		t.Fatal("independent complete source groups could not be published", err)
	}
	if err = execution.CopyBundle(home, accepted); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "second"} {
		actual, err := os.ReadFile(filepath.Join(home, "."+name, "settings"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual, content) {
			t.Fatal("source group content was lost during complete publication")
		}
	}
	if _, err = configuration.Read(run); err != nil {
		t.Fatal("published bundle cannot be recovered", err)
	}
}
