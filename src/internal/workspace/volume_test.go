package workspace

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestRepositoryCacheSurvivesJournalReopen(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := Options{OwnerID: uuid.NewString()}
	store, err := OpenVolume(root, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.BindWorkspace("workspace", uuid.NewString(), "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	grant, err := store.Create(testGrant())
	if err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(root, "repositories")
	if err := os.Mkdir(cache, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(cache, "retained-object")
	if err := os.WriteFile(sentinel, []byte("cached object"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenVolume(root, options)
	if err != nil {
		t.Fatal("cache prevented the existing journal from reopening", err)
	}
	defer store.Close()
	retained, err := store.Get(grant.AttemptID)
	if err != nil || retained.TaskID != grant.TaskID {
		t.Fatal("journal ownership was lost while reopening the cache", err)
	}
	contents, err := os.ReadFile(sentinel)
	if err != nil || string(contents) != "cached object" {
		t.Fatal("journal startup changed cache data", err)
	}
}

func TestRepositoryCacheCannotCreateReplacementAuthority(t *testing.T) {
	for _, emptyJournal := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing journal", true: "empty journal"}[emptyJournal], func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(root, "repositories"), 0700); err != nil {
				t.Fatal(err)
			}
			if emptyJournal {
				if err := os.Mkdir(filepath.Join(root, "journal"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			store, err := OpenVolume(root, Options{OwnerID: uuid.NewString()})
			if err == nil {
				store.Close()
				t.Fatal("orphaned cache allowed a replacement controller journal")
			}
		})
	}
}
