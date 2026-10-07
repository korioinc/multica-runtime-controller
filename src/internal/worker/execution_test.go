package worker

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func TestClaudeWorkerReplacementRetainsConversationWithoutCredentials(t *testing.T) {
	taskRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(taskRoot, workspace.ClaudeSessionsDir), 0700); err != nil {
		t.Fatal(err)
	}
	credential := []byte("private-credential-" + uuid.NewString())
	settings := append(append([]byte(`{"env":{"ANTHROPIC_AUTH_TOKEN":"`), credential...), []byte(`"}}`)...)
	conversation := []byte("user's retained task conversation\n")
	sessionName := uuid.NewString() + ".jsonl"
	for attempt := range 2 {
		homePath := t.TempDir()
		configPath := filepath.Join(homePath, workspace.ClaudeConfigDir)
		if err := os.Mkdir(configPath, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(configPath, "settings.json"), settings, 0600); err != nil {
			t.Fatal(err)
		}
		home, err := os.OpenRoot(homePath)
		if err != nil {
			t.Fatal(err)
		}
		err = prepareClaudeSessions(home, taskRoot)
		home.Close()
		if err != nil {
			t.Fatal(err)
		}
		session := filepath.Join(configPath, "projects", workspace.ClaudeProjectDir, sessionName)
		if attempt == 0 {
			if err := os.MkdirAll(filepath.Dir(session), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(session, conversation, 0600); err != nil {
				t.Fatal(err)
			}
		} else if retained, err := os.ReadFile(session); err != nil || !bytes.Equal(retained, conversation) {
			t.Fatal("replacement worker lost the previous conversation", err)
		}
		if retained, err := os.ReadFile(filepath.Join(configPath, "settings.json")); err != nil || !bytes.Equal(retained, settings) {
			t.Fatal("session preparation changed operator authentication", err)
		}
	}
	if err := filepath.WalkDir(taskRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if bytes.Contains(raw, credential) {
			t.Fatal("private operator credential reached retained task files")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestClaudeWorkerCannotReplaceExistingSessionOwnership(t *testing.T) {
	for _, indirect := range []bool{false, true} {
		t.Run(map[bool]string{false: "directory", true: "other-task-link"}[indirect], func(t *testing.T) {
			taskRoot, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(taskRoot, workspace.ClaudeSessionsDir), 0700); err != nil {
				t.Fatal(err)
			}
			homePath := t.TempDir()
			projects := filepath.Join(homePath, workspace.ClaudeConfigDir, "projects")
			if err := os.MkdirAll(filepath.Dir(projects), 0700); err != nil {
				t.Fatal(err)
			}
			if indirect {
				err = os.Symlink(t.TempDir(), projects)
			} else {
				err = os.Mkdir(projects, 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			victim := filepath.Join(projects, "conversation.jsonl")
			original := []byte("another conversation owner's data")
			if err := os.WriteFile(victim, original, 0600); err != nil {
				t.Fatal(err)
			}
			home, err := os.OpenRoot(homePath)
			if err != nil {
				t.Fatal(err)
			}
			defer home.Close()
			if err := prepareClaudeSessions(home, taskRoot); err == nil {
				t.Fatal("worker adopted unrelated session storage")
			}
			if retained, err := os.ReadFile(victim); err != nil || !bytes.Equal(retained, original) {
				t.Fatal("rejected worker changed another conversation", err)
			}
		})
	}
}
