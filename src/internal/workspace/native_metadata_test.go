package workspace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
)

func TestNativeArtifactCopyRejectsChangedOrIndirectBytes(t *testing.T) {
	for _, attack := range []string{"unchanged", "same-size-mutation", "file-link", "directory-link", "hardlink"} {
		t.Run(attack, func(t *testing.T) {
			base := t.TempDir()
			path := filepath.Join(base, "skills/current/SKILL.md")
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			raw := []byte("bound native skill")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(t.TempDir(), "sentinel")
			if err := os.WriteFile(outside, raw, 0600); err != nil {
				t.Fatal(err)
			}
			artifact := NativeArtifact{Path: "skills/current/SKILL.md", Size: int64(len(raw)), SHA256: core.Digest(raw)}
			switch attack {
			case "same-size-mutation":
				if err := os.WriteFile(path, []byte("other native skill"), 0600); err != nil {
					t.Fatal(err)
				}
			case "file-link":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, path); err != nil {
					t.Fatal(err)
				}
			case "directory-link":
				if err := os.Rename(filepath.Dir(path), filepath.Dir(path)+"-retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Dir(outside), filepath.Dir(path)); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(path, filepath.Join(filepath.Dir(outside), "alias")); err != nil {
					t.Fatal(err)
				}
			}
			root, err := os.OpenRoot(base)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			copied, err := ReadNativeArtifact(root, artifact)
			if attack == "unchanged" {
				if err != nil || string(copied) != string(raw) {
					t.Fatal("valid manifest bytes were refused", err)
				}
			} else if err == nil {
				t.Fatal("unbound native bytes were admitted")
			}
			if current, err := os.ReadFile(outside); err != nil || string(current) != string(raw) {
				t.Fatal("artifact copy changed outside bytes", err)
			}
		})
	}
}

func TestNativeMetadataRequiresPrivateMandatoryConfigurationWithinBudget(t *testing.T) {
	root := filepath.Join(t.TempDir(), "task")
	metadata := NativeMetadata{WorkerSessionID: uuid.NewString(), TurnSequence: 1, Artifacts: []NativeArtifact{}, CodexConfig: []byte("model='fixture'\n")}
	metadata.ArtifactRoot = NativeArtifactRoot(root, metadata.WorkerSessionID, metadata.TurnSequence)
	metadata.TaskMarker, _ = json.Marshal(map[string]string{"managed_by": "multica-daemon-task", "agent_id": uuid.NewString()})
	if err := metadata.Validate("codex", root); err != nil {
		t.Fatal(err)
	}
	metadata.CodexConfig = make([]byte, MaxAssignmentBytes)
	if err := metadata.Validate("codex", root); err == nil {
		t.Fatal("oversized mandatory native configuration was admitted")
	}
}

func TestNativePathAdapterPreservesOpaqueUserValues(t *testing.T) {
	const scratch = "/private/scratch/task/codex-home"
	raw := []byte("model = '/private/scratch/task/opaque-user-value'\n[[skills.config]]\npath = '/private/scratch/task/codex-home/skills/operator/SKILL.md'\nenabled = false\n")
	rebased, err := rebaseCodexSkillPaths(raw, scratch, "/workspace/task/codex-home")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rebased), "/workspace/task/codex-home/skills/operator/SKILL.md") || !strings.Contains(string(rebased), "/private/scratch/task/opaque-user-value") {
		t.Fatal("typed path adaptation rewrote opaque user values or retained a scratch skill path")
	}
}
