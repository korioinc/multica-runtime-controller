package official

import (
	"os"
	"path/filepath"
	"testing"
)

func skillInputDirectory(t *testing.T) string {
	t.Helper()
	path, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func skillInputFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func baseHomeInput(t *testing.T) *os.Root {
	t.Helper()
	root, err := os.OpenRoot(skillInputDirectory(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root
}

func TestDaemonGlobalSkillLinksDoNotGrantMutableControllerContent(t *testing.T) {
	task, global := skillInputDirectory(t), skillInputDirectory(t)
	skillInputFile(t, filepath.Join(task, "codex-home/skills/assigned/SKILL.md"), "authorized assignment")
	skillInputFile(t, filepath.Join(global, ".codex/skills/global/SKILL.md"), "unselected controller instructions")
	if err := os.Symlink(filepath.Join(global, ".codex/skills/global"), filepath.Join(task, "codex-home/skills/global")); err != nil {
		t.Fatal(err)
	}
	overrides, err := ReadTaskHomeOverrides("codex", task, global, baseHomeInput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer overrides.Close()
	foundAssignment := false
	for _, directory := range overrides.Directories {
		raw, readErr := directory.Root.ReadFile("SKILL.md")
		if readErr != nil {
			t.Fatal(readErr)
		}
		if string(raw) == "unselected controller instructions" {
			t.Fatal("daemon's inherited link promoted mutable controller data to an assignment")
		}
		foundAssignment = foundAssignment || string(raw) == "authorized assignment"
	}
	if !foundAssignment {
		t.Fatal("daemon's actual assignment was lost with its inherited defaults")
	}
}

func TestDaemonTaskSkillsRejectUnapprovedLinkedData(t *testing.T) {
	for _, scenario := range []string{"foreign target", "different global name", "nested link", "relative global link", "global link chain", "provider HOME link"} {
		t.Run(scenario, func(t *testing.T) {
			task, global, outside := skillInputDirectory(t), skillInputDirectory(t), skillInputDirectory(t)
			skillInputFile(t, filepath.Join(task, "codex-home/skills/assigned/SKILL.md"), "authorized assignment")
			skillInputFile(t, filepath.Join(global, ".codex/skills/global/SKILL.md"), "inherited global instructions")
			skillInputFile(t, filepath.Join(outside, "SKILL.md"), "ungranted private instructions")
			path, target := filepath.Join(task, "codex-home/skills/unapproved"), outside
			switch scenario {
			case "different global name":
				target = filepath.Join(global, ".codex/skills/global")
			case "nested link":
				path = filepath.Join(task, "codex-home/skills/assigned/reference.md")
				target = filepath.Join(outside, "SKILL.md")
			case "relative global link":
				path = filepath.Join(task, "codex-home/skills/global")
				var err error
				target, err = filepath.Rel(filepath.Dir(path), filepath.Join(global, ".codex/skills/global"))
				if err != nil {
					t.Fatal(err)
				}
			case "global link chain":
				path = filepath.Join(task, "codex-home/skills/unapproved")
				target = filepath.Join(global, ".codex/skills/unapproved")
				if err := os.Symlink(outside, target); err != nil {
					t.Fatal(err)
				}
			case "provider HOME link":
				path = filepath.Join(task, "codex-home")
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
			overrides, err := ReadTaskHomeOverrides("codex", task, global, baseHomeInput(t))
			defer overrides.Close()
			if err == nil {
				t.Fatal("unapproved linked data acquired task assignment authority")
			}
		})
	}
}
