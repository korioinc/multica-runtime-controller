package checkout

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestContextRejectsUnrelatedSkillLinks(t *testing.T) {
	for _, mismatch := range []string{"foreign-target", "different-name", "nested-link"} {
		t.Run(mismatch, func(t *testing.T) {
			prepared, outside := t.TempDir(), t.TempDir()
			worker, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			globalSkills, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			write := func(path, contents string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			write(filepath.Join(prepared, "workdir/AGENTS.md"), "task brief")
			write(filepath.Join(prepared, ".multica_sidecar_manifest.json"), `{"files":[]}`)
			write(filepath.Join(globalSkills, "global/SKILL.md"), "global instructions")
			protected := filepath.Join(outside, "SKILL.md")
			write(protected, "ungranted private instructions")
			name, target := "global", outside
			if mismatch == "different-name" {
				name, target = "alias", filepath.Join(globalSkills, "global")
			}
			if mismatch == "nested-link" {
				name, target = "task/reference", filepath.Join(globalSkills, "global")
			}
			link := filepath.Join(prepared, "codex-home/skills", name)
			if err := os.MkdirAll(filepath.Dir(link), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, link); err != nil {
				t.Fatal(err)
			}
			if err := SeedContext(prepared, worker, filepath.Join(t.TempDir(), "context.json"), "codex", globalSkills); err == nil {
				t.Fatal("context accepted a skill link outside the inherited global-skill contract")
			}
			if _, err := os.ReadFile(filepath.Join(worker, "codex-skills", name, "SKILL.md")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("context disclosed unassigned linked instructions to the worker", err)
			}
			got, err := os.ReadFile(protected)
			if err != nil || string(got) != "ungranted private instructions" {
				t.Fatal("context export changed unrelated private instructions", err)
			}
		})
	}
}
