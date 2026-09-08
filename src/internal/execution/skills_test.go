package execution

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func skillBundle(t *testing.T, operator string) configuration.Bundle {
	t.Helper()
	operator, err := filepath.EvalSymlinks(operator)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := configuration.CaptureOrRead(t.TempDir(), []configuration.Copy{{SourceGroup: filepath.Base(operator), Source: operator, Target: wire.Home + "/.codex"}}, filepath.Dir(operator))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func skillContents(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != want {
		t.Fatalf("skill content at %s: got %q, want %q, error %v", path, got, want, err)
	}
}

func TestAssignedSkillsPreserveAndRestoreOperatorDefaults(t *testing.T) {
	home, seed, assigned := t.TempDir(), t.TempDir(), t.TempDir()
	operator := filepath.Join(t.TempDir(), "operator")
	configFile(t, filepath.Join(operator, "skills/shared/SKILL.md"), "operator instructions")
	configFile(t, filepath.Join(operator, "skills/shared/references/operator.md"), "operator reference")
	configFile(t, filepath.Join(operator, "skills/global/SKILL.md"), "global instructions")
	configFile(t, filepath.Join(operator, "auth.json"), "operator credential fixture")
	configFile(t, filepath.Join(seed, skillHome, "shared/SKILL.md"), "image instructions")
	configFile(t, filepath.Join(seed, skillHome, "shared/references/image.md"), "image reference")
	configFile(t, filepath.Join(seed, skillHome, "builtin/SKILL.md"), "image default")
	configFile(t, filepath.Join(assigned, "shared/SKILL.md"), "assigned instructions")
	configFile(t, filepath.Join(assigned, "temporary/SKILL.md"), "temporary assigned instructions")
	configFile(t, filepath.Join(home, ".codex/auth.json"), "private refreshed credential")
	bundle := skillBundle(t, operator)
	if err := hydrateSkills(assigned, home, seed, bundle); err != nil {
		t.Fatal(err)
	}
	skillContents(t, filepath.Join(home, skillHome, "shared/SKILL.md"), "assigned instructions")
	skillContents(t, filepath.Join(home, skillHome, "temporary/SKILL.md"), "temporary assigned instructions")
	skillContents(t, filepath.Join(home, skillHome, "global/SKILL.md"), "global instructions")
	skillContents(t, filepath.Join(home, skillHome, "builtin/SKILL.md"), "image default")
	for _, reference := range []string{"operator.md", "image.md"} {
		if _, err := os.ReadFile(filepath.Join(home, skillHome, "shared/references", reference)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("assigned skill retained a reference from an overridden base: %v", err)
		}
	}
	// HOME edits and a changed live projection cannot become the next base.
	configFile(t, filepath.Join(home, skillHome, "shared/SKILL.md"), "previous task customization")
	configFile(t, filepath.Join(operator, "skills/shared/SKILL.md"), "unselected operator revision")
	if err := os.RemoveAll(assigned); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(assigned, 0700); err != nil {
		t.Fatal(err)
	}
	if err := hydrateSkills(assigned, home, seed, bundle); err != nil {
		t.Fatal(err)
	}
	skillContents(t, filepath.Join(home, skillHome, "shared/SKILL.md"), "operator instructions")
	skillContents(t, filepath.Join(home, skillHome, "shared/references/operator.md"), "operator reference")
	skillContents(t, filepath.Join(home, skillHome, "shared/references/image.md"), "image reference")
	skillContents(t, filepath.Join(home, skillHome, "global/SKILL.md"), "global instructions")
	if _, err := os.ReadFile(filepath.Join(home, skillHome, "temporary/SKILL.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("withdrawn assignment remained available to the next task: %v", err)
	}
	skillContents(t, filepath.Join(home, ".codex/auth.json"), "private refreshed credential")
	skillContents(t, filepath.Join(seed, skillHome, "shared/SKILL.md"), "image instructions")
	skillContents(t, filepath.Join(operator, "skills/shared/SKILL.md"), "unselected operator revision")
}

func TestAssignedSkillNameNormalizationRestoresConfiguredNames(t *testing.T) {
	home, seed, assigned := t.TempDir(), t.TempDir(), t.TempDir()
	operator := filepath.Join(t.TempDir(), "operator")
	configFile(t, filepath.Join(operator, "skills/Code Review/SKILL.md"), "operator review instructions")
	configFile(t, filepath.Join(operator, "skills/code_review/SKILL.md"), "additional operator review instructions")
	configFile(t, filepath.Join(operator, "skills/deployment/SKILL.md"), "unrelated deployment instructions")
	configFile(t, filepath.Join(seed, skillHome, "CODE.review/SKILL.md"), "image review instructions")
	configFile(t, filepath.Join(assigned, "code-review/SKILL.md"), "assigned review instructions")
	bundle := skillBundle(t, operator)
	if err := hydrateSkills(assigned, home, seed, bundle); err != nil {
		t.Fatal(err)
	}
	skillContents(t, filepath.Join(home, skillHome, "code-review/SKILL.md"), "assigned review instructions")
	skillContents(t, filepath.Join(home, skillHome, "deployment/SKILL.md"), "unrelated deployment instructions")
	for _, previous := range []string{"Code Review", "code_review", "CODE.review"} {
		if _, err := os.ReadFile(filepath.Join(home, skillHome, previous, "SKILL.md")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("configured instructions remained active alongside their normalized assignment: %v", err)
		}
	}
	if err := os.RemoveAll(filepath.Join(assigned, "code-review")); err != nil {
		t.Fatal(err)
	}
	if err := hydrateSkills(assigned, home, seed, bundle); err != nil {
		t.Fatal(err)
	}
	skillContents(t, filepath.Join(home, skillHome, "Code Review/SKILL.md"), "operator review instructions")
	skillContents(t, filepath.Join(home, skillHome, "code_review/SKILL.md"), "additional operator review instructions")
	skillContents(t, filepath.Join(home, skillHome, "CODE.review/SKILL.md"), "image review instructions")
	skillContents(t, filepath.Join(home, skillHome, "deployment/SKILL.md"), "unrelated deployment instructions")
	if _, err := os.ReadFile(filepath.Join(home, skillHome, "code-review/SKILL.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("withdrawn normalized assignment remained active after restoring defaults: %v", err)
	}
}

func TestInvalidAssignmentCannotEraseSkillsOrReadOutsideItsSource(t *testing.T) {
	home, assigned, outside := t.TempDir(), t.TempDir(), t.TempDir()
	operator := filepath.Join(t.TempDir(), "operator")
	configFile(t, filepath.Join(operator, "skills/global/SKILL.md"), "operator instructions")
	bundle := skillBundle(t, operator)
	if err := hydrateSkills(assigned, home, "", bundle); err != nil {
		t.Fatal(err)
	}
	protected := filepath.Join(outside, "private.md")
	configFile(t, protected, "ungranted private instructions")
	if err := os.Symlink(outside, filepath.Join(assigned, "global")); err != nil {
		t.Fatal(err)
	}
	if err := hydrateSkills(assigned, home, "", bundle); err == nil {
		t.Fatal("assigned skill followed a link outside its source")
	}
	skillContents(t, filepath.Join(home, skillHome, "global/SKILL.md"), "operator instructions")
	skillContents(t, protected, "ungranted private instructions")
	if _, err := os.ReadFile(filepath.Join(home, skillHome, "global/private.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid assignment disclosed instructions outside its source: %v", err)
	}
}

func TestSkillPublicationDoesNotFollowPrivateHomeLinks(t *testing.T) {
	home, assigned, outside := t.TempDir(), t.TempDir(), t.TempDir()
	operator := filepath.Join(t.TempDir(), "operator")
	configFile(t, filepath.Join(operator, "skills/global/SKILL.md"), "operator instructions")
	protected := filepath.Join(outside, "skills/global/SKILL.md")
	configFile(t, protected, "unrelated private instructions")
	if err := os.Symlink(outside, filepath.Join(home, ".codex")); err != nil {
		t.Fatal(err)
	}
	if err := hydrateSkills(assigned, home, "", skillBundle(t, operator)); err == nil {
		t.Fatal("skill publication followed a private HOME link")
	}
	skillContents(t, protected, "unrelated private instructions")
}

func TestPreparedGlobalSkillLinksKeepCommittedOperatorPrecedence(t *testing.T) {
	home, prepared := t.TempDir(), t.TempDir()
	worker, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	operator := filepath.Join(t.TempDir(), "operator")
	configFile(t, filepath.Join(operator, "skills/global/SKILL.md"), "committed operator instructions")
	bundle := skillBundle(t, operator)
	globalSkills, err := filepath.EvalSymlinks(filepath.Join(operator, "skills"))
	if err != nil {
		t.Fatal(err)
	}
	configFile(t, filepath.Join(prepared, "workdir/AGENTS.md"), "assigned task brief")
	configFile(t, filepath.Join(prepared, ".multica_sidecar_manifest.json"), `{"files":[]}`)
	configFile(t, filepath.Join(prepared, "codex-home/skills/task/SKILL.md"), "assigned task instructions")
	if err := os.Symlink(filepath.Join(globalSkills, "global"), filepath.Join(prepared, "codex-home/skills/global")); err != nil {
		t.Fatal(err)
	}
	// A controller-HOME edit is not a new selected configuration. The daemon's
	// link must not turn that edit into task-assigned content in the worker.
	configFile(t, filepath.Join(globalSkills, "global/SKILL.md"), "unselected controller edit")
	if err := checkout.SeedContext(prepared, worker, filepath.Join(t.TempDir(), "context.json"), "codex", globalSkills); err != nil {
		t.Fatal(err)
	}
	if err := hydrateSkills(filepath.Join(worker, "codex-skills"), home, "", bundle); err != nil {
		t.Fatal(err)
	}
	skillContents(t, filepath.Join(home, skillHome, "global/SKILL.md"), "committed operator instructions")
	skillContents(t, filepath.Join(home, skillHome, "task/SKILL.md"), "assigned task instructions")
	skillContents(t, filepath.Join(globalSkills, "global/SKILL.md"), "unselected controller edit")
}
