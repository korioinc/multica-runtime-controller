package execution

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

const codexSkillHome = ".codex/skills"

type taskHomeFixture struct {
	worker, task, globalHome string
	request                  wire.Request
	manifest                 runtimeimage.Descriptor
	bundle                   configuration.Bundle
}

func canonicalHomeDirectory(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func newTaskHomeFixture(t *testing.T, bundle configuration.Bundle, seed string) *taskHomeFixture {
	t.Helper()
	sha := core.Digest([]byte("task HOME fixture installed code"))
	contract := core.Contract{SchemaVersion: core.Version, ControllerABI: core.ABI, BuildID: sha, Platform: "linux/amd64", RuntimePath: core.Root + "/runtime", RuntimeSHA256: sha, GoVersion: "go1.26.1", ShimPaths: map[string]string{}}
	for _, alias := range []string{"pi", "codex", "copilot", "agy"} {
		contract.ShimPaths[alias] = core.Root + "/shims/" + alias
	}
	manifest := runtimeimage.Descriptor{SchemaVersion: 1, Kind: "multica-runtime-image", ImageBuildID: uuid.NewString(), Platform: contract.Platform, Controller: contract, Daemon: runtimeimage.Daemon{Executable: runtimeimage.Executable{Path: "/opt/tools/multica", Version: "0.4.41", SHA256: sha}, AdapterContract: runtimeimage.AdapterContract}, Providers: map[string]runtimeimage.Executable{"codex": {Path: "/opt/tools/codex", Version: "1.0.0", SHA256: sha}, "pi": {Path: "/opt/tools/pi", Version: "1.0.0", SHA256: sha}}, BinDirs: []string{"/opt/tools"}, HomeSeed: seed}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := manifest.Reference("registry.example/runtime@sha256:"+sha, core.Digest(raw), bundle.Digest)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &taskHomeFixture{worker: canonicalHomeDirectory(t), task: canonicalHomeDirectory(t), globalHome: canonicalHomeDirectory(t), bundle: bundle, manifest: manifest, request: wire.Request{SchemaVersion: wire.RequestSchemaVersion, TaskID: uuid.NewString(), AttemptID: uuid.NewString(), OwnerID: uuid.NewString(), WorkerSubPath: ".multica-runtime/workers/" + uuid.NewString(), Provider: "codex", RuntimeRef: ref}}
	if err := os.MkdirAll(filepath.Join(fixture.task, "codex-home/skills"), 0700); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture *taskHomeFixture) prepare(t *testing.T) string {
	t.Helper()
	digest, err := prepareTaskHome(fixture.request, fixture.worker, fixture.task, fixture.globalHome, fixture.manifest, fixture.bundle)
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.HomeDigest = digest
	return filepath.Join(fixture.worker, taskHomeArtifacts, fixture.request.AttemptID+".tar")
}

func (fixture *taskHomeFixture) install(t *testing.T) string {
	t.Helper()
	home := canonicalHomeDirectory(t)
	artifact := fixture.prepare(t)
	if err := InstallTaskHome(home, artifact, fixture.request); err != nil {
		t.Fatal(err)
	}
	return home
}

func taskHomeBundle(t *testing.T, operator string) configuration.Bundle {
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

func taskHomeContents(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != want {
		t.Fatalf("task HOME changed selected content at %s: got %q, want %q, error %v", path, got, want, err)
	}
}

func TestTaskHomeRestoresCommittedDefaultsAfterAssignmentWithdrawal(t *testing.T) {
	operator, seed := canonicalHomeDirectory(t), canonicalHomeDirectory(t)
	configFile(t, filepath.Join(operator, "skills/shared/SKILL.md"), "operator instructions")
	configFile(t, filepath.Join(operator, "skills/shared/references/operator.md"), "operator reference")
	configFile(t, filepath.Join(operator, "skills/global/SKILL.md"), "global instructions")
	configFile(t, filepath.Join(operator, "auth.json"), "committed credential")
	configFile(t, filepath.Join(seed, codexSkillHome, "shared/SKILL.md"), "image instructions")
	configFile(t, filepath.Join(seed, codexSkillHome, "shared/references/image.md"), "image reference")
	configFile(t, filepath.Join(seed, codexSkillHome, "builtin/SKILL.md"), "image default")
	fixture := newTaskHomeFixture(t, taskHomeBundle(t, operator), seed)
	assigned := filepath.Join(fixture.task, "codex-home/skills")
	configFile(t, filepath.Join(assigned, "shared/SKILL.md"), "assigned instructions")
	configFile(t, filepath.Join(assigned, "temporary/SKILL.md"), "temporary assignment")
	first := fixture.install(t)
	taskHomeContents(t, filepath.Join(first, codexSkillHome, "shared/SKILL.md"), "assigned instructions")
	taskHomeContents(t, filepath.Join(first, codexSkillHome, "temporary/SKILL.md"), "temporary assignment")
	taskHomeContents(t, filepath.Join(first, codexSkillHome, "global/SKILL.md"), "global instructions")
	taskHomeContents(t, filepath.Join(first, codexSkillHome, "builtin/SKILL.md"), "image default")
	for _, name := range []string{"operator.md", "image.md"} {
		if _, err := os.ReadFile(filepath.Join(first, codexSkillHome, "shared/references", name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("task assignment inherited instructions from a replaced base", err)
		}
	}
	configFile(t, filepath.Join(first, ".codex/auth.json"), "private refreshed credential")
	configFile(t, filepath.Join(first, codexSkillHome, "shared/SKILL.md"), "previous task customization")
	configFile(t, filepath.Join(operator, "skills/shared/SKILL.md"), "unselected operator revision")
	if err := os.RemoveAll(assigned); err != nil {
		t.Fatal(err)
	}
	fixture.request.TaskID, fixture.request.AttemptID = uuid.NewString(), uuid.NewString()
	second := fixture.install(t)
	taskHomeContents(t, filepath.Join(second, codexSkillHome, "shared/SKILL.md"), "operator instructions")
	taskHomeContents(t, filepath.Join(second, codexSkillHome, "shared/references/operator.md"), "operator reference")
	taskHomeContents(t, filepath.Join(second, codexSkillHome, "shared/references/image.md"), "image reference")
	if _, err := os.ReadFile(filepath.Join(second, codexSkillHome, "temporary/SKILL.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("withdrawn instructions leaked into the next task", err)
	}
	taskHomeContents(t, filepath.Join(second, ".codex/auth.json"), "committed credential")
	taskHomeContents(t, filepath.Join(first, ".codex/auth.json"), "private refreshed credential")
}

func TestTaskHomeAssignmentReplacesEveryNormalizedDefault(t *testing.T) {
	operator, seed := canonicalHomeDirectory(t), canonicalHomeDirectory(t)
	configFile(t, filepath.Join(operator, "skills/Code Review/SKILL.md"), "operator review")
	configFile(t, filepath.Join(operator, "skills/code_review/SKILL.md"), "second operator review")
	configFile(t, filepath.Join(operator, "skills/deployment/SKILL.md"), "unrelated deployment")
	configFile(t, filepath.Join(seed, codexSkillHome, "CODE.review/SKILL.md"), "image review")
	fixture := newTaskHomeFixture(t, taskHomeBundle(t, operator), seed)
	assigned := filepath.Join(fixture.task, "codex-home/skills/code-review")
	configFile(t, filepath.Join(assigned, "SKILL.md"), "assigned review")
	first := fixture.install(t)
	taskHomeContents(t, filepath.Join(first, codexSkillHome, "code-review/SKILL.md"), "assigned review")
	taskHomeContents(t, filepath.Join(first, codexSkillHome, "deployment/SKILL.md"), "unrelated deployment")
	for _, name := range []string{"Code Review", "code_review", "CODE.review"} {
		if _, err := os.ReadFile(filepath.Join(first, codexSkillHome, name, "SKILL.md")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("base instructions remained active beside their normalized assignment", err)
		}
	}
	if err := os.RemoveAll(assigned); err != nil {
		t.Fatal(err)
	}
	fixture.request.TaskID, fixture.request.AttemptID = uuid.NewString(), uuid.NewString()
	second := fixture.install(t)
	taskHomeContents(t, filepath.Join(second, codexSkillHome, "Code Review/SKILL.md"), "operator review")
	taskHomeContents(t, filepath.Join(second, codexSkillHome, "code_review/SKILL.md"), "second operator review")
	taskHomeContents(t, filepath.Join(second, codexSkillHome, "CODE.review/SKILL.md"), "image review")
}

func TestTaskHomeGlobalLinksCannotOverrideCommittedConfiguration(t *testing.T) {
	operator := canonicalHomeDirectory(t)
	configFile(t, filepath.Join(operator, "skills/global/SKILL.md"), "committed global instructions")
	fixture := newTaskHomeFixture(t, taskHomeBundle(t, operator), "")
	globalSkills := filepath.Join(fixture.globalHome, ".codex/skills")
	configFile(t, filepath.Join(fixture.task, "codex-home/skills/task/SKILL.md"), "assigned task instructions")
	configFile(t, filepath.Join(globalSkills, "global/SKILL.md"), "unselected controller edit")
	if err := os.Symlink(filepath.Join(globalSkills, "global"), filepath.Join(fixture.task, "codex-home/skills/global")); err != nil {
		t.Fatal(err)
	}
	home := fixture.install(t)
	taskHomeContents(t, filepath.Join(home, codexSkillHome, "global/SKILL.md"), "committed global instructions")
	taskHomeContents(t, filepath.Join(home, codexSkillHome, "task/SKILL.md"), "assigned task instructions")
	taskHomeContents(t, filepath.Join(globalSkills, "global/SKILL.md"), "unselected controller edit")
}

func TestRejectedTaskAssignmentPreservesPreviouslyInstalledPrivateHome(t *testing.T) {
	operator, outside := canonicalHomeDirectory(t), canonicalHomeDirectory(t)
	configFile(t, filepath.Join(operator, "skills/global/SKILL.md"), "operator instructions")
	fixture := newTaskHomeFixture(t, taskHomeBundle(t, operator), "")
	home := fixture.install(t)
	configFile(t, filepath.Join(outside, "SKILL.md"), "ungranted private instructions")
	if err := os.Symlink(outside, filepath.Join(fixture.task, "codex-home/skills/global")); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareTaskHome(fixture.request, fixture.worker, fixture.task, fixture.globalHome, fixture.manifest, fixture.bundle); err == nil {
		t.Fatal("unapproved assignment acquired task HOME authority")
	}
	taskHomeContents(t, filepath.Join(home, codexSkillHome, "global/SKILL.md"), "operator instructions")
	taskHomeContents(t, filepath.Join(outside, "SKILL.md"), "ungranted private instructions")
}
