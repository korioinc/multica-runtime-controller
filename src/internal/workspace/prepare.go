package workspace

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/processgroup"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/pelletier/go-toml/v2"
)

const cleanupManifestName = ".multica_sidecar_manifest.json"
const preparationBytes = 1 << 20
const nativeTaskBytes = 64 << 20

const (
	ClaudeConfigDir         = ".claude"
	ClaudeSessionsDir       = "claude-sessions"
	ClaudeProjectDir        = "workdir"
	claudeSkillSettingsFile = "claude-runtime-skill-settings.json"
)

// SupportedProvider is the execution contract shared by preparation and workers.
func SupportedProvider(provider string) bool {
	return provider == "codex" || provider == "pi" || provider == "claude"
}

var ErrPreparationWritersUnproven = errors.New("preparation process group termination is unproven")

type nativeSkill struct {
	Name, Content string
	Files         []struct{ Path, Content string }
}

type nativeRuntimeSkill struct{ Key, Name string }

type NativeEnvironment struct {
	RootDir            string `json:"rootDir"`
	WorkDir            string `json:"workDir"`
	MulticaConfigRoot  string `json:"multicaConfigRoot"`
	CodexHome          string `json:"codexHome,omitempty"`
	ClaudeSettingsPath string `json:"claudeSettingsPath,omitempty"`
}

// Prepared is private controller authority. CleanupManifest must never be
// obtained from a worker or used as an authorization record outside this journal.
type Prepared struct {
	Conversation          *ConversationKey  `json:"conversation,omitempty"`
	WorkspaceAnchorTaskID string            `json:"workspaceAnchorTaskID,omitempty"`
	Digest                string            `json:"digest"`
	OwnerID               string            `json:"ownerID"`
	WorkspaceID           string            `json:"workspaceID"`
	TaskID                string            `json:"taskID"`
	AgentID               string            `json:"agentID"`
	AttemptID             string            `json:"attemptID"`
	Generation            uint64            `json:"generation"`
	PVCUID                string            `json:"pvcUID"`
	TaskRoot              string            `json:"taskRoot"`
	Provider              string            `json:"provider"`
	Executable            string            `json:"executable"`
	RuntimeDigest         string            `json:"runtimeDigest"`
	ConfigurationDigest   string            `json:"configurationDigest"`
	Environment           NativeEnvironment `json:"environment"`
	Repositories          []Repository      `json:"repositories"`
	SkillIDs              []string          `json:"skillIDs"`
	CreatedAt             time.Time         `json:"createdAt"`
	CleanupManifest       json.RawMessage   `json:"cleanupManifest"`
	AllowedLinks          map[string]string `json:"allowedLinks"`
	NativeMetadata        *NativeMetadata   `json:"nativeMetadata,omitempty"`
}

type Preparation struct {
	Conversation          ConversationKey
	WorkspaceAnchorTaskID string
	WorkerSessionID       string
	TurnSequence          uint64
	ScratchRoot           string
	// DiagnosticTaskID keeps private anchor-based generation attributed to the current task.
	DiagnosticTaskID string
	// LiveReuse is true only for the exact idle owner proved by the Store.
	LiveReuse                                        bool
	OwnerID, WorkspaceID, TaskID, AgentID, AttemptID string
	Generation                                       uint64
	PVCUID, WorkspacesRoot, TaskRoot                 string
	Provider, Executable, RuntimeDigest              string
	ConfigurationDigest, Command                     string
	CodexVersion, Profile, ResumeSessionID           string
	// CodexHelper is independently observed from the admitted immutable image.
	CodexHelper                    runtimeimage.Executable
	WorkspaceSlug, IssueIdentifier string
	CustomArgs, Environment        []string
	Task                           json.RawMessage
	Repositories                   []Repository
	SkillIDs                       []string
	IsReuse                        bool
	Prior                          *Prepared
	// Checkpoint durably retains validated native files before task configuration.
	Checkpoint func(Prepared) error
	// Only these controller-owned files may change after native validation.
	Instructions, ServiceTierConfig []byte
}

func preparedDigest(p Prepared) string {
	p.Digest = ""
	raw, _ := json.Marshal(p)
	return digest(raw)
}

func ValidatePrepared(p Prepared) error {
	if !canonicalUUID(p.OwnerID) || !canonicalUUID(p.WorkspaceID) || !canonicalUUID(p.TaskID) || !canonicalUUID(p.AgentID) || !canonicalUUID(p.AttemptID) || p.Generation == 0 || !validOpaque(p.PVCUID) || !canonicalPath(p.TaskRoot) || !canonicalPath(p.Executable) || !fingerprint(p.RuntimeDigest) || !fingerprint(p.ConfigurationDigest) || p.CreatedAt.IsZero() || p.Digest != preparedDigest(p) {
		return errors.New("invalid prepared task authority")
	}
	if p.WorkspaceAnchorTaskID != "" && (!canonicalUUID(p.WorkspaceAnchorTaskID) || p.Conversation == nil || !p.Conversation.Valid() ||
		p.Conversation.OwnerID != p.OwnerID || p.Conversation.WorkspaceID != p.WorkspaceID || p.Conversation.AgentID != p.AgentID ||
		p.Conversation.Kind == ConversationTask && p.Conversation.SubjectID != p.TaskID) || p.Conversation != nil && p.WorkspaceAnchorTaskID == "" {
		return errors.New("invalid conversation preparation authority")
	}
	if !SupportedProvider(p.Provider) {
		return errors.New("unsupported prepared provider")
	}
	if err := ValidateTaskRoot(filepath.Dir(filepath.Dir(p.TaskRoot)), p.TaskRoot, p.WorkspaceID, preparedAnchor(p)); err != nil {
		return errors.New("prepared root does not match task identity")
	}
	for path, target := range p.AllowedLinks {
		if p.NativeMetadata != nil && NativeProjectionLinks(p.Provider)[path] == target {
			continue
		}
		relative, ok := nativeHomeRelative(path)
		if !ok || p.Provider != "codex" || !canonicalPath(target) || !strings.HasSuffix(target, "/"+relative) {
			return errors.New("prepared task contains an unsupported HOME link")
		}
	}
	want := NativeEnvironment{RootDir: p.TaskRoot, WorkDir: filepath.Join(p.TaskRoot, "workdir"), MulticaConfigRoot: filepath.Join(p.TaskRoot, "multica-config")}
	if p.Provider == "codex" {
		want.CodexHome = filepath.Join(p.TaskRoot, "codex-home")
	}
	if p.Provider == "claude" && p.Environment.ClaudeSettingsPath != "" {
		want.ClaudeSettingsPath = filepath.Join(p.TaskRoot, claudeSkillSettingsFile)
		if p.NativeMetadata != nil {
			want.ClaudeSettingsPath = NativeMetadataView + "/" + claudeSkillSettingsFile
		}
	}
	if p.Environment != want || len(p.CleanupManifest) == 0 || len(p.CleanupManifest) > preparationBytes {
		return errors.New("invalid prepared environment")
	}
	_, err := decodeCleanup(p.TaskRoot, p.CleanupManifest)
	if err == nil && p.NativeMetadata != nil {
		err = p.NativeMetadata.Validate(p.Provider, p.TaskRoot)
		var marker struct {
			AgentID string `json:"agent_id"`
		}
		if json.Unmarshal(p.NativeMetadata.TaskMarker, &marker) != nil || marker.AgentID != p.AgentID {
			return errors.New("native guard belongs to a different agent")
		}
		for path, target := range NativeProjectionLinks(p.Provider) {
			if path != TaskResourcesRelative && p.AllowedLinks[path] != target {
				return errors.New("required native projection is missing")
			}
		}
	}
	return err
}

// PrepareTask requires the caller to hold journal-backed exclusive ownership and
// prove previous worker/helper writers stopped, throughout this call. No failure
// removes the task root, converts reuse to prepare, or grants execution authority.
func PrepareTask(ctx context.Context, input Preparation) (Prepared, error) {
	if input.WorkerSessionID != "" {
		return prepareResidentTask(ctx, input)
	}
	anchor := input.WorkspaceAnchorTaskID
	if anchor == "" {
		anchor = input.TaskID
	}
	if ValidateTaskRoot(input.WorkspacesRoot, input.TaskRoot, input.WorkspaceID, anchor) != nil || !canonicalPath(input.Command) || !json.Valid(input.Task) || len(input.Task) > nativeTaskBytes || input.IsReuse != (input.Prior != nil) || !input.IsReuse && anchor != input.TaskID {
		return Prepared{}, errors.New("invalid task preparation input")
	}
	if input.DiagnosticTaskID != "" && !canonicalUUID(input.DiagnosticTaskID) {
		return Prepared{}, errors.New("invalid diagnostic task identity")
	}
	if len(input.Instructions) > nativeTaskBytes || len(input.ServiceTierConfig) > preparationBytes || len(input.ServiceTierConfig) > 0 && !json.Valid(input.ServiceTierConfig) {
		return Prepared{}, errors.New("invalid task configuration input")
	}
	if len(input.ServiceTierConfig) == 0 {
		input.ServiceTierConfig = nil
	}
	p := Prepared{OwnerID: input.OwnerID, WorkspaceID: input.WorkspaceID, TaskID: input.TaskID, AgentID: input.AgentID, AttemptID: input.AttemptID, Generation: input.Generation, PVCUID: input.PVCUID, TaskRoot: input.TaskRoot, Provider: input.Provider, Executable: input.Executable, RuntimeDigest: input.RuntimeDigest, ConfigurationDigest: input.ConfigurationDigest, Repositories: slices.Clone(input.Repositories), SkillIDs: slices.Clone(input.SkillIDs), CreatedAt: time.Now().UTC(), CleanupManifest: json.RawMessage(`{}`), AllowedLinks: map[string]string{}}
	p.WorkspaceAnchorTaskID = input.WorkspaceAnchorTaskID
	if input.WorkspaceAnchorTaskID != "" {
		p.Conversation = clonePointer(&input.Conversation)
	}
	p.Environment = NativeEnvironment{RootDir: p.TaskRoot, WorkDir: filepath.Join(p.TaskRoot, "workdir"), MulticaConfigRoot: filepath.Join(p.TaskRoot, "multica-config")}
	if p.Provider == "codex" {
		p.Environment.CodexHome = filepath.Join(p.TaskRoot, "codex-home")
	}
	p.Digest = preparedDigest(p)
	if err := ValidatePrepared(p); err != nil {
		return Prepared{}, err
	}
	home := environmentValue(input.Environment, "HOME")
	if !canonicalPath(home) || environmentValue(input.Environment, "CODEX_HOME") != "" {
		return Prepared{}, errors.New("helper requires its current HOME and no CODEX_HOME override")
	}
	var homeLinks map[string]string
	var err error
	if p.Provider == "codex" {
		homeLinks, err = preparationHomeLinks(home)
		if err != nil {
			return Prepared{}, err
		}
	}
	var task struct {
		AgentID               string
		AgentSkills           []nativeSkill
		DisabledRuntimeSkills []nativeRuntimeSkill
	}
	if err := json.Unmarshal(input.Task, &task); err != nil || task.AgentID != input.AgentID {
		return Prepared{}, errors.New("native task context has a different agent")
	}
	for _, skill := range task.AgentSkills {
		if strings.TrimSpace(skill.Name) == "" || strings.TrimSpace(skill.Content) == "" {
			return Prepared{}, errors.New("empty bound skill")
		}
		for _, file := range skill.Files {
			if !fs.ValidPath(file.Path) || strings.Contains(file.Path, "\\") {
				return Prepared{}, errors.New("skill file escapes its directory")
			}
		}
	}
	base, err := openCanonicalRoot(input.WorkspacesRoot)
	if err != nil {
		return Prepared{}, err
	}
	defer base.Close()
	relative, _ := filepath.Rel(input.WorkspacesRoot, input.TaskRoot)
	if input.IsReuse {
		prior := *input.Prior
		if err := ValidatePrepared(prior); err != nil {
			return Prepared{}, err
		}
		if prior.OwnerID != p.OwnerID || prior.WorkspaceID != p.WorkspaceID || !samePreparationConversation(prior, p) || prior.AgentID != p.AgentID || prior.PVCUID != p.PVCUID || prior.TaskRoot != p.TaskRoot || prior.Provider != p.Provider || prior.Executable != p.Executable || prior.RuntimeDigest != p.RuntimeDigest || prior.ConfigurationDigest != p.ConfigurationDigest {
			return Prepared{}, errors.New("reuse authority belongs to a different task or configuration")
		}
		for link, target := range prior.AllowedLinks {
			rel, known := homeLinks[link]
			if !known || target != filepath.Join(home, rel) {
				return Prepared{}, errors.New("reuse has an untrusted HOME link")
			}
			p.AllowedLinks[link] = target
		}
		if p.Provider == "codex" {
			root, err := base.OpenRoot(relative)
			if err != nil {
				return Prepared{}, err
			}
			err = cleanCodexHelpers(root, input.CodexHelper)
			root.Close()
			if err != nil {
				return Prepared{}, err
			}
		}
		if err := verifyPreparedFiles(prior); err != nil {
			return Prepared{}, err
		}
		root, err := base.OpenRoot(relative)
		if err != nil {
			return Prepared{}, err
		}
		// Re-publish the validated private record, never the worker's bytes.
		err = root.WriteFile(cleanupManifestName, prior.CleanupManifest, 0600)
		root.Close()
		if err != nil {
			return Prepared{}, err
		}
	} else if _, err := base.Lstat(relative); !errors.Is(err, os.ErrNotExist) {
		return Prepared{}, errors.New("fresh preparation refuses an existing or inaccessible task root")
	}
	if err := pinNativeRoot(base, input); err != nil {
		return Prepared{}, err
	}
	params := map[string]any{"WorkspacesRoot": input.WorkspacesRoot, "Provider": input.Provider, "CodexVersion": input.CodexVersion, "CodexCustomArgs": input.CustomArgs, "Profile": input.Profile, "Task": input.Task}
	action := "prepare"
	if input.IsReuse {
		action = "reuse"
		params["WorkDir"] = p.Environment.WorkDir
		params["ResumeSessionID"] = input.ResumeSessionID
	} else {
		params["WorkspaceID"], params["TaskID"] = input.WorkspaceID, input.TaskID
		params["WorkspaceSlug"] = input.WorkspaceSlug
		params["IssueIdentifier"] = input.IssueIdentifier
	}
	payload, _ := json.Marshal(map[string]any{"action": action, action: params})
	if len(payload) > nativeTaskBytes+preparationBytes {
		return Prepared{}, errors.New("helper request exceeds preparation budget")
	}
	diagnosticTaskID := input.TaskID
	if input.DiagnosticTaskID != "" {
		diagnosticTaskID = input.DiagnosticTaskID
	}
	finishHelper := diagnostics.StartPhase("task_native_helper", diagnostics.TaskAttributes(diagnosticTaskID, input.AttemptID)...)
	response, err := runPreparation(ctx, input.Command, input.Environment, payload)
	finishHelper(err)
	if err != nil {
		return Prepared{}, err
	}
	var result struct {
		Environment *NativeEnvironment `json:"environment"`
		Error       string             `json:"error"`
	}
	if json.Unmarshal(response, &result) != nil || result.Error != "" || result.Environment == nil {
		// Native diagnostics may contain credential-bearing paths or input.
		return Prepared{}, errors.New("helper failed or returned an invalid environment")
	}
	if p.Provider == "claude" {
		p.Environment.ClaudeSettingsPath = result.Environment.ClaudeSettingsPath
		if p.Environment.ClaudeSettingsPath == "" && claudeSkillPolicyRequired(task.AgentSkills, task.DisabledRuntimeSkills) {
			return Prepared{}, errors.New("helper did not materialize disabled Claude skills")
		}
	}
	p.Digest = preparedDigest(p)
	if *result.Environment != p.Environment || ValidatePrepared(p) != nil {
		return Prepared{}, errors.New("helper returned an invalid environment")
	}
	root, err := base.OpenRoot(relative)
	if err != nil {
		return Prepared{}, err
	}
	defer root.Close()
	if p.Provider == "pi" && !input.IsReuse {
		if err := root.Mkdir("pi-sessions", 0700); err != nil {
			return Prepared{}, err
		}
	}
	if p.Provider == "claude" && !input.IsReuse {
		if err := root.Mkdir(ClaudeSessionsDir, 0700); err != nil {
			return Prepared{}, err
		}
	}
	for link, relative := range homeLinks {
		if target, err := root.Readlink(link); err == nil {
			if target != filepath.Join(home, relative) {
				return Prepared{}, errors.New("helper produced an untrusted HOME link")
			}
			p.AllowedLinks[link] = target
		}
	}
	p.CleanupManifest, err = readBounded(root, cleanupManifestName)
	if err != nil {
		return Prepared{}, errors.New("helper did not persist its cleanup manifest")
	}
	if _, err := decodeCleanup(p.TaskRoot, p.CleanupManifest); err != nil {
		return Prepared{}, err
	}
	if err := scanTaskTree(root, p.AllowedLinks); err != nil {
		return Prepared{}, err
	}
	if err := verifyNativeFiles(root, p); err != nil {
		return Prepared{}, err
	}
	// Skill injection can fail with a native warning. Verify every requested
	// skill body and supporting file before considering the helper successful.
	skillsPath := "workdir/.pi/skills"
	if p.Provider == "codex" {
		skillsPath = "codex-home/skills"
	} else if p.Provider == "claude" {
		skillsPath = filepath.Join("workdir", ClaudeConfigDir, "skills")
	}
	if err := verifyBoundSkills(root, skillsPath, task.AgentSkills); err != nil {
		return Prepared{}, err
	}
	p.Digest = preparedDigest(p)
	if err := ValidatePrepared(p); err != nil {
		return Prepared{}, err
	}
	if input.Checkpoint != nil {
		if err := input.Checkpoint(p); err != nil {
			return Prepared{}, err
		}
	}
	// The helper's writers have stopped and all task files were validated above.
	// Confined, fixed-path writes are the only subsequent filesystem mutation.
	if err := ctx.Err(); err != nil {
		return Prepared{}, err
	}
	if err := writeTaskConfiguration(root, input); err != nil {
		return Prepared{}, err
	}
	return p, nil
}

var codexHelperNames = []string{"apply_patch", "applypatch", "codex-execve-wrapper", "codex-linux-sandbox", ".lock"}
var codexHelperDirectory = regexp.MustCompile(`^codex-arg0[A-Za-z0-9]{6}$`)

// The preparation caller holds journal-backed ownership and has fenced every
// prior writer. Remove only proven Codex startup aliases before retained-tree
// validation. No task link is followed, and all groups validate before deletion.
func cleanCodexHelpers(task *os.Root, helper runtimeimage.Executable) error {
	parent := task
	var opened []*os.Root
	defer func() {
		for _, root := range opened {
			root.Close()
		}
	}()
	openDirectory := func(root *os.Root, name string) (*os.Root, os.FileInfo, error) {
		info, err := root.Lstat(name)
		if err != nil {
			return nil, nil, err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !info.IsDir() || !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
			return nil, nil, errors.New("Codex temporary directory is indirect or untrusted")
		}
		child, err := root.OpenRoot(name)
		if err != nil {
			return nil, nil, err
		}
		opened = append(opened, child)
		actual, err := child.Stat(".")
		if err != nil || !os.SameFile(info, actual) {
			return nil, nil, errors.New("Codex temporary directory changed while opening")
		}
		return child, info, nil
	}
	for _, name := range []string{"codex-home", "tmp", "arg0"} {
		child, _, err := openDirectory(parent, name)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		parent = child
	}
	entries, err := fs.ReadDir(parent.FS(), ".")
	if err != nil || len(entries) == 0 {
		return err
	}
	if !runtimeimage.ImmutablePath(helper.Path) || !canonicalPath(helper.Path) || !fingerprint(helper.SHA256) {
		return errors.New("Codex helper cleanup requires an admitted native executable")
	}
	sha, err := core.HashFile(helper.Path)
	if err != nil || sha != helper.SHA256 {
		return errors.New("Codex native helper differs from the admitted image observation")
	}
	type group struct {
		name  string
		root  *os.Root
		info  os.FileInfo
		files map[string]os.FileInfo
	}
	groups := make([]group, 0, len(entries))
	for _, entry := range entries {
		if !codexHelperDirectory.MatchString(entry.Name()) {
			return errors.New("Codex temporary root contains an unknown entry")
		}
		root, info, err := openDirectory(parent, entry.Name())
		if err != nil {
			return err
		}
		files, err := fs.ReadDir(root.FS(), ".")
		if err != nil {
			return err
		}
		// Graceful Codex shutdown can remove its aliases but leave this directory.
		if len(files) == 0 {
			continue
		}
		if len(files) != len(codexHelperNames) {
			return errors.New("Codex helper group contains incomplete or unexpected entries")
		}
		candidate := group{name: entry.Name(), root: root, info: info, files: make(map[string]os.FileInfo)}
		for _, name := range codexHelperNames {
			info, err := root.Lstat(name)
			if err != nil {
				return err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
				return errors.New("Codex helper entry has untrusted ownership or shared links")
			}
			if name == ".lock" {
				if !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || info.Size() != 0 {
					return errors.New("Codex helper lock is not an empty private regular file")
				}
			} else {
				target, err := root.Readlink(name)
				if info.Mode()&os.ModeSymlink == 0 || err != nil || target != helper.Path {
					return errors.New("Codex helper alias differs from the admitted native executable")
				}
			}
			candidate.files[name] = info
		}
		groups = append(groups, candidate)
	}
	for _, candidate := range groups {
		info, err := parent.Lstat(candidate.name)
		if err != nil || !info.IsDir() || !os.SameFile(info, candidate.info) {
			return errors.New("Codex helper directory changed before cleanup")
		}
		for _, name := range codexHelperNames {
			info, err := candidate.root.Lstat(name)
			if err != nil || !os.SameFile(info, candidate.files[name]) {
				return errors.New("Codex helper entry changed before cleanup")
			}
		}
		for _, name := range codexHelperNames {
			if err := candidate.root.Remove(name); err != nil {
				return err
			}
		}
		if err := parent.Remove(candidate.name); err != nil {
			return err
		}
	}
	return nil
}

func verifyBoundSkills(root *os.Root, skillsPath string, skills []nativeSkill) error {
	if len(skills) == 0 {
		return nil
	}
	bodies := make([][]byte, len(skills))
	for i, skill := range skills {
		body := strings.TrimSpace(skill.Content)
		if strings.HasPrefix(body, "---\n") {
			if _, rest, ok := strings.Cut(body[4:], "\n---"); ok {
				body = strings.TrimSpace(rest)
			}
		}
		if body == "" {
			return errors.New("empty bound skill body")
		}
		bodies[i] = []byte(body)
	}
	remaining := len(bodies)
	err := fs.WalkDir(root.FS(), skillsPath, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || entry.Name() != "SKILL.md" {
			return walkErr
		}
		raw, err := readBoundedLimit(root, path, nativeTaskBytes)
		if err != nil {
			return nil
		}
	candidate:
		for i, body := range bodies {
			if body == nil || !bytes.Contains(raw, body) {
				continue
			}
			for _, file := range skills[i].Files {
				if file.Path == "SKILL.md" {
					continue
				}
				content, err := readBoundedLimit(root, filepath.Join(filepath.Dir(path), file.Path), nativeTaskBytes)
				if err != nil || string(content) != file.Content {
					continue candidate
				}
			}
			bodies[i] = nil
			remaining--
		}
		if remaining == 0 {
			return fs.SkipAll
		}
		return nil
	})
	if err != nil || remaining != 0 {
		return errors.New("bound skill was not fully materialized")
	}
	return nil
}

func writeTaskConfiguration(root *os.Root, input Preparation) error {
	instructionsPath := "workdir/AGENTS.md"
	if input.Provider == "claude" {
		instructionsPath = "workdir/CLAUDE.md"
	}
	for _, file := range []struct {
		path string
		raw  []byte
	}{
		{"workdir/.pi/extensions/pi-openai-service-tier.json", input.ServiceTierConfig},
		{instructionsPath, input.Instructions},
	} {
		if file.raw == nil {
			continue
		}
		parent := "."
		for _, part := range strings.Split(filepath.Dir(file.path), "/") {
			parent = filepath.Join(parent, part)
			if err := root.Mkdir(parent, 0700); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			info, err := root.Lstat(parent)
			if err != nil || !info.IsDir() {
				return errors.New("task configuration directory is indirect or unavailable")
			}
		}
		if err := configuration.WriteFile(root, file.path, file.raw, 0600); err != nil {
			return err
		}
		info, err := root.Lstat(file.path)
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("task configuration is not a regular file")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return errors.New("task configuration has a shared or uninspectable hardlink")
		}
	}
	return nil
}

func canonicalPath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.ContainsAny(path, "\x00\r\n")
}

var nativeHomeLinks = map[string]string{
	"codex-home/auth.json":     ".codex/auth.json",
	"codex-home/plugins/cache": ".codex/plugins/cache",
}

func nativeHomeRelative(path string) (string, bool) {
	if relative, ok := nativeHomeLinks[path]; ok {
		return relative, true
	}
	name, ok := strings.CutPrefix(path, "codex-home/skills/")
	if !ok || name == "." || !fs.ValidPath(name) || strings.ContainsAny(name, "/\\\x00\r\n") {
		return "", false
	}
	return ".codex/skills/" + name, true
}

// Only captured HOME directories authorize links; task-owned names cannot add authority.
func preparationHomeLinks(home string) (map[string]string, error) {
	links := maps.Clone(nativeHomeLinks)
	root, err := openCanonicalRoot(home)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	for _, path := range []string{".codex", ".codex/skills"} {
		info, err := root.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			return links, nil
		}
		if err != nil || !info.IsDir() {
			return nil, errors.New("HOME skills directory is unavailable or indirect")
		}
	}
	entries, err := fs.ReadDir(root.FS(), ".codex/skills")
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			return nil, errors.New("HOME skill directory contains a symbolic link")
		}
		if entry.IsDir() {
			path := "codex-home/skills/" + entry.Name()
			relative, ok := nativeHomeRelative(path)
			if !ok {
				return nil, errors.New("HOME skill name is invalid")
			}
			links[path] = relative
		}
	}
	return links, nil
}

func environmentValue(environment []string, key string) string {
	var value string
	for _, pair := range environment {
		if v, ok := strings.CutPrefix(pair, key+"="); ok {
			value = v
		}
	}
	return value
}

func openCanonicalRoot(path string) (*os.Root, error) {
	actual, err := filepath.EvalSymlinks(path)
	if err != nil || actual != path {
		return nil, errors.New("workspace path contains a symlink or is unavailable")
	}
	return os.OpenRoot(path)
}

func pinNativeRoot(root *os.Root, input Preparation) error {
	anchor := input.WorkspaceAnchorTaskID
	if anchor == "" {
		anchor = input.TaskID
	}
	key := digest([]byte(input.WorkspaceID + "\x00" + anchor))[:32]
	directory := filepath.Join(".task_roots", key)
	path := filepath.Join(directory, "root.json")
	relative, _ := filepath.Rel(input.WorkspacesRoot, input.TaskRoot)
	want := struct {
		WorkspaceID  string `json:"workspace_id"`
		TaskID       string `json:"task_id"`
		RelativePath string `json:"relative_path"`
	}{input.WorkspaceID, anchor, relative}
	raw, _ := json.Marshal(want)
	if current, err := readBounded(root, path); err == nil {
		var actual = want
		actual.WorkspaceID, actual.TaskID, actual.RelativePath = "", "", ""
		if json.Unmarshal(current, &actual) != nil || actual != want {
			return errors.New("native task root index conflicts with journal")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if input.IsReuse {
		return errors.New("native task root index is missing for retained data")
	}
	if err := root.MkdirAll(directory, 0700); err != nil {
		return err
	}
	file, err := root.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(raw)
	return errors.Join(err, file.Sync(), file.Close())
}

type cleanupTargets struct {
	Files []string `json:"files,omitempty"`
	Dirs  []string `json:"dirs,omitempty"`
}

func decodeCleanup(taskRoot string, raw []byte) (cleanupTargets, error) {
	var targets cleanupTargets
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&targets); err != nil {
		return targets, errors.New("invalid native cleanup manifest")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return targets, errors.New("invalid trailing cleanup data")
	}
	seen := map[string]bool{}
	for _, path := range append(slices.Clone(targets.Files), targets.Dirs...) {
		rel, err := filepath.Rel(taskRoot, path)
		if err != nil || !canonicalPath(path) || !fs.ValidPath(rel) || rel == "." || seen[path] {
			return targets, errors.New("cleanup target escapes or aliases the task")
		}
		seen[path] = true
	}
	return targets, nil
}

// VerifyTaskTree checks a stopped task before worker admission. Allowed links
// come from controller authority, never from task-owned marker files.
func VerifyTaskTree(taskRoot string, allowedLinks map[string]string) error {
	root, err := openCanonicalRoot(taskRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	return scanTaskTree(root, allowedLinks)
}

// ValidateSession binds a provider pointer to a real regular file in this task.
// The Pi HOME form is interpreted against the fixed worker HOME contract, never
// against the controller process's ambient HOME or its filesystem links.
func ValidateSession(p Prepared, session string) error {
	if err := ValidatePrepared(p); err != nil {
		return err
	}
	root, err := openCanonicalRoot(p.TaskRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	var paths []string
	switch p.Provider {
	case "codex":
		if !canonicalUUID(session) {
			return errors.New("invalid Codex session identity")
		}
		for _, pattern := range []string{"codex-home/sessions/rollout-*-" + session + ".jsonl*", "codex-home/sessions/*/*/*/rollout-*-" + session + ".jsonl*"} {
			matches, err := fs.Glob(root.FS(), pattern)
			if err != nil {
				return errors.New("Codex session lookup failed")
			}
			paths = append(paths, matches...)
		}
	case "pi":
		if !canonicalPath(session) {
			return errors.New("invalid Pi session identity")
		}
		if strings.HasPrefix(session, "/home/multica/agents/.multica/pi-sessions/") {
			session = filepath.Join(p.TaskRoot, "pi-sessions", strings.TrimPrefix(session, "/home/multica/agents/.multica/pi-sessions/"))
		}
		rel, err := filepath.Rel(p.TaskRoot, session)
		if err != nil || !fs.ValidPath(rel) || !strings.HasPrefix(rel, "pi-sessions/") || filepath.Ext(rel) != ".jsonl" {
			return errors.New("Pi session belongs to a different task")
		}
		paths = []string{rel}
	case "claude":
		return validateClaudeSession(root, p, session)
	}
	for _, path := range paths {
		info, err := root.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			continue
		}
		file, err := root.Open(path)
		if err != nil {
			continue
		}
		line, readErr := bufio.NewReader(io.LimitReader(file, preparationBytes+1)).ReadBytes('\n')
		_ = file.Close()
		if readErr != nil && !errors.Is(readErr, io.EOF) || len(line) == 0 || len(line) > preparationBytes {
			continue
		}
		var header struct {
			Type    string `json:"type"`
			ID      string `json:"id"`
			Cwd     string `json:"cwd"`
			Payload struct {
				ID  string `json:"id"`
				Cwd string `json:"cwd"`
			} `json:"payload"`
		}
		if json.Unmarshal(line, &header) != nil {
			continue
		}
		if p.Provider == "codex" && header.Type == "session_meta" && header.Payload.ID == session && header.Payload.Cwd == p.Environment.WorkDir {
			return nil
		}
		if p.Provider == "pi" && header.Type == "session" && canonicalUUID(header.ID) && header.Cwd == p.Environment.WorkDir {
			return nil
		}
	}
	return errors.New("provider session is absent from this task")
}

func validateClaudeSession(root *os.Root, p Prepared, session string) error {
	if !canonicalUUID(session) {
		return errors.New("invalid Claude session identity")
	}
	directory := filepath.Join(ClaudeSessionsDir, ClaudeProjectDir)
	for _, path := range []string{ClaudeSessionsDir, directory} {
		info, err := root.Lstat(path)
		if err != nil || !info.IsDir() {
			return errors.New("Claude sessions directory is unavailable or indirect")
		}
	}
	path := filepath.Join(directory, session+".jsonl")
	info, err := root.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("Claude session is unavailable or indirect")
	}
	file, err := root.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return errors.New("Claude session changed while opening")
	}
	stat, ok := opened.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return errors.New("Claude session has a shared or uninspectable hardlink")
	}
	// Claude can write queue and attachment metadata before its first message.
	// Inspect only a bounded prefix, and never treat metadata as resumable work.
	reader := &io.LimitedReader{R: file, N: preparationBytes + 1}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), preparationBytes+1)
	for scanner.Scan() {
		if reader.N == 0 {
			return errors.New("Claude session prefix exceeds validation budget")
		}
		var record struct {
			Type      string `json:"type"`
			SessionID string `json:"sessionId"`
			Cwd       string `json:"cwd"`
			Message   struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
			} `json:"message"`
		}
		if json.Unmarshal(scanner.Bytes(), &record) != nil || record.Type == "" {
			return errors.New("malformed Claude session record")
		}
		if record.SessionID != "" && record.SessionID != session || record.Cwd != "" && record.Cwd != p.Environment.WorkDir {
			return errors.New("Claude session belongs to a different task or conversation")
		}
		if record.Type != "user" && record.Type != "assistant" {
			continue
		}
		if record.SessionID != session || record.Cwd != p.Environment.WorkDir || record.Message.Role != record.Type {
			return errors.New("Claude conversation record has no matching task identity")
		}
		var content string
		if json.Unmarshal(record.Message.Content, &content) == nil && strings.TrimSpace(content) != "" {
			return nil
		}
		var blocks []struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(record.Message.Content, &blocks) == nil && len(blocks) > 0 {
			for _, block := range blocks {
				if strings.TrimSpace(block.Type) == "" {
					return errors.New("malformed Claude conversation content")
				}
			}
			return nil
		}
		return errors.New("Claude conversation record has no content")
	}
	return errors.New("Claude session has no resumable conversation")
}

func verifyPreparedFiles(p Prepared) error {
	root, err := openCanonicalRoot(p.TaskRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	if p.NativeMetadata != nil {
		return errors.Join(verifyNativeProjectionBindings(root, p), verifyNativeFiles(root, p))
	}
	if err := scanTaskTree(root, p.AllowedLinks); err != nil {
		return err
	}
	want, err := decodeCleanup(p.TaskRoot, p.CleanupManifest)
	if err != nil {
		return err
	}
	current, err := readBounded(root, cleanupManifestName)
	if err != nil {
		return errors.New("retained cleanup manifest is missing")
	}
	actual, err := decodeCleanup(p.TaskRoot, current)
	if err != nil || !slices.Equal(want.Files, actual.Files) || !slices.Equal(want.Dirs, actual.Dirs) {
		return errors.New("retained cleanup targets differ from private journal")
	}
	return verifyNativeFiles(root, p)
}

func scanTaskTree(root *os.Root, allowed map[string]string) error {
	return fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := root.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := root.Readlink(path)
			if err == nil && allowed[path] != "" && target == allowed[path] {
				return nil
			}
			// A worker checkout may contain internal links. Resolve the entire
			// chain inside that repository, excluding private task metadata.
			parts := strings.SplitN(path, "/", 3)
			if len(parts) == 3 && parts[0] == "workdir" {
				repo, err := root.OpenRoot(parts[0] + "/" + parts[1])
				if err == nil {
					defer repo.Close()
					git, gitErr := repo.Lstat(".git")
					if gitErr == nil && git.IsDir() {
						return checkout.VerifyLink(repo, parts[2])
					}
				}
			}
			return errors.New("task has an untrusted symbolic link")
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return errors.New("task has a special filesystem entry")
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); info.Mode().IsRegular() && (!ok || stat.Nlink != 1) {
			return errors.New("task has a shared or uninspectable hardlink")
		}
		if entry.Name() == ".git" && !info.IsDir() || strings.HasSuffix(path, "/objects/info/alternates") || strings.HasSuffix(path, "/objects/info/http-alternates") || strings.HasSuffix(path, "/.git/commondir") {
			return errors.New("task Git directory references external objects")
		}
		return nil
	})
}

func verifyNativeFiles(root *os.Root, p Prepared) error {
	if p.WorkspaceAnchorTaskID != "" {
		raw, err := readBounded(root, ".task_owner")
		var owner struct {
			WorkspaceID string `json:"workspace_id"`
			TaskID      string `json:"task_id"`
		}
		if err != nil {
			return errors.New("native workspace owner is unavailable")
		}
		if json.Unmarshal(raw, &owner) != nil {
			owner.TaskID = strings.TrimSpace(string(raw))
		}
		if owner.TaskID != p.WorkspaceAnchorTaskID || owner.WorkspaceID != "" && owner.WorkspaceID != p.WorkspaceID {
			return errors.New("native workspace owner differs from its anchor")
		}
	}
	for _, directory := range []string{"workdir", "multica-config"} {
		info, err := root.Lstat(directory)
		if err != nil || !info.IsDir() {
			return errors.New("prepared native directory is unavailable")
		}
	}
	if p.NativeMetadata != nil {
		return verifyNativeProjectionBindings(root, p)
	}
	marker, err := readBounded(root, "workdir/.multica/daemon_task_context.json")
	var context struct {
		AgentID   string `json:"agent_id"`
		ManagedBy string `json:"managed_by"`
	}
	if err != nil || json.Unmarshal(marker, &context) != nil || context.AgentID != p.AgentID || context.ManagedBy != "multica-daemon-task" {
		return errors.New("native task context was not materialized")
	}
	switch p.Provider {
	case "codex":
		info, err := root.Lstat("codex-home/sessions")
		if err != nil || !info.IsDir() {
			return errors.New("task-local Codex sessions directory is unavailable")
		}
		config, err := readBounded(root, "codex-home/config.toml")
		var parsed map[string]any
		if err != nil || len(config) == 0 || toml.Unmarshal(config, &parsed) != nil {
			return errors.New("Codex configuration was not materialized")
		}
	case "pi":
		info, err := root.Lstat("pi-sessions")
		if err != nil || !info.IsDir() {
			return errors.New("task-local Pi sessions directory is unavailable")
		}
	case "claude":
		info, err := root.Lstat(ClaudeSessionsDir)
		if err != nil || !info.IsDir() {
			return errors.New("task-local Claude sessions directory is unavailable")
		}
		if p.Environment.ClaudeSettingsPath != "" {
			return verifyClaudeSkillSettings(root)
		}
	default:
		return errors.New("unsupported native provider")
	}
	return nil
}

func verifyClaudeSkillSettings(root *os.Root) error {
	raw, err := readBounded(root, claudeSkillSettingsFile)
	if err != nil {
		return errors.New("Claude skill policy is unavailable")
	}
	var policy struct {
		SkillOverrides map[string]string `json:"skillOverrides"`
		Permissions    struct {
			Deny []string `json:"deny"`
		} `json:"permissions"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&policy) != nil || decoder.Decode(new(any)) != io.EOF || len(policy.SkillOverrides) == 0 && len(policy.Permissions.Deny) == 0 {
		return errors.New("invalid Claude skill policy")
	}
	for name, state := range policy.SkillOverrides {
		if strings.TrimSpace(name) == "" || strings.ContainsAny(name, "\x00\r\n") || state != "off" {
			return errors.New("Claude skill policy does not disable its inherited skill")
		}
	}
	for _, rule := range policy.Permissions.Deny {
		if !strings.HasPrefix(rule, "Skill(") || !strings.HasSuffix(rule, ")") || len(rule) <= len("Skill()") || strings.ContainsAny(rule, "\x00\r\n") {
			return errors.New("invalid Claude skill denial")
		}
	}
	return nil
}

var nativeSkillNamePattern = regexp.MustCompile(`[^a-z0-9]+`)

// The pinned helper skips inherited skills shadowed by admitted bound skills.
// Its execenv.sanitizeSkillName lowercases names, replaces non-ASCII-alphanumeric
// runs with a hyphen, trims hyphens, and substitutes "skill" for an empty name.
// Reproduce only that omission check: Reuse can also omit a failed policy write.
func claudeSkillPolicyRequired(skills []nativeSkill, disabled []nativeRuntimeSkill) bool {
	nameKey := func(name string) string {
		name = strings.Trim(nativeSkillNamePattern.ReplaceAllString(strings.ToLower(strings.TrimSpace(name)), "-"), "-")
		if name == "" {
			return "skill"
		}
		return name
	}
	bound := make(map[string]bool, len(skills))
	for _, skill := range skills {
		bound[nameKey(skill.Name)] = true
	}
	for _, skill := range disabled {
		name := strings.TrimSpace(skill.Name)
		if name == "" {
			name = filepath.Base(filepath.Clean(filepath.FromSlash(strings.TrimSpace(skill.Key))))
		}
		if !bound[nameKey(name)] {
			return true
		}
	}
	return false
}

func preparedAnchor(p Prepared) string {
	if p.WorkspaceAnchorTaskID != "" {
		return p.WorkspaceAnchorTaskID
	}
	return p.TaskID
}

func samePreparationConversation(prior, current Prepared) bool {
	if prior.Conversation == nil || current.Conversation == nil {
		return prior.Conversation == nil && current.Conversation == nil && prior.TaskID == current.TaskID
	}
	return *prior.Conversation == *current.Conversation && preparedAnchor(prior) == preparedAnchor(current)
}

func readBounded(root *os.Root, path string) ([]byte, error) {
	return readBoundedLimit(root, path, preparationBytes)
}

func readBoundedLimit(root *os.Root, path string, limit int64) ([]byte, error) {
	info, err := root.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errors.New("preparation file is not a bounded regular file")
	}
	file, err := root.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if int64(len(raw)) > limit {
		return nil, errors.New("preparation file exceeds budget")
	}
	return raw, err
}

type preparationBuffer struct{ bytes.Buffer }

func (b *preparationBuffer) Write(raw []byte) (int, error) {
	if b.Len()+len(raw) > preparationBytes {
		return 0, errors.New("helper output exceeds budget")
	}
	return b.Buffer.Write(raw)
}

func runPreparation(ctx context.Context, binary string, environment []string, payload []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "__multica_execenv_prepare")
	command.Env, command.Stdin = environment, bytes.NewReader(payload)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = time.Second
	var output, diagnostic preparationBuffer
	command.Stdout, command.Stderr = &output, &diagnostic
	if err := command.Start(); err != nil {
		return nil, errors.New("preparation helper could not start")
	}
	err := command.Wait()
	// A helper's successful exit does not prove its descendants stopped.
	if stopErr := processgroup.Stop(command.Process.Pid); stopErr != nil {
		return nil, errors.Join(ErrPreparationWritersUnproven, stopErr)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, errors.New("preparation helper failed")
	}
	return output.Bytes(), nil
}
