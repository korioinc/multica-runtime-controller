package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/pelletier/go-toml/v2"
)

func prepareResidentTask(ctx context.Context, input Preparation) (Prepared, error) {
	if !canonicalUUID(input.WorkerSessionID) || input.TurnSequence == 0 || !canonicalPath(input.ScratchRoot) ||
		input.ScratchRoot == input.WorkspacesRoot || strings.HasPrefix(input.ScratchRoot, input.WorkspacesRoot+"/") {
		return Prepared{}, errors.New("resident preparation requires private scratch and an exact turn")
	}
	if err := os.MkdirAll(input.ScratchRoot, 0700); err != nil {
		return Prepared{}, err
	}
	private, err := openCanonicalRoot(input.ScratchRoot)
	if err != nil {
		return Prepared{}, err
	}
	private.Close()
	scratch, err := os.MkdirTemp(input.ScratchRoot, "native-")
	if err != nil {
		return Prepared{}, err
	}
	defer os.RemoveAll(scratch)
	anchor := input.WorkspaceAnchorTaskID
	if anchor == "" {
		anchor = input.TaskID
	}
	if ValidateTaskRoot(input.WorkspacesRoot, input.TaskRoot, input.WorkspaceID, anchor) != nil || input.IsReuse != (input.Prior != nil) {
		return Prepared{}, errors.New("invalid resident task root")
	}
	helper := input
	helper.DiagnosticTaskID = input.TaskID
	helper.WorkerSessionID, helper.TurnSequence, helper.ScratchRoot, helper.LiveReuse = "", 0, "", false
	helper.TaskID, helper.WorkspaceAnchorTaskID, helper.Conversation = anchor, "", ConversationKey{}
	helper.WorkspacesRoot, helper.IsReuse, helper.Prior, helper.Checkpoint = scratch, false, nil, nil
	helper.Instructions, helper.ServiceTierConfig = nil, nil
	helper.TaskRoot, err = TaskRoot(scratch, helper.WorkspaceID, anchor, input.WorkspaceSlug, input.IssueIdentifier)
	if err != nil {
		return Prepared{}, err
	}
	generated, err := PrepareTask(ctx, helper)
	if err != nil {
		return Prepared{}, err
	}
	root, err := openCanonicalRoot(generated.TaskRoot)
	if err != nil {
		return Prepared{}, err
	}
	defer root.Close()
	metadata := &NativeMetadata{WorkerSessionID: input.WorkerSessionID, TurnSequence: input.TurnSequence,
		ArtifactRoot: NativeArtifactRoot(input.TaskRoot, input.WorkerSessionID, input.TurnSequence), Artifacts: []NativeArtifact{}}
	metadata.TaskMarker, err = readBounded(root, TaskMarkerRelative)
	if err != nil {
		return Prepared{}, err
	}
	metadata.ProjectResources, err = readBounded(root, TaskResourcesRelative)
	if errors.Is(err, os.ErrNotExist) {
		metadata.ProjectResources, err = nil, nil
	}
	if err != nil {
		metadata.ProjectResources = nil
	}
	if input.Provider == "codex" {
		metadata.CodexConfig, err = readBoundedLimit(root, "codex-home/config.toml", MaxAssignmentBytes)
		if err == nil {
			metadata.CodexConfig, err = rebaseCodexSkillPaths(metadata.CodexConfig, generated.Environment.CodexHome, input.TaskRoot+"/codex-home")
		}
		if err != nil {
			return Prepared{}, err
		}
	} else if input.Provider == "claude" && generated.Environment.ClaudeSettingsPath != "" {
		metadata.ClaudeSettings, err = readBounded(root, claudeSkillSettingsFile)
		if err != nil {
			return Prepared{}, err
		}
	} else if input.Provider == "pi" {
		metadata.ServiceTier = slices.Clone(input.ServiceTierConfig)
	}
	files, err := residentSkillFiles(root, input.Provider)
	if err != nil {
		return Prepared{}, err
	}
	for path, raw := range files {
		metadata.Artifacts = append(metadata.Artifacts, NativeArtifact{Path: path, SHA256: core.Digest(raw), Size: int64(len(raw))})
	}
	slices.SortFunc(metadata.Artifacts, func(a, b NativeArtifact) int { return strings.Compare(a.Path, b.Path) })
	if metadata.Validate(input.Provider, input.TaskRoot) != nil {
		// Resources are optional; mandatory configuration must fit privately.
		metadata.ProjectResources = nil
		if err := metadata.Validate(input.Provider, input.TaskRoot); err != nil {
			return Prepared{}, err
		}
	}
	p := Prepared{Conversation: clonePointer(&input.Conversation), WorkspaceAnchorTaskID: anchor,
		OwnerID: input.OwnerID, WorkspaceID: input.WorkspaceID, TaskID: input.TaskID, AgentID: input.AgentID,
		AttemptID: input.AttemptID, Generation: input.Generation, PVCUID: input.PVCUID, TaskRoot: input.TaskRoot,
		Provider: input.Provider, Executable: input.Executable, RuntimeDigest: input.RuntimeDigest, ConfigurationDigest: input.ConfigurationDigest,
		Repositories: slices.Clone(input.Repositories), SkillIDs: slices.Clone(input.SkillIDs), CreatedAt: time.Now().UTC(), CleanupManifest: json.RawMessage(`{}`),
		AllowedLinks: NativeProjectionLinks(input.Provider), NativeMetadata: metadata,
		Environment: NativeEnvironment{RootDir: input.TaskRoot, WorkDir: input.TaskRoot + "/workdir", MulticaConfigRoot: input.TaskRoot + "/multica-config"}}
	if input.Provider == "codex" {
		p.Environment.CodexHome = input.TaskRoot + "/codex-home"
	}
	if len(metadata.ClaudeSettings) != 0 {
		p.Environment.ClaudeSettingsPath = NativeMetadataView + "/" + claudeSkillSettingsFile
	}
	if input.Prior != nil {
		prior := *input.Prior
		if ValidatePrepared(prior) != nil || prior.OwnerID != p.OwnerID || prior.WorkspaceID != p.WorkspaceID || !samePreparationConversation(prior, p) ||
			prior.AgentID != p.AgentID || prior.PVCUID != p.PVCUID || prior.TaskRoot != p.TaskRoot || prior.Provider != p.Provider ||
			prior.Executable != p.Executable || prior.RuntimeDigest != p.RuntimeDigest || prior.ConfigurationDigest != p.ConfigurationDigest {
			return Prepared{}, errors.New("resident preparation changed workspace authority")
		}
		if input.LiveReuse && (prior.NativeMetadata == nil || prior.NativeMetadata.WorkerSessionID != input.WorkerSessionID || prior.NativeMetadata.TurnSequence >= input.TurnSequence) {
			return Prepared{}, errors.New("live preparation belongs to a different desktop owner")
		}
		if prior.NativeMetadata != nil {
			p.AllowedLinks = maps.Clone(prior.AllowedLinks)
		}
	}
	if p.AllowedLinks[TaskResourcesRelative] == "" {
		metadata.ProjectResources = nil
	}
	base, err := openCanonicalRoot(input.WorkspacesRoot)
	if err != nil {
		return Prepared{}, err
	}
	defer base.Close()
	relative, _ := filepath.Rel(input.WorkspacesRoot, input.TaskRoot)
	if !input.IsReuse {
		parent, name := filepath.Split(relative)
		if err := makeNativeDirectories(base, strings.TrimSuffix(parent, "/")); err != nil {
			return Prepared{}, err
		}
		directory, err := OpenNativeDirectory(base, strings.TrimSuffix(parent, "/"))
		if err != nil {
			return Prepared{}, err
		}
		err = directory.Mkdir(name, 0700)
		directory.Close()
		if err != nil {
			return Prepared{}, errors.New("fresh resident setup refuses an existing root")
		}
	}
	live, err := OpenNativeDirectory(base, relative)
	if err != nil {
		return Prepared{}, err
	}
	defer live.Close()
	if input.IsReuse {
		prior := *input.Prior
		if prior.NativeMetadata != nil && p.AllowedLinks[TaskResourcesRelative] != "" {
			parent, name := filepath.Split(TaskResourcesRelative)
			directory, err := OpenNativeDirectory(live, strings.TrimSuffix(parent, "/"))
			var target string
			if err == nil {
				target, err = directory.Readlink(name)
				directory.Close()
			}
			if err != nil || target != p.AllowedLinks[TaskResourcesRelative] {
				delete(p.AllowedLinks, TaskResourcesRelative)
				metadata.ProjectResources = nil
				prior.AllowedLinks = maps.Clone(prior.AllowedLinks)
				delete(prior.AllowedLinks, TaskResourcesRelative)
			}
		}
		if err := verifyNativeFiles(live, prior); err != nil {
			return Prepared{}, err
		}
	} else {
		for _, name := range []string{"workdir", "multica-config", "codex-home/sessions", "pi-sessions", ClaudeSessionsDir + "/" + ClaudeProjectDir} {
			if err := makeNativeDirectories(live, name); err != nil {
				return Prepared{}, err
			}
		}
		owner, err := readBounded(root, ".task_owner")
		if err != nil {
			return Prepared{}, err
		}
		if err := writeNativeExclusive(live, ".task_owner", owner); err != nil {
			return Prepared{}, err
		}
		if err := writeNativeExclusive(live, cleanupManifestName, p.CleanupManifest); err != nil {
			return Prepared{}, err
		}
	}
	if err := pinNativeRoot(base, input); err != nil {
		return Prepared{}, err
	}
	if !input.LiveReuse {
		if err := establishNativeProjections(live, &p, input); err != nil {
			return Prepared{}, err
		}
	}
	if err := verifyNativeProjectionBindings(live, p); err != nil {
		return Prepared{}, err
	}
	if err := publishNativeArtifacts(live, p, files); err != nil {
		return Prepared{}, err
	}
	actual, err := live.Stat(".")
	current, currentErr := base.Lstat(relative)
	if err != nil || currentErr != nil || !os.SameFile(actual, current) {
		return Prepared{}, errors.New("resident task directory changed during preparation")
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
	return p, nil
}

func rebaseCodexSkillPaths(raw []byte, scratch, actual string) ([]byte, error) {
	var config map[string]any
	if err := toml.Unmarshal(raw, &config); err != nil {
		return nil, err
	}
	if skills, ok := config["skills"].(map[string]any); ok {
		if entries, ok := skills["config"].([]any); ok {
			for _, entry := range entries {
				item, ok := entry.(map[string]any)
				if !ok {
					return nil, errors.New("unsupported Codex skill configuration")
				}
				path, ok := item["path"].(string)
				if !ok {
					return nil, errors.New("invalid Codex disabled skill path")
				}
				if strings.HasPrefix(path, scratch+"/") {
					relative, err := filepath.Rel(scratch, path)
					if err != nil || !fs.ValidPath(relative) || !strings.HasPrefix(relative, "skills/") {
						return nil, errors.New("unsupported generated Codex path")
					}
					item["path"] = filepath.Join(actual, relative)
				}
			}
		}
	}
	return toml.Marshal(config)
}

func residentSkillFiles(root *os.Root, provider string) (map[string][]byte, error) {
	path := map[string]string{"codex": "codex-home/skills", "pi": "workdir/.pi/skills", "claude": "workdir/.claude/skills"}[provider]
	files := map[string][]byte{}
	if _, err := root.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return files, nil
	}
	err := fs.WalkDir(root.FS(), path, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		} // Admitted operator defaults are already in private HOME.
		info, err := root.Lstat(name)
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("unsupported generated skill artifact")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return errors.New("generated skill artifact has shared links")
		}
		raw, err := readBoundedLimit(root, name, nativeTaskBytes)
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(path, name)
		files[filepath.Join("skills", relative)] = raw
		return nil
	})
	return files, err
}

func makeNativeDirectories(root *os.Root, path string) error {
	if !fs.ValidPath(path) {
		return errors.New("invalid platform directory path")
	}
	parent := root
	var opened []*os.Root
	defer func() {
		for _, directory := range opened {
			directory.Close()
		}
	}()
	for _, part := range strings.Split(path, "/") {
		if err := parent.Mkdir(part, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err := parent.Lstat(part)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("platform directory is indirect")
		}
		child, err := parent.OpenRoot(part)
		if err != nil {
			return err
		}
		actual, err := child.Stat(".")
		if err != nil || !os.SameFile(info, actual) {
			child.Close()
			return errors.New("platform directory changed while opening")
		}
		opened = append(opened, child)
		parent = child
	}
	return nil
}

func writeNativeExclusive(root *os.Root, path string, raw []byte) error {
	parent, name := filepath.Split(path)
	if parent == "" {
		parent = "."
	} else {
		parent = strings.TrimSuffix(parent, "/")
	}
	directory, err := OpenNativeDirectory(root, parent)
	if err != nil {
		return err
	}
	defer directory.Close()
	file, err := directory.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(raw)
	return errors.Join(err, file.Sync(), file.Close())
}

func establishNativeProjections(root *os.Root, p *Prepared, input Preparation) error {
	owned := map[string]bool{}
	if input.Prior != nil && input.Prior.NativeMetadata == nil {
		manifest, err := decodeCleanup(input.TaskRoot, input.Prior.CleanupManifest)
		if err != nil {
			return err
		}
		for _, path := range manifest.Files {
			relative, err := filepath.Rel(input.TaskRoot, path)
			if err != nil {
				return err
			}
			owned[relative] = true
		}
	}
	// Refuse every required collision before removing any old platform file.
	for path, target := range NativeProjectionLinks(p.Provider) {
		info, err := root.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		actual, linkErr := root.Readlink(path)
		if linkErr == nil && actual == target {
			continue
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !owned[path] || !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
			if path == TaskResourcesRelative {
				delete(p.AllowedLinks, path)
				p.NativeMetadata.ProjectResources = nil
				continue
			}
			return errors.New("required native projection has an unowned collision")
		}
	}
	if input.Prior != nil && input.Prior.NativeMetadata == nil {
		// Only trusted generated project files are removed during writer-free setup.
		for path := range owned {
			if strings.HasPrefix(path, "workdir/.pi/skills/") || strings.HasPrefix(path, "workdir/.claude/skills/") {
				parent, name := filepath.Split(path)
				directory, err := OpenNativeDirectory(root, strings.TrimSuffix(parent, "/"))
				if err != nil {
					return err
				}
				info, err := directory.Lstat(name)
				if err == nil && info.Mode().IsRegular() {
					stat, ok := info.Sys().(*syscall.Stat_t)
					if !ok || stat.Nlink != 1 {
						directory.Close()
						return errors.New("old generated skill is shared")
					}
					err = directory.Remove(name)
				}
				directory.Close()
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
		}
	}
	for path, target := range NativeProjectionLinks(p.Provider) {
		parent, name := filepath.Split(path)
		if err := makeNativeDirectories(root, strings.TrimSuffix(parent, "/")); err != nil {
			if path == TaskResourcesRelative {
				delete(p.AllowedLinks, path)
				p.NativeMetadata.ProjectResources = nil
				continue
			}
			return err
		}
		directory, err := OpenNativeDirectory(root, strings.TrimSuffix(parent, "/"))
		if err != nil {
			return err
		}
		actual, linkErr := directory.Readlink(name)
		if linkErr == nil && actual == target {
			directory.Close()
			continue
		}
		info, statErr := directory.Lstat(name)
		if statErr == nil {
			if !owned[path] || !info.Mode().IsRegular() {
				directory.Close()
				if path == TaskResourcesRelative {
					delete(p.AllowedLinks, path)
					p.NativeMetadata.ProjectResources = nil
					continue
				}
				return errors.New("required native projection has an unowned collision")
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || stat.Nlink != 1 {
				directory.Close()
				return errors.New("old native projection is shared")
			}
			if err := directory.Remove(name); err != nil {
				directory.Close()
				return err
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			directory.Close()
			return statErr
		}
		err = directory.Symlink(target, name)
		directory.Close()
		if err != nil {
			if path == TaskResourcesRelative {
				delete(p.AllowedLinks, path)
				p.NativeMetadata.ProjectResources = nil
				continue
			}
			return err
		}
	}
	if p.Provider == "codex" {
		home := environmentValue(input.Environment, "HOME")
		for path, relative := range nativeHomeLinks {
			if err := makeNativeDirectories(root, filepath.Dir(path)); err != nil {
				return err
			}
			parent, name := filepath.Split(path)
			directory, err := OpenNativeDirectory(root, strings.TrimSuffix(parent, "/"))
			if err != nil {
				return err
			}
			target := filepath.Join(home, relative)
			actual, err := directory.Readlink(name)
			if errors.Is(err, os.ErrNotExist) {
				err = directory.Symlink(target, name)
			} else if err == nil && actual != target {
				err = errors.New("native HOME binding changed")
			}
			directory.Close()
			if err != nil {
				return err
			}
			p.AllowedLinks[path] = target
		}
	}
	return nil
}

func publishNativeArtifacts(root *os.Root, p Prepared, files map[string][]byte) error {
	relative, _ := filepath.Rel(p.TaskRoot, p.NativeMetadata.ArtifactRoot)
	parent, name := filepath.Split(relative)
	if err := makeNativeDirectories(root, strings.TrimSuffix(parent, "/")); err != nil {
		return err
	}
	directory, err := OpenNativeDirectory(root, strings.TrimSuffix(parent, "/"))
	if err != nil {
		return err
	}
	defer directory.Close()
	// Exclusive mkdir is the no-replace publication boundary supported by NFS.
	if err := directory.Mkdir(name, 0700); err != nil {
		return errors.New("native artifact generation already exists or is unavailable")
	}
	generation, err := OpenNativeDirectory(directory, name)
	if err != nil {
		return err
	}
	defer generation.Close()
	for _, artifact := range p.NativeMetadata.Artifacts {
		if err := makeNativeDirectories(generation, filepath.Dir(artifact.Path)); err != nil {
			return err
		}
		if err := writeNativeExclusive(generation, artifact.Path, files[artifact.Path]); err != nil {
			return err
		}
	}
	manifest, _ := json.Marshal(p.NativeMetadata.Artifacts)
	if err := writeNativeExclusive(generation, "published.json", manifest); err != nil {
		return err
	}
	file, err := generation.Open(".")
	if err != nil {
		return err
	}
	err = errors.Join(file.Sync(), file.Close())
	original, statErr := generation.Stat(".")
	current, currentErr := directory.Lstat(name)
	if err != nil || statErr != nil || currentErr != nil || !os.SameFile(original, current) {
		return errors.New("native artifact generation changed during publication")
	}
	published, err := OpenNativeDirectory(root, relative)
	if err != nil {
		return errors.New("native artifact namespace changed during publication")
	}
	actual, err := published.Stat(".")
	published.Close()
	if err != nil || !os.SameFile(original, actual) {
		return errors.New("native artifact publication changed its directory identity")
	}
	return nil
}
