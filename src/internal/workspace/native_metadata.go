package workspace

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/korioinc/multica-runtime-controller/internal/core"
)

const (
	RuntimeArtifactsDir    = ".multica-runtime-artifacts"
	NativeMetadataRoot     = "/run/multica/native-metadata"
	NativeMetadataView     = NativeMetadataRoot + "/current"
	NativeTaskMarker       = "/run/multica/native-task-marker.json"
	TaskMarkerRelative     = "workdir/.multica/daemon_task_context.json"
	TaskResourcesRelative  = "workdir/.multica/project/resources.json"
	PiServiceTierRelative  = "workdir/.pi/extensions/pi-openai-service-tier.json"
	MaxNativeArtifactBytes = nativeTaskBytes
)

type NativeArtifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// NativeMetadata stays in the private journal and assignment. Only Artifacts
// refer to exported files; provider configuration and resource bytes stay private.
type NativeMetadata struct {
	WorkerSessionID  string           `json:"workerSessionID"`
	TurnSequence     uint64           `json:"turnSequence"`
	ArtifactRoot     string           `json:"artifactRoot"`
	Artifacts        []NativeArtifact `json:"artifacts"`
	CodexConfig      []byte           `json:"codexConfig,omitempty"`
	ClaudeSettings   json.RawMessage  `json:"claudeSettings,omitempty"`
	ServiceTier      json.RawMessage  `json:"serviceTier,omitempty"`
	TaskMarker       json.RawMessage  `json:"taskMarker"`
	ProjectResources json.RawMessage  `json:"projectResources,omitempty"`
}

func (m NativeMetadata) Digest() string {
	raw, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return core.Digest(raw)
}

func (p Prepared) OmitNativeResources() Prepared {
	if p.NativeMetadata != nil {
		metadata := *p.NativeMetadata
		metadata.ProjectResources = nil
		p.NativeMetadata = &metadata
		p.Digest = preparedDigest(p)
	}
	return p
}

func NativeArtifactRoot(taskRoot, sessionID string, turn uint64) string {
	return filepath.Join(taskRoot, RuntimeArtifactsDir, sessionID, strconv.FormatUint(turn, 10))
}

func (m NativeMetadata) Validate(provider, taskRoot string) error {
	if !SupportedProvider(provider) || !canonicalUUID(m.WorkerSessionID) || m.TurnSequence == 0 ||
		m.ArtifactRoot != NativeArtifactRoot(taskRoot, m.WorkerSessionID, m.TurnSequence) || m.Artifacts == nil || !json.Valid(m.TaskMarker) {
		return errors.New("invalid native metadata identity")
	}
	var marker struct{ ManagedBy, AgentID string }
	var fields map[string]json.RawMessage
	if json.Unmarshal(m.TaskMarker, &fields) != nil || json.Unmarshal(fields["managed_by"], &marker.ManagedBy) != nil ||
		json.Unmarshal(fields["agent_id"], &marker.AgentID) != nil || marker.ManagedBy != "multica-daemon-task" || !canonicalUUID(marker.AgentID) {
		return errors.New("invalid native task guard")
	}
	for _, raw := range []json.RawMessage{m.ClaudeSettings, m.ServiceTier, m.ProjectResources} {
		if len(raw) != 0 && !json.Valid(raw) {
			return errors.New("invalid private native JSON")
		}
	}
	if provider == "codex" && len(m.CodexConfig) == 0 || provider != "codex" && len(m.CodexConfig) != 0 ||
		provider != "claude" && len(m.ClaudeSettings) != 0 || provider != "pi" && len(m.ServiceTier) != 0 {
		return errors.New("private native configuration belongs to a different provider")
	}
	var size int64
	for i, file := range m.Artifacts {
		if !fs.ValidPath(file.Path) || strings.Contains(file.Path, "\\") || !strings.HasPrefix(file.Path, "skills/") ||
			!core.ValidSHA(file.SHA256) || file.Size < 0 || file.Size > nativeTaskBytes || i > 0 && m.Artifacts[i-1].Path >= file.Path {
			return errors.New("invalid native artifact manifest")
		}
		size += file.Size
	}
	raw, err := json.Marshal(m)
	if err != nil || len(raw) > MaxAssignmentBytes || size > nativeTaskBytes {
		return errors.New("native metadata exceeds assignment or artifact budget")
	}
	return nil
}

func NativeProjectionLinks(provider string) map[string]string {
	links := map[string]string{TaskMarkerRelative: NativeTaskMarker, TaskResourcesRelative: NativeMetadataView + "/resources.json"}
	switch provider {
	case "codex":
		links["codex-home/config.toml"] = NativeMetadataView + "/codex/config.toml"
		links["codex-home/skills"] = NativeMetadataView + "/codex/skills"
	case "pi":
		links[PiServiceTierRelative] = NativeMetadataView + "/pi-openai-service-tier.json"
	}
	return links
}

// OpenNativeDirectory pins every directory identity and refuses links. All
// following file operations remain confined to the returned descriptor.
func OpenNativeDirectory(root *os.Root, relative string) (*os.Root, error) {
	if !fs.ValidPath(relative) {
		return nil, errors.New("invalid native directory path")
	}
	parent := root
	var opened []*os.Root
	defer func() {
		for _, child := range opened {
			_ = child.Close()
		}
	}()
	for _, part := range strings.Split(relative, "/") {
		info, err := parent.Lstat(part)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("native directory is indirect or unavailable")
		}
		child, err := parent.OpenRoot(part)
		if err != nil {
			return nil, err
		}
		actual, err := child.Stat(".")
		if err != nil || !os.SameFile(info, actual) {
			child.Close()
			return nil, errors.New("native directory changed while opening")
		}
		opened = append(opened, child)
		parent = child
	}
	result := opened[len(opened)-1]
	opened = opened[:len(opened)-1]
	return result, nil
}

// ReadNativeArtifact hashes the bytes read from the same non-following file
// descriptor. Callers copy these bytes, never reopen the validated pathname.
func ReadNativeArtifact(root *os.Root, artifact NativeArtifact) ([]byte, error) {
	parent, name := filepath.Split(artifact.Path)
	parent = strings.TrimSuffix(parent, "/")
	directory, err := OpenNativeDirectory(root, parent)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	file, err := directory.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("native artifact is not a regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || info.Size() != artifact.Size {
		return nil, errors.New("native artifact has shared links or changed size")
	}
	raw, err := io.ReadAll(io.LimitReader(file, artifact.Size+1))
	if err != nil || int64(len(raw)) != artifact.Size || core.Digest(raw) != artifact.SHA256 {
		return nil, errors.New("copied native artifact differs from its manifest")
	}
	return raw, nil
}

func verifyNativeProjectionBindings(root *os.Root, p Prepared) error {
	for path, target := range p.AllowedLinks {
		parent, name := filepath.Split(path)
		directory, err := OpenNativeDirectory(root, strings.TrimSuffix(parent, "/"))
		if err != nil {
			return err
		}
		actual, err := directory.Readlink(name)
		directory.Close()
		if err != nil || actual != target {
			return errors.New("native projection changed its binding")
		}
	}
	return nil
}
