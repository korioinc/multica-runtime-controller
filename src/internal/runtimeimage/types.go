// Package runtimeimage verifies the installed image and binds task execution to
// its immutable contents. It never installs tools or resolves registry tags.
package runtimeimage

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
)

const (
	Root            = "/opt/multica/runtime"
	DescriptorPath  = Root + "/image.json"
	AdapterContract = "multica-v0.4.43-helper-agent-v1"
)

type Executable struct {
	Path    string `json:"path"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
}

type Daemon struct {
	Executable
	AdapterContract string `json:"adapterContract"`
}

type Descriptor struct {
	Kind         string                `json:"kind"`
	ImageBuildID string                `json:"imageBuildID"`
	Platform     string                `json:"platform"`
	Controller   core.Contract         `json:"controller"`
	Daemon       Daemon                `json:"daemon"`
	Providers    map[string]Executable `json:"providers"`
	BinDirs      []string              `json:"binDirs"`
	Env          map[string]string     `json:"env"`
}

// Ref contains only stable execution identity. Kubernetes resource identity is
// deliberately separate so an equivalent configuration preserves native sessions.
type Ref struct {
	Image               string                `json:"image"`
	Platform            string                `json:"platform"`
	ImageBuildID        string                `json:"imageBuildID"`
	DescriptorDigest    string                `json:"descriptorDigest"`
	Controller          core.Contract         `json:"controller"`
	Daemon              Daemon                `json:"daemon"`
	Providers           map[string]Executable `json:"providers"`
	ConfigurationDigest string                `json:"configurationDigest"`
}

func canonicalUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value && id != uuid.Nil
}

// Repository components use the distribution reference grammar: one dot,
// one or two underscores, or repeated hyphens between alphanumeric runs.
const repositoryComponent = `[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*`
const registryHost = `(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)*|\[[a-fA-F0-9:]+\])(?::[0-9]+)?`

var repositoryDigest = regexp.MustCompile(`^(?:` + registryHost + `/)?` + repositoryComponent + `(?:/` + repositoryComponent + `)*@sha256:[a-f0-9]{64}$`)

// NormalizeImageID accepts only a pullable repository digest actually reported
// by the runtime. An opaque config/container digest is never made pullable by
// appending a repository or resolving the current tag.
func NormalizeImageID(value string) (string, error) {
	value = strings.TrimPrefix(value, "docker-pullable://")
	if !repositoryDigest.MatchString(value) {
		return "", diagnostics.Wrap("runtime_image_id_unverifiable", errors.New("runtime imageID is not a pullable repository digest"))
	}
	return value, nil
}

func ImmutablePath(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || strings.ContainsAny(path, "\x00\n\r") {
		return false
	}
	for _, root := range []string{"/home", "/workspace", "/tmp", "/run", "/var", "/proc", "/sys", "/dev", "/mnt", "/media"} {
		if path == root || strings.HasPrefix(path, root+"/") {
			return false
		}
	}
	return true
}

func (e Executable) validate() error {
	if !ImmutablePath(e.Path) || strings.TrimSpace(e.Version) != e.Version || e.Version == "" || strings.ContainsAny(e.Version, "\x00\n\r") || !core.ValidSHA(e.SHA256) {
		return errors.New("invalid installed executable descriptor")
	}
	if e.Path == core.Root || strings.HasPrefix(e.Path, core.Root+"/") {
		return errors.New("provider or daemon points at controller code")
	}
	return nil
}

func validateComponents(controller core.Contract, daemon Daemon, installed map[string]Executable, platform string) error {
	if err := controller.Validate(platform); err != nil {
		return err
	}
	if err := daemon.Executable.validate(); err != nil {
		return err
	}
	if daemon.AdapterContract != AdapterContract {
		return errors.New("unsupported official daemon adapter contract")
	}
	if len(installed) == 0 {
		return errors.New("runtime image must enable a supported provider")
	}
	for id, provider := range installed {
		if strings.TrimSpace(id) != id || id == "" || strings.ContainsAny(id, "\x00\r\n") {
			return errors.New("invalid runtime inventory identifier")
		}
		if err := provider.validate(); err != nil {
			return fmt.Errorf("provider %s: %w", id, err)
		}
	}
	return nil
}

func (d Descriptor) Validate(platform string) error {
	if d.Kind != "multica-runtime-image" || !canonicalUUID(d.ImageBuildID) || !core.SupportedPlatform(platform) || d.Platform != platform {
		return errors.New("runtime image descriptor build/platform mismatch")
	}
	if err := validateComponents(d.Controller, d.Daemon, d.Providers, platform); err != nil {
		return err
	}
	if len(d.BinDirs) == 0 {
		return errors.New("runtime image PATH is empty")
	}
	seen := map[string]bool{}
	for _, path := range d.BinDirs {
		if !ImmutablePath(path) || seen[path] || path == core.Root || strings.HasPrefix(path, core.Root+"/") {
			return errors.New("invalid runtime image PATH directory")
		}
		seen[path] = true
	}
	for key, value := range d.Env {
		if !envName.MatchString(key) || Reserved(key) {
			return fmt.Errorf("invalid runtime image variable %s", key)
		}
		if _, err := expand(value, Locations{}); err != nil {
			return err
		}
	}
	return nil
}

func (r Ref) Validate() error {
	image, err := NormalizeImageID(r.Image)
	if err != nil || image != r.Image || !canonicalUUID(r.ImageBuildID) || !core.ValidSHA(r.DescriptorDigest) || !core.ValidSHA(r.ConfigurationDigest) {
		return errors.New("invalid runtime reference; bound image/configuration required")
	}
	return validateComponents(r.Controller, r.Daemon, r.Providers, r.Platform)
}

func (r Ref) Equal(other Ref) bool {
	a, _ := json.Marshal(r)
	b, _ := json.Marshal(other)
	return string(a) == string(b)
}

func (d Descriptor) Reference(image, descriptorDigest, configurationDigest string) (Ref, error) {
	r := Ref{Image: image, Platform: d.Platform, ImageBuildID: d.ImageBuildID, DescriptorDigest: descriptorDigest, Controller: d.Controller, Daemon: d.Daemon, Providers: d.Providers, ConfigurationDigest: configurationDigest}
	return r, r.Validate()
}
