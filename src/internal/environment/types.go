// Package environment owns preparation and verified consumption of immutable tool generations.
package environment

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/core"
)

const Root = "/opt/multica/environment"
const ReadyName = "READY"

type Input struct {
	SchemaVersion    int               `json:"schemaVersion"`
	CoreImage        string            `json:"coreImage"`
	EnvironmentImage string            `json:"environmentImage"`
	Platform         string            `json:"platform"`
	ScriptSHA256     string            `json:"scriptSHA256"`
	Revision         string            `json:"revision"`
	Providers        []string          `json:"providers"`
	Inputs           map[string]string `json:"inputs"`
}

type Provider struct {
	Entrypoint string `json:"entrypoint"`
	Version    string `json:"version"`
}
type Probe struct {
	Argv           []string `json:"argv"`
	TimeoutSeconds int      `json:"timeoutSeconds"`
}
type Manifest struct {
	SchemaVersion int                 `json:"schemaVersion"`
	Providers     map[string]Provider `json:"providers"`
	BinDirs       []string            `json:"binDirs"`
	Env           map[string]string   `json:"env"`
	HomeSeed      string              `json:"homeSeed,omitempty"`
	Checks        []Probe             `json:"checks,omitempty"`
}
type Fingerprint struct {
	Entrypoint    string `json:"entrypoint"`
	SHA256        string `json:"sha256"`
	VersionOutput string `json:"versionOutput"`
}
type Ref struct {
	SchemaVersion    int                    `json:"schemaVersion"`
	EnvironmentID    string                 `json:"environmentID"`
	ContentDigest    string                 `json:"contentDigest"`
	ManifestDigest   string                 `json:"manifestDigest"`
	Core             core.Contract          `json:"core"`
	CoreImage        string                 `json:"coreImage"`
	EnvironmentImage string                 `json:"environmentImage"`
	Platform         string                 `json:"platform"`
	Providers        map[string]Fingerprint `json:"providers"`
}

func Alias(id string) string {
	if id == "antigravity" {
		return "agy"
	}
	return id
}
func SupportedProvider(id string) bool {
	switch id {
	case "pi", "codex", "copilot", "antigravity":
		return true
	}
	return false
}
func Canonical(input Input) ([]byte, error) {
	if input.SchemaVersion != 1 || !core.PinnedImage(input.CoreImage) || !core.PinnedImage(input.EnvironmentImage) || !core.SupportedPlatform(input.Platform) || !core.ValidSHA(input.ScriptSHA256) {
		return nil, errors.New("invalid environment identity contract/image/platform/script hash")
	}
	set := map[string]bool{}
	for _, p := range input.Providers {
		if !SupportedProvider(p) {
			return nil, fmt.Errorf("unsupported provider %q", p)
		}
		set[p] = true
	}
	if len(set) == 0 {
		return nil, errors.New("environment must enable a provider")
	}
	providers := make([]string, 0, len(set))
	for p := range set {
		providers = append(providers, p)
	}
	sort.Strings(providers)
	inputs := input.Inputs
	if inputs == nil {
		inputs = map[string]string{}
	}
	// A map is deliberate: encoding/json's sorted keys and HTML escaping match Helm toJson.
	return json.Marshal(map[string]any{"schemaVersion": 1, "coreImage": input.CoreImage, "environmentImage": input.EnvironmentImage, "platform": input.Platform, "scriptSHA256": input.ScriptSHA256, "revision": input.Revision, "providers": providers, "inputs": inputs})
}
func Identity(input Input) (string, error) {
	b, err := Canonical(input)
	if err != nil {
		return "", err
	}
	return core.Digest(b), nil
}
func ReadInput(path string) (Input, error) {
	var input Input
	b, err := os.ReadFile(path)
	if err != nil {
		return input, err
	}
	err = json.Unmarshal(b, &input)
	if err != nil {
		return input, err
	}
	_, err = Identity(input)
	return input, err
}
func (r Ref) Equal(other Ref) bool {
	a, _ := json.Marshal(r)
	b, _ := json.Marshal(other)
	return string(a) == string(b)
}
func (r Ref) Validate() error {
	if r.SchemaVersion != 1 || !core.ValidSHA(r.EnvironmentID) || !core.ValidSHA(r.ContentDigest) || !core.ValidSHA(r.ManifestDigest) || !core.PinnedImage(r.CoreImage) || !core.PinnedImage(r.EnvironmentImage) {
		return errors.New("invalid environment reference")
	}
	if err := r.Core.Validate(r.Platform); err != nil {
		return err
	}
	if len(r.Providers) == 0 {
		return errors.New("empty environment reference providers")
	}
	for id, p := range r.Providers {
		if !SupportedProvider(id) || !core.ValidSHA(p.SHA256) || p.Entrypoint == "" {
			return errors.New("invalid provider fingerprint")
		}
	}
	return nil
}
func Reserved(name string) bool {
	name = strings.ToUpper(name)
	return name == "PATH" || name == "HOME" || name == "TMPDIR" || name == "PWD" || name == "OLDPWD" || name == "SHELL" || name == "ENV" || name == "BASH_ENV" || name == "LD_PRELOAD" || strings.HasPrefix(name, "MULTICA_") || strings.HasPrefix(name, "KUBERNETES_") || strings.HasPrefix(name, "ENV_") || strings.HasPrefix(name, "TASK_")
}
