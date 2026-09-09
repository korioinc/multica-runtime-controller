// Package configuration owns the immutable, per-Pod capture of operator files.
// Configuration contents stay outside task request Secrets and runtime identity
// uses content, never the names or UIDs of derived Kubernetes objects.
package configuration

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
)

const (
	Home          = "/home/multica/agents"
	InputRoot     = "/opt/multica/config-input"
	BundleName    = "configuration.json"
	MaxGroupBytes = 1 << 20
)

var groupName = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$`)

type Copy struct {
	SourceGroup string `json:"sourceGroup"`
	Source      string `json:"source"`
	Target      string `json:"target"`
}

type File struct {
	Target  string `json:"target"`
	Mode    uint32 `json:"mode"`
	SHA256  string `json:"sha256"`
	Content []byte `json:"content"`
}

type Group struct {
	Name        string   `json:"name"`
	Directories []string `json:"directories"`
	Files       []File   `json:"files"`
}

type Bundle struct {
	SchemaVersion int     `json:"schemaVersion"`
	Groups        []Group `json:"groups"`
	Digest        string  `json:"digest"`
}

func GroupName(value string) bool { return len(value) <= 63 && groupName.MatchString(value) }

func HomePath(path string, directory bool) bool {
	if filepath.Clean(path) != path || !strings.HasPrefix(path, Home+"/") || strings.ContainsAny(path, "\x00\n\r") {
		return false
	}
	for _, protected := range []string{Home + "/.multica/pi-sessions", Home + "/.multica/config.json", Home + "/.pi/agent/sessions"} {
		if path == protected || strings.HasPrefix(path, protected+"/") || !directory && strings.HasPrefix(protected, path+"/") {
			return false
		}
	}
	return true
}

func relativeTarget(value string, directory bool) bool {
	return filepath.IsLocal(value) && filepath.Clean(value) == value && value != "." && HomePath(Home+"/"+value, directory)
}

func (g Group) Validate() error {
	if !GroupName(g.Name) || g.Files == nil || g.Directories == nil {
		return errors.New("invalid configuration source group")
	}
	files := map[string]bool{}
	directories := map[string]bool{}
	size := 0
	for i, path := range g.Directories {
		if !relativeTarget(path, true) || i > 0 && g.Directories[i-1] >= path {
			return errors.New("invalid configuration directories")
		}
		directories[path] = true
	}
	for i, file := range g.Files {
		if !relativeTarget(file.Target, false) || i > 0 && g.Files[i-1].Target >= file.Target || file.Mode != 0600 && file.Mode != 0700 || file.SHA256 != core.Digest(file.Content) {
			return errors.New("invalid configuration file integrity or target")
		}
		files[file.Target] = true
		size += len(file.Content)
	}
	if size > MaxGroupBytes {
		return diagnostics.ForGroup("configuration_payload_too_large", g.Name, errors.New("configuration source exceeds supported payload size"))
	}
	for path := range files {
		if directories[path] {
			return errors.New("configuration target is both file and directory")
		}
		for parent := filepath.Dir(path); parent != "."; parent = filepath.Dir(parent) {
			if files[parent] {
				return errors.New("configuration file shadows a parent")
			}
		}
	}
	for path := range directories {
		for parent := filepath.Dir(path); parent != "."; parent = filepath.Dir(parent) {
			if files[parent] {
				return errors.New("configuration file shadows a directory parent")
			}
		}
	}
	return nil
}

// GroupDigest includes logical mapping and executable policy, with content
// hashes in place of contents. Physical mount paths and object identity are absent.
func GroupDigest(g Group) string {
	type item struct {
		Target string `json:"target"`
		Mode   uint32 `json:"mode"`
		SHA256 string `json:"sha256"`
	}
	items := make([]item, 0, len(g.Files))
	for _, f := range g.Files {
		items = append(items, item{f.Target, f.Mode, f.SHA256})
	}
	raw, _ := json.Marshal(struct {
		Name        string   `json:"name"`
		Directories []string `json:"directories"`
		Files       []item   `json:"files"`
	}{g.Name, g.Directories, items})
	return core.Digest(raw)
}

func Digest(groups []Group) string {
	type record struct {
		Name   string `json:"name"`
		Digest string `json:"digest"`
	}
	items := make([]record, 0, len(groups))
	for _, g := range groups {
		items = append(items, record{g.Name, GroupDigest(g)})
	}
	raw, _ := json.Marshal(items)
	return core.Digest(raw)
}

func (b Bundle) Validate() error {
	if b.SchemaVersion != 1 || b.Groups == nil || !core.ValidSHA(b.Digest) {
		return errors.New("invalid configuration bundle")
	}
	targets := map[string]string{}
	files := map[string]bool{}
	for i, g := range b.Groups {
		if i > 0 && b.Groups[i-1].Name >= g.Name {
			return errors.New("configuration source groups are not unique/canonical")
		}
		if err := g.Validate(); err != nil {
			return err
		}
		for _, path := range g.Directories {
			if prior := targets[path]; prior != "" && prior != g.Name {
				return errors.New("configuration source groups overlap")
			}
			targets[path] = g.Name
		}
		for _, f := range g.Files {
			if targets[f.Target] != "" {
				return errors.New("configuration source groups overlap")
			}
			targets[f.Target] = g.Name
			files[f.Target] = true
		}
	}
	for path := range targets {
		for parent := filepath.Dir(path); parent != "."; parent = filepath.Dir(parent) {
			if files[parent] {
				return errors.New("configuration file shadows another target")
			}
		}
	}
	if Digest(b.Groups) != b.Digest {
		return errors.New("configuration bundle digest mismatch")
	}
	return nil
}

func normalize(g *Group) {
	slices.Sort(g.Directories)
	g.Directories = slices.Compact(g.Directories)
	slices.SortFunc(g.Files, func(a, b File) int { return strings.Compare(a.Target, b.Target) })
}

func Read(run string) (b Bundle, resultErr error) {
	defer func() { resultErr = diagnostics.Wrap("configuration_bundle_invalid", resultErr) }()
	path := filepath.Join(run, BundleName)
	info, err := os.Lstat(path)
	if err != nil {
		return b, err
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<20 {
		return b, errors.New("invalid committed configuration bundle file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return b, err
	}
	if err = decode(raw, &b); err != nil {
		return b, err
	}
	return b, b.Validate()
}
