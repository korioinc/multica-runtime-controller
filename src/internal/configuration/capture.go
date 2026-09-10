package configuration

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"golang.org/x/sys/unix"
)

func decode(raw []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("trailing configuration JSON")
	}
	return nil
}

func ValidateCopies(copies []Copy, inputRoot string) error {
	if !filepath.IsAbs(inputRoot) || filepath.Clean(inputRoot) != inputRoot {
		return errors.New("canonical configuration input root required")
	}
	for i, c := range copies {
		base := filepath.Join(inputRoot, c.SourceGroup)
		if !GroupName(c.SourceGroup) || filepath.Clean(c.Source) != c.Source || c.Source != base && !strings.HasPrefix(c.Source, base+"/") || !HomePath(c.Target, true) {
			return errors.New("configuration copy requires a source group, confined source and native target")
		}
		for _, prior := range copies[:i] {
			if c.Target == prior.Target || strings.HasPrefix(c.Target, prior.Target+"/") || strings.HasPrefix(prior.Target, c.Target+"/") {
				return errors.New("configuration copy targets overlap")
			}
			if c.SourceGroup == prior.SourceGroup && (c.Source == prior.Source || strings.HasPrefix(c.Source, prior.Source+"/") || strings.HasPrefix(prior.Source, c.Source+"/")) {
				return errors.New("configuration copy sources overlap")
			}
		}
	}
	return nil
}

// projection chooses kubelet's payload once per logical source volume. A
// subsequent mutable ConfigMap update cannot change the selected tree mid-walk.
func projection(source string) (string, error) {
	info, err := os.Lstat(source)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("configuration source group is not a real directory")
	}
	base, err := filepath.EvalSymlinks(source)
	if err != nil {
		return "", err
	}
	data := filepath.Join(base, "..data")
	info, err = os.Lstat(data)
	if os.IsNotExist(err) {
		return base, nil
	}
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return "", errors.New("configuration projection has invalid data link")
	}
	snapshot, err := filepath.EvalSymlinks(data)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(base, snapshot)
	if err != nil || rel == "." || !filepath.IsLocal(rel) {
		return "", errors.New("configuration projection escapes its source group")
	}
	info, err = os.Stat(snapshot)
	if err != nil || !info.IsDir() {
		return "", errors.New("configuration projection payload is not a directory")
	}
	return snapshot, nil
}

func readFile(path string) ([]byte, fs.FileMode, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !info.Mode().IsRegular() || info.Size() > MaxGroupBytes {
		return nil, 0, errors.New("configuration payload is not a bounded regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(f, MaxGroupBytes+1))
	if err != nil {
		return nil, 0, err
	}
	if len(raw) > MaxGroupBytes {
		return nil, 0, errors.New("configuration file exceeds ConfigMap payload limit")
	}
	return raw, info.Mode(), nil
}

func collect(g *Group, source, target string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() && !info.IsDir() {
		return errors.New("configuration input contains a link or special file")
	}
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		dest := filepath.Join(target, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !HomePath(dest, info.IsDir()) {
			return errors.New("configuration tree shadows native session or daemon authority")
		}
		name := strings.TrimPrefix(dest, Home+"/")
		if info.IsDir() {
			g.Directories = append(g.Directories, name)
			return nil
		}
		if !info.Mode().IsRegular() {
			return errors.New("configuration tree contains a link or special file")
		}
		raw, mode, err := readFile(path)
		if err != nil {
			return err
		}
		g.Files = append(g.Files, File{Target: name, Mode: uint32(0600 | mode.Perm()&0100), SHA256: core.Digest(raw), Content: raw})
		return nil
	})
}

func Capture(copies []Copy, inputRoot string) (Bundle, error) {
	b := Bundle{SchemaVersion: 1, Groups: []Group{}}
	if err := ValidateCopies(copies, inputRoot); err != nil {
		return b, diagnostics.Wrap("configuration_mapping_invalid", err)
	}
	groups := map[string]*Group{}
	roots := map[string]string{}
	for _, copy := range copies {
		if _, ok := groups[copy.SourceGroup]; !ok {
			root, err := projection(filepath.Join(inputRoot, copy.SourceGroup))
			if err != nil {
				return b, diagnostics.ForGroup("configuration_source_invalid", copy.SourceGroup, err)
			}
			roots[copy.SourceGroup] = root
			groups[copy.SourceGroup] = &Group{Name: copy.SourceGroup, Directories: []string{}, Files: []File{}}
		}
		rel, err := filepath.Rel(filepath.Join(inputRoot, copy.SourceGroup), copy.Source)
		if err != nil {
			return b, err
		}
		source := filepath.Join(roots[copy.SourceGroup], rel)
		resolved, err := filepath.EvalSymlinks(source)
		if err != nil {
			return b, err
		}
		if resolved != source {
			return b, errors.New("configuration input contains symlink parents")
		}
		if err = collect(groups[copy.SourceGroup], source, copy.Target); err != nil {
			return b, diagnostics.ForGroup("configuration_payload_invalid", copy.SourceGroup, err)
		}
	}
	for _, g := range groups {
		normalize(g)
		b.Groups = append(b.Groups, *g)
	}
	slices.SortFunc(b.Groups, func(a, b Group) int { return strings.Compare(a.Name, b.Name) })
	b.Digest = Digest(b.Groups)
	return b, b.Validate()
}

// Commit atomically publishes the entire manifest and file payload together.
// A retry validates and reuses this committed capture without reopening sources.
func Commit(run string, b Bundle) (Bundle, error) {
	if err := b.Validate(); err != nil {
		return b, err
	}
	path := filepath.Join(run, BundleName)
	if _, err := os.Lstat(path); err == nil {
		return Read(run)
	} else if !os.IsNotExist(err) {
		return b, err
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return b, err
	}
	f, err := os.CreateTemp(run, ".configuration-")
	if err != nil {
		return b, err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return b, err
	}
	if closeErr != nil {
		return b, closeErr
	}
	if err = os.Link(f.Name(), path); err != nil && !os.IsExist(err) {
		return b, err
	}
	dir, err := os.Open(run)
	if err != nil {
		return b, err
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return b, err
	}
	return Read(run)
}

func CaptureOrRead(run string, copies []Copy, inputRoot string) (Bundle, error) {
	if _, err := os.Lstat(filepath.Join(run, BundleName)); err == nil {
		return Read(run)
	} else if !os.IsNotExist(err) {
		return Bundle{}, err
	}
	b, err := Capture(copies, inputRoot)
	if err != nil {
		return b, err
	}
	return Commit(run, b)
}
