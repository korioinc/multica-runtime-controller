package environment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
)

type Options struct {
	Root, CoreRoot         string
	Input                  Input
	ExpectedID, ScriptPath string
	LockPath               string
	InstallEnv             []string
	BaseEnv                []string
	Timeout                time.Duration
	Output                 io.Writer
}

func Prepare(ctx context.Context, opts Options) (Ref, error) {
	var zero Ref
	id, err := Identity(opts.Input)
	if err != nil {
		return zero, err
	}
	if id != opts.ExpectedID {
		return zero, errors.New("environment identity mismatch")
	}
	if opts.Root == "" {
		opts.Root = Root
	}
	if opts.CoreRoot == "" {
		opts.CoreRoot = core.Root
	}
	if err = realDirectory(opts.Root); err != nil {
		return zero, err
	}
	contract, err := core.Check(opts.CoreRoot, opts.Input.Platform)
	if err != nil {
		return zero, err
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 20 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	lockPath := opts.LockPath
	if lockPath == "" {
		lockPath = filepath.Join(opts.Root, ".prepare.lock")
	}
	lock, err := acquire(ctx, lockPath)
	if err != nil {
		return zero, err
	}
	defer release(lock)
	if _, err = os.Lstat(filepath.Join(opts.Root, ReadyName)); err == nil {
		ref, _, err := Check(opts.Root, opts.CoreRoot, opts.Input, nil)
		return ref, err
	} else if !os.IsNotExist(err) {
		return zero, err
	}
	script, err := os.ReadFile(opts.ScriptPath)
	if err != nil {
		return zero, err
	}
	if core.Digest(script) != opts.Input.ScriptSHA256 {
		return zero, errors.New("bootstrap script SHA-256 mismatch")
	}
	entries, err := os.ReadDir(opts.Root)
	if err != nil {
		return zero, err
	}
	for _, entry := range entries {
		if filepath.Join(opts.Root, entry.Name()) == lockPath {
			continue
		}
		if err = os.RemoveAll(filepath.Join(opts.Root, entry.Name())); err != nil {
			return zero, err
		}
	}
	tmp, err := os.MkdirTemp("", "multica-prepare-")
	if err != nil {
		return zero, err
	}
	defer os.RemoveAll(tmp)
	scriptPath := filepath.Join(tmp, "bootstrap.sh")
	if err = os.WriteFile(scriptPath, script, 0400); err != nil {
		return zero, err
	}
	inputs := opts.Input.Inputs
	if inputs == nil {
		inputs = map[string]string{}
	}
	b, err := json.Marshal(inputs)
	if err != nil {
		return zero, err
	}
	inputsPath := filepath.Join(tmp, "inputs.json")
	if err = os.WriteFile(inputsPath, b, 0400); err != nil {
		return zero, err
	}
	home := filepath.Join(tmp, "home")
	if err = os.Mkdir(home, 0700); err != nil {
		return zero, err
	}
	providersJSON, _ := json.Marshal(opts.Input.Providers)
	installEnv := safeInstallEnv(opts.InstallEnv)
	installEnv = append(installEnv, "HOME="+home, "TMPDIR="+tmp, "ENV_PROVIDERS="+string(providersJSON), "ENV_ROOT="+opts.Root, "ENV_PLATFORM="+opts.Input.Platform, "ENV_REVISION="+opts.Input.Revision, "ENV_INPUTS_FILE="+inputsPath, "ENV_MANIFEST_FILE="+filepath.Join(opts.Root, "environment.json"))
	if _, err = runTree(ctx, []string{"/bin/bash", scriptPath}, installEnv, opts.Root, opts.Output); err != nil {
		return zero, fmt.Errorf("bootstrap failed: %w", err)
	}
	manifest, manifestBytes, err := readManifest(opts.Root, opts.Input)
	if err != nil {
		return zero, err
	}
	// Validation receives a fresh HOME and public configuration only. Install
	// credentials cannot become provider-probe/task defaults or READY metadata.
	home = filepath.Join(tmp, "probe-home")
	if err = os.Mkdir(home, 0700); err != nil {
		return zero, err
	}
	if err = CopySeed(opts.Root, manifest, home); err != nil {
		return zero, err
	}
	baseEnv := opts.BaseEnv
	if baseEnv == nil {
		baseEnv = []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
	}
	runtimeEnv, err := Vars(manifest, baseEnv, Locations{Root: opts.Root, Home: home, TmpDir: tmp, Workspace: tmp})
	if err != nil {
		return zero, err
	}
	fingerprints := map[string]Fingerprint{}
	verified := map[string]verifiedExecutable{}
	for id, provider := range manifest.Providers {
		before, err := snapshotExecutable(opts.Root, provider.Entrypoint)
		if err != nil {
			return zero, err
		}
		probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		output, probeErr := runTree(probeCtx, []string{before.path, "--version"}, runtimeEnv, opts.Root, nil)
		cancel()
		if probeErr != nil {
			return zero, fmt.Errorf("provider %s version probe failed: %w", id, probeErr)
		}
		after, err := snapshotExecutable(opts.Root, provider.Entrypoint)
		if err != nil {
			return zero, err
		}
		if !before.equal(after) {
			return zero, fmt.Errorf("provider %s changed during its version probe", id)
		}
		verified[id] = before
		fingerprints[id] = Fingerprint{Entrypoint: provider.Entrypoint, SHA256: before.hash, VersionOutput: output}
	}
	for _, check := range manifest.Checks {
		argv := append([]string(nil), check.Argv...)
		argv[0], err = executable(opts.Root, argv[0])
		if err != nil {
			return zero, err
		}
		timeout := time.Duration(check.TimeoutSeconds) * time.Second
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		_, checkErr := runTree(checkCtx, argv, runtimeEnv, opts.Root, nil)
		cancel()
		if checkErr != nil {
			return zero, fmt.Errorf("environment check failed: %w", checkErr)
		}
	}
	// Generic checks may create files but may not replace the executable that
	// actually passed its version probe, even by retargeting an equal-byte symlink.
	for id, fp := range fingerprints {
		current, err := snapshotExecutable(opts.Root, fp.Entrypoint)
		if err != nil {
			return zero, err
		}
		if !verified[id].equal(current) {
			return zero, fmt.Errorf("provider %s changed after its version probe", id)
		}
	}
	// A check is permitted to create output; it may not replace the manifest being verified.
	current, err := os.ReadFile(filepath.Join(opts.Root, "environment.json"))
	if err != nil {
		return zero, err
	}
	if core.Digest(current) != core.Digest(manifestBytes) {
		return zero, errors.New("manifest changed during validation")
	}
	digest, err := contentDigest(opts.Root)
	if err != nil {
		return zero, err
	}
	ref := Ref{SchemaVersion: 1, EnvironmentID: id, ContentDigest: digest, ManifestDigest: core.Digest(manifestBytes), Core: contract, CoreImage: opts.Input.CoreImage, EnvironmentImage: opts.Input.EnvironmentImage, Platform: opts.Input.Platform, Providers: fingerprints}
	if err = ctx.Err(); err != nil {
		return zero, err
	}
	if err = atomicJSON(filepath.Join(opts.Root, ReadyName), ref); err != nil {
		return zero, err
	}
	return ref, nil
}

type verifiedExecutable struct {
	path, hash, link string
	file, entry      os.FileInfo
}

func snapshotExecutable(root, relative string) (verifiedExecutable, error) {
	var v verifiedExecutable
	path, err := executable(root, relative)
	if err != nil {
		return v, err
	}
	v.path = path
	v.file, err = os.Stat(path)
	if err != nil {
		return v, err
	}
	v.entry, err = os.Lstat(filepath.Join(root, relative))
	if err != nil {
		return v, err
	}
	if v.entry.Mode()&os.ModeSymlink != 0 {
		v.link, err = os.Readlink(filepath.Join(root, relative))
		if err != nil {
			return v, err
		}
	}
	v.hash, err = core.HashFile(path)
	return v, err
}
func (v verifiedExecutable) equal(other verifiedExecutable) bool {
	return v.path == other.path && v.hash == other.hash && v.link == other.link && v.file.Mode() == other.file.Mode() && v.entry.Mode() == other.entry.Mode() && os.SameFile(v.file, other.file) && os.SameFile(v.entry, other.entry)
}

var ErrIntegrity = errors.New("environment integrity validation failed")

func Check(root, coreRoot string, input Input, expected *Ref) (ref Ref, manifest Manifest, resultErr error) {
	defer func() {
		if resultErr != nil {
			resultErr = fmt.Errorf("%w: %w", ErrIntegrity, resultErr)
		}
	}()
	id, err := Identity(input)
	if err != nil {
		return ref, manifest, err
	}
	contract, err := core.Check(coreRoot, input.Platform)
	if err != nil {
		return ref, manifest, err
	}
	path := filepath.Join(root, ReadyName)
	st, err := os.Lstat(path)
	if err != nil {
		return ref, manifest, err
	}
	if !st.Mode().IsRegular() {
		return ref, manifest, errors.New("READY is not a regular file")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ref, manifest, err
	}
	if err = json.Unmarshal(b, &ref); err != nil {
		return ref, manifest, err
	}
	if err = ref.Validate(); err != nil {
		return ref, manifest, err
	}
	a, _ := json.Marshal(contract)
	b, _ = json.Marshal(ref.Core)
	if string(a) != string(b) {
		return ref, manifest, fmt.Errorf("%w: READY belongs to another core build", core.ErrCompatibility)
	}
	if ref.EnvironmentID != id || ref.CoreImage != input.CoreImage || ref.EnvironmentImage != input.EnvironmentImage || ref.Platform != input.Platform {
		return ref, manifest, errors.New("READY core/image/platform/identity mismatch")
	}
	if expected != nil && !ref.Equal(*expected) {
		return ref, manifest, errors.New("mounted READY differs from pinned task environment")
	}
	var manifestBytes []byte
	manifest, manifestBytes, err = readManifest(root, input)
	if err != nil {
		return ref, manifest, err
	}
	if core.Digest(manifestBytes) != ref.ManifestDigest {
		return ref, manifest, errors.New("environment manifest digest mismatch")
	}
	if len(ref.Providers) != len(manifest.Providers) {
		return ref, manifest, errors.New("READY provider set mismatch")
	}
	for id, p := range manifest.Providers {
		fp, ok := ref.Providers[id]
		if !ok || fp.Entrypoint != p.Entrypoint {
			return ref, manifest, errors.New("READY entrypoint mismatch")
		}
		path, err := executable(root, p.Entrypoint)
		if err != nil {
			return ref, manifest, err
		}
		hash, err := core.HashFile(path)
		if err != nil {
			return ref, manifest, err
		}
		if hash != fp.SHA256 {
			return ref, manifest, errors.New("provider fingerprint mismatch")
		}
	}
	digest, err := contentDigest(root)
	if err != nil {
		return ref, manifest, err
	}
	if digest != ref.ContentDigest {
		return ref, manifest, errors.New("environment content digest mismatch")
	}
	return ref, manifest, nil
}

// CheckWritable validates only the explicitly granted native write locations.
func CheckWritable(loc Locations) error {
	for _, path := range []string{loc.Home, loc.TmpDir, loc.Workspace} {
		if err := writableDirectory(path); err != nil {
			return fmt.Errorf("writable environment location %s: %w", path, err)
		}
	}
	return nil
}
