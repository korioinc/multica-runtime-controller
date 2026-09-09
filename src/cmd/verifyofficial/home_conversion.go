package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	configurationbundle "github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type homeConversion struct {
	TaskID                 string            `json:"taskID"`
	AttemptID              string            `json:"attemptID"`
	WorkerSubPath          string            `json:"workerSubPath"`
	ConfigurationDigest    string            `json:"configurationDigest"`
	HomeDigest             string            `json:"homeDigest"`
	NativeEvidence         string            `json:"nativeEvidence"`
	ConsumerEvidence       string            `json:"consumerEvidence"`
	RetryEvidence          string            `json:"retryEvidence"`
	AssignmentInputSHA256  map[string]string `json:"assignmentInputSHA256,omitempty"`
	SelectedContentSHA256  map[string]string `json:"selectedContentSHA256"`
	ExcludedContentSHA256  []string          `json:"excludedContentSHA256"`
	PreservedPrivateSHA256 map[string]string `json:"preservedPrivateSHA256"`
}

func (v *verifier) convertTaskHome(parent context.Context, fixture *nativeHomeFixture, observed homeObservation, assigned bool) (result homeConversion, returnErr error) {
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	phase := "home-unassigned"
	if assigned {
		phase = "home-assigned"
	}
	request, err := v.bindObservedHome(fixture, observed)
	if err != nil {
		return result, err
	}
	release, err := v.store.AcquireLease(filepath.Base(request.WorkerSubPath))
	if err != nil {
		return result, err
	}
	defer release()
	workerRoot := filepath.Join(wire.WorkspaceRoot, request.WorkerSubPath)
	artifact := filepath.Join(workerRoot, ".runtime-home", request.AttemptID+".tar")
	defer func() { returnErr = errors.Join(returnErr, discardConvertedArchive(artifact)) }()
	expected, assignmentHashes, err := captureNativeHomeExpectation(fixture.bundle, observed, assigned)
	if err != nil {
		return result, err
	}
	request.HomeDigest, err = execution.PrepareTaskHome(request, workerRoot, v.descriptor, fixture.bundle)
	if err != nil {
		return result, err
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return result, err
	}
	if _, err := wire.Decode(raw); err != nil {
		return result, err
	}
	home := filepath.Join(fixture.directory, phase, "agents")
	if err := os.MkdirAll(home, 0700); err != nil {
		return result, err
	}
	if err := execution.InstallTaskHome(home, artifact, request); err != nil {
		return result, err
	}
	consumer := phase + "-converted-provider.json"
	if err := v.consumeConvertedHome(ctx, request, home, consumer); err != nil {
		return result, err
	}
	selected, excluded, err := verifyConvertedHome(home, expected, fixture.forbidden, assigned)
	if err != nil {
		return result, err
	}
	private, retry, err := v.verifyConvertedHomeRetry(ctx, request, home, artifact, phase)
	if err != nil {
		return result, err
	}
	preserved := map[string]string{}
	for name, content := range private {
		preserved[name] = core.Digest(content)
		// The next task must not acquire this consumer's personal state.
		fixture.forbidden = append(fixture.forbidden, bytes.Clone(content))
	}
	return homeConversion{TaskID: request.TaskID, AttemptID: request.AttemptID, WorkerSubPath: request.WorkerSubPath, ConfigurationDigest: fixture.bundle.Digest, HomeDigest: request.HomeDigest, NativeEvidence: phase + ".json", ConsumerEvidence: consumer, RetryEvidence: retry, AssignmentInputSHA256: assignmentHashes, SelectedContentSHA256: selected, ExcludedContentSHA256: excluded, PreservedPrivateSHA256: preserved}, nil
}

// Freeze the committed defaults and actual daemon-rendered assignment before
// controller preparation. Rendering may add metadata; the supplied instruction
// must survive, and the subsequent HOME transport must preserve every byte.
func captureNativeHomeExpectation(bundle configurationbundle.Bundle, observed homeObservation, assigned bool) (map[string][]byte, map[string]string, error) {
	expected := map[string][]byte{}
	for _, group := range bundle.Groups {
		for _, file := range group.Files {
			expected[file.Target] = bytes.Clone(file.Content)
		}
	}
	if !assigned {
		return expected, nil, nil
	}
	root, err := os.OpenRoot(observed.CodexHome)
	if err != nil {
		return nil, nil, err
	}
	defer root.Close()
	assignment := map[string][]byte{}
	for _, name := range []string{"skills/code-review/SKILL.md", "skills/code-review/references/assigned.md"} {
		info, err := root.Lstat(name)
		if err != nil || !info.Mode().IsRegular() {
			return nil, nil, fmt.Errorf("native assigned input is not a regular file: %s", name)
		}
		raw, err := root.ReadFile(name)
		if err != nil {
			return nil, nil, err
		}
		assignment[".codex/"+name] = raw
	}
	if !bytes.Contains(assignment[".codex/skills/code-review/SKILL.md"], bytes.TrimSpace([]byte(homeAssignedInstructions))) {
		return nil, nil, errors.New("native rendered skill lost its requested instruction")
	}
	if !bytes.Equal(assignment[".codex/skills/code-review/references/assigned.md"], []byte(homeAssignedReference)) {
		return nil, nil, errors.New("native assigned reference changed its requested content")
	}
	for name := range expected {
		if strings.HasPrefix(name, ".codex/skills/Code Review/") {
			delete(expected, name)
		}
	}
	hashes := map[string]string{}
	for name, content := range assignment {
		expected[name] = content
		hashes[name] = core.Digest(content)
	}
	return expected, hashes, nil
}

func (v *verifier) bindObservedHome(fixture *nativeHomeFixture, observed homeObservation) (wire.Request, error) {
	request := wire.Request{SchemaVersion: wire.RequestSchemaVersion, TaskID: observed.TaskID, Provider: "codex", Args: []string{}, RuntimeRef: fixture.ref, AttemptID: uuid.NewString(), OwnerID: v.selection.OwnerID, BrokerPort: 1, BrokerToken: "native-home-fixture-" + uuid.NewString(), TerminationGraceSeconds: 1}
	request.Env = wire.Environment(map[string]string{"MULTICA_TASK_ID": observed.TaskID, "MULTICA_TASK_CONFIG_ROOT": observed.ConfigRoot, "MULTICA_TOKEN": nativeHomeTaskToken, "MULTICA_WORKSPACE_ID": workspaceID, "MULTICA_AGENT_ID": agentID})
	request.WorkDir = filepath.Join(filepath.Dir(observed.ConfigRoot), "workdir")
	root, err := wire.StorageRoot(request)
	if err != nil || observed.Home != wire.Home || observed.CodexHome != filepath.Join(root, "codex-home") {
		return wire.Request{}, errors.New("native provider HOME is outside its claimed task preparation")
	}
	claim, err := v.store.Lookup(observed.TaskID, nativeHomeTaskToken, workspaceID, agentID)
	if err != nil {
		return wire.Request{}, err
	}
	if claim.RuntimeRef == nil || !claim.RuntimeRef.Equal(fixture.ref) {
		return wire.Request{}, errors.New("native HOME claim was not admitted with the selected configuration")
	}
	binding, err := v.store.Bind(claim, root, "", fixture.ref)
	if err != nil {
		return wire.Request{}, err
	}
	request.WorkerSubPath, request.RepositoryURLs = binding.WorkerSubPath, claim.RepositoryURLs
	return request, nil
}

func (v *verifier) consumeConvertedHome(ctx context.Context, request wire.Request, home, evidence string, args ...string) error {
	output := filepath.Join(v.evidence, evidence)
	command := exec.CommandContext(ctx, v.helper, append([]string{"home-provider", output}, args...)...)
	command.Dir = request.WorkDir
	command.Env = wire.Environment(map[string]string{"HOME": home, "CODEX_HOME": filepath.Join(home, ".codex"), "MULTICA_TASK_CONFIG_ROOT": wire.Value(request.Env, "MULTICA_TASK_CONFIG_ROOT"), "MULTICA_TASK_ID": request.TaskID})
	if raw, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("converted HOME consumer failed: %w: %s", err, raw)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		return err
	}
	var observed homeObservation
	if err := json.Unmarshal(raw, &observed); err != nil {
		return err
	}
	if observed.TaskID != request.TaskID || observed.Home != home || observed.CodexHome != filepath.Join(home, ".codex") {
		return errors.New("provider consumed a HOME outside its prepared task identity")
	}
	return nil
}

func verifyConvertedHome(home string, expected map[string][]byte, forbidden [][]byte, assigned bool) (map[string]string, []string, error) {
	rejected := append([][]byte{}, forbidden...)
	if assigned {
		rejected = append(rejected, []byte(homeBaseInstructions), []byte(homeBaseReference))
	} else {
		rejected = append(rejected, []byte(homeAssignedInstructions), []byte(homeAssignedReference))
	}
	contentHashes := map[string]string{}
	for name, want := range expected {
		raw, err := os.ReadFile(filepath.Join(home, name))
		if err != nil {
			return nil, nil, fmt.Errorf("read prepared HOME input at %s: %w", name, err)
		}
		if !bytes.Equal(raw, want) {
			return nil, nil, fmt.Errorf("prepared HOME changed selected input at %s: source SHA256 %s, installed SHA256 %s", name, core.Digest(want), core.Digest(raw))
		}
		contentHashes[name] = core.Digest(raw)
	}
	if err := rejectConvertedHomeContent(home, rejected); err != nil {
		return nil, nil, err
	}
	excludedHashes := make([]string, 0, len(rejected))
	for _, content := range rejected {
		excludedHashes = append(excludedHashes, core.Digest(content))
	}
	return contentHashes, excludedHashes, nil
}

func (v *verifier) verifyConvertedHomeRetry(ctx context.Context, request wire.Request, home, artifact, phase string) (map[string][]byte, string, error) {
	if err := v.consumeConvertedHome(ctx, request, home, phase+"-private-update.json", "--fixture-private-home-update"); err != nil {
		return nil, "", err
	}
	private := map[string][]byte{}
	for _, name := range []string{".codex/auth.json", ".codex/sessions/provider-private.jsonl"} {
		raw, err := os.ReadFile(filepath.Join(home, name))
		if err != nil || len(raw) == 0 {
			return nil, "", errors.New("native consumer did not produce its personal credential/session state")
		}
		private[name] = raw
	}
	if err := execution.InstallTaskHome(home, artifact, request); err != nil {
		return nil, "", err
	}
	retry := phase + "-retry-provider.json"
	if err := v.consumeConvertedHome(ctx, request, home, retry); err != nil {
		return nil, "", err
	}
	for name, before := range private {
		after, err := os.ReadFile(filepath.Join(home, name))
		if err != nil || !bytes.Equal(after, before) {
			return nil, "", errors.New("HOME retry replaced the consumer's native credential/session state")
		}
	}
	return private, retry, nil
}

func rejectConvertedHomeContent(home string, forbidden [][]byte) error {
	root, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer root.Close()
	return fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		if !entry.Type().IsRegular() && entry.Type()&os.ModeSymlink == 0 {
			return errors.New("converted HOME exposes a special file to its consumer")
		}
		file, err := root.Open(path)
		if err != nil {
			return fmt.Errorf("converted HOME exposes an unconfined input at %s: %w", path, err)
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("converted HOME exposes an unapproved directory link")
		}
		return rejectHomeFileContent(file, forbidden)
	})
}

func rejectHomeFileContent(file io.Reader, forbidden [][]byte) error {
	longest := 1
	for _, value := range forbidden {
		longest = max(longest, len(value))
	}
	buffer := make([]byte, 32*1024+longest)
	carried := 0
	for {
		n, err := file.Read(buffer[carried:])
		content := buffer[:carried+n]
		for _, value := range forbidden {
			if len(value) != 0 && bytes.Contains(content, value) {
				return errors.New("converted HOME imported unselected instructions, credentials or session data")
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		carried = min(longest-1, len(content))
		copy(buffer[:carried], content[len(content)-carried:])
	}
}

func discardConvertedArchive(path string) error {
	if err := os.Remove(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
