package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// verifyAccess runs in the disposable controller's actual mount/credential
// context. Its positive control uses an observed claim before the negative
// cases exercise the same real hardlink shim with conflicting authority.
func verifyAccess(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if err := fixtureOnly(); err != nil {
		return err
	}
	if path != "/tmp/fixture-access-request.json" {
		return errors.New("access fixture requires its explicit temporary input")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	request, err := wire.Decode(raw)
	if err != nil {
		return err
	}
	if request.Provider != "pi" {
		return errors.New("access fixture requires its observed Pi claim")
	}
	session, err := wire.PiSession(request)
	if err != nil {
		return err
	}
	beforeSession, err := os.ReadFile(session)
	if err != nil {
		return err
	}
	worker := filepath.Join(wire.WorkspaceRoot, request.WorkerSubPath)
	before, err := treeDigest(worker)
	if err != nil {
		return err
	}
	base := map[string]string{}
	for _, entry := range append(os.Environ(), request.Env...) {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			base[key] = value
		}
	}
	invoke := func(env map[string]string, directory string) ([]byte, error) {
		command := exec.CommandContext(ctx, wire.ControllerRoot+"/shims/pi", "--version", "--session", session)
		command.Env = wire.Environment(env)
		command.Dir = directory
		command.Stdin = strings.NewReader("")
		var stderr bytes.Buffer
		command.Stderr = &stderr
		out, err := command.Output()
		if stderr.Len() != 0 {
			return nil, errors.New("runtime diagnostics entered the provider stderr stream")
		}
		return out, err
	}
	output, err := invoke(base, request.WorkDir)
	if err != nil || len(bytes.TrimSpace(output)) == 0 {
		return fmt.Errorf("observed-claim positive control failed: %w", err)
	}
	user, err := os.MkdirTemp("/tmp", "fixture-private-user-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(user)
	sentinel := filepath.Join(user, "unfinished.txt")
	if err := os.WriteFile(sentinel, []byte("private user work"), 0600); err != nil {
		return err
	}
	type deniedCase struct {
		Case   string `json:"case"`
		TaskID string `json:"taskID"`
	}
	cases := []struct{ name, key, value, directory string }{
		{"unobserved", "MULTICA_TASK_ID", uuid.NewString(), request.WorkDir},
		{"credential-mismatch", "MULTICA_TOKEN", "mat_not_the_observed_credential", request.WorkDir},
		{"scope-mismatch", "MULTICA_WORKSPACE_ID", uuid.NewString(), request.WorkDir},
		{"local-directory", "", "", user},
	}
	denied := []deniedCase{}
	for _, test := range cases {
		values := map[string]string{}
		for key, value := range base {
			values[key] = value
		}
		if test.key != "" {
			values[test.key] = test.value
		}
		output, err := invoke(values, test.directory)
		var refused *exec.ExitError
		if !errors.As(err, &refused) || refused.ExitCode() == 0 || len(output) != 0 {
			return fmt.Errorf("%s unexpectedly executed a provider", test.name)
		}
		denied = append(denied, deniedCase{test.name, values["MULTICA_TASK_ID"]})
	}
	after, err := treeDigest(worker)
	if err != nil || after != before {
		return errors.New("authorization probes changed bound worker files")
	}
	afterSession, err := os.ReadFile(session)
	if err != nil || !bytes.Equal(beforeSession, afterSession) {
		return errors.New("authorization probes changed provider history")
	}
	private, err := os.ReadFile(sentinel)
	if err != nil || string(private) != "private user work" {
		return errors.New("local-directory refusal modified user files")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"positiveControl": request.TaskID, "denied": denied, "workerDataDigest": after, "passed": true})
}

func treeDigest(directory string) (string, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return "", err
	}
	defer root.Close()
	hash := sha256.New()
	err = fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || name == "." {
			return err
		}
		// HOME archives are attempt-owned transport artifacts. The positive
		// execution creates and cleans them; all work and session data still
		// participate in the preservation proof.
		if name == ".runtime-home" && entry.IsDir() {
			return fs.SkipDir
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		fmt.Fprintf(hash, "%s\x00%o\x00", name, info.Mode())
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := root.Readlink(name)
			if err != nil {
				return err
			}
			_, err = io.WriteString(hash, target)
			return err
		}
		if info.Mode().IsRegular() {
			file, err := root.Open(name)
			if err != nil {
				return err
			}
			_, err = io.Copy(hash, file)
			return errors.Join(err, file.Close())
		}
		if !info.IsDir() {
			return errors.New("unsupported worker data object")
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
