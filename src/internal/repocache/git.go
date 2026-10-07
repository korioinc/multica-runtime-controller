package repocache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/processgroup"
)

type gitOutput struct{ bytes.Buffer }

var ErrWritersUnproven = errors.New("repository cache Git writers could not be stopped; cache is quarantined until process restart")

func (b *gitOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 8<<20 {
		return 0, errors.New("Git output exceeds budget")
	}
	return b.Buffer.Write(p)
}

func (m *Manager) git(ctx context.Context, dir string, r remote, input []byte, args ...string) ([]byte, error) {
	if m.writersUnproven() {
		return nil, ErrWritersUnproven
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + m.root, "XDG_CONFIG_HOME=" + m.root,
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0",
		"GIT_LFS_SKIP_SMUDGE=1", "GIT_ATTR_NOSYSTEM=1", "GIT_NO_REPLACE_OBJECTS=1", "GIT_OPTIONAL_LOCKS=0", "LC_ALL=C"}
	// Preserve the controller's trusted CA file without inheriting ambient Git
	// configuration or proxy settings. Remote addresses must still be public.
	if ca := os.Getenv("SSL_CERT_FILE"); ca != "" {
		cmd.Env = append(cmd.Env, "GIT_SSL_CAINFO="+ca)
	}
	settings := []string{"protocol.allow=never", "protocol.https.allow=always", "credential.helper=", "credential.interactive=false",
		"http.proxy=", "http.followRedirects=false", "http.sslVerify=true", "http.extraHeader=", "http.cookieFile=", "http.saveCookies=false",
		"core.hooksPath=/dev/null", "core.fsmonitor=false", "core.attributesFile=/dev/null", "init.templateDir=", "gc.auto=0", "maintenance.auto=false",
		"core.logAllRefUpdates=false",
		"fetch.recurseSubmodules=false", "fetch.fsckObjects=true", "transfer.fsckObjects=true", "fetch.unpackLimit=1", "pack.threads=1"}
	settings = append(settings, r.config...)
	cmd.Env = append(cmd.Env, "GIT_CONFIG_COUNT="+strconv.Itoa(len(settings)))
	for i, setting := range settings {
		key, value, found := bytes.Cut([]byte(setting), []byte("="))
		if !found {
			return nil, errors.New("invalid controlled Git configuration")
		}
		cmd.Env = append(cmd.Env, fmt.Sprintf("GIT_CONFIG_KEY_%d=%s", i, key), fmt.Sprintf("GIT_CONFIG_VALUE_%d=%s", i, value))
	}
	cmd.Stdin = bytes.NewReader(input)
	var output gitOutput
	cmd.Stdout = &output
	// Git diagnostics can contain a remote URL or credentials; never propagate
	// stderr to task errors or logs.
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 2 * time.Second
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.New("repository cache Git command could not start")
	}
	err := cmd.Wait()
	if stopErr := processgroup.Stop(cmd.Process.Pid); stopErr != nil {
		m.poison()
		return nil, ErrWritersUnproven
	}
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, errors.New("repository cache Git command failed")
	}
	return output.Bytes(), nil
}
