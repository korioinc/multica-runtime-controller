package environment

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
)

func write(t *testing.T, path, text string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), mode); err != nil {
		t.Fatal(err)
	}
}
func fixture(t *testing.T, script string) Options {
	t.Helper()
	base := t.TempDir()
	source := filepath.Join(base, "artifact")
	for _, name := range []string{"runtime", "multica"} {
		write(t, filepath.Join(source, name), "#!/bin/sh\nexit 0\n", 0555)
	}
	hash, _ := core.HashFile(filepath.Join(source, "runtime"))
	contract := core.Contract{ContractVersion: 1, BuildID: "fixture", Platform: "linux/amd64", OfficialVersion: "0.4.40", OfficialSHA256: hash, Files: map[string]string{"runtime": hash, "multica": hash}}
	b, _ := json.Marshal(contract)
	write(t, filepath.Join(source, "contract.json"), string(b), 0444)
	coreRoot := filepath.Join(base, "core")
	if _, err := core.Materialize(source, coreRoot, "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	input := Input{SchemaVersion: 1, CoreImage: "example/core@sha256:" + strings.Repeat("1", 64), EnvironmentImage: "example/environment@sha256:" + strings.Repeat("2", 64), Platform: "linux/amd64", ScriptSHA256: core.Digest([]byte(script)), Providers: []string{"pi"}, Inputs: map[string]string{}}
	id, err := Identity(input)
	if err != nil {
		t.Fatal(err)
	}
	store := filepath.Join(base, "tools")
	if err = Layout(store, "owner", id); err != nil {
		t.Fatal(err)
	}
	scriptPath := filepath.Join(base, "bootstrap.sh")
	write(t, scriptPath, script, 0400)
	return Options{Root: filepath.Join(store, "generations", id), CoreRoot: coreRoot, Input: input, ExpectedID: id, ScriptPath: scriptPath, InstallEnv: []string{"PATH=/usr/bin:/bin", "INSTALL_TOKEN=private-install-value"}, Timeout: 10 * time.Second}
}

const workingScript = `set -eu
mkdir -p "$ENV_ROOT/bin" "$ENV_ROOT/seed"
test -n "$INSTALL_TOKEN"
printf "%s" "$INSTALL_TOKEN" > "$HOME/install-credential"
printf '%s\n' '#!/bin/sh' 'test -z "${INSTALL_TOKEN:-}"' 'test ! -f "$HOME/install-credential"' 'printf "provider ready\\n"' > "$ENV_ROOT/bin/provider"
chmod 755 "$ENV_ROOT/bin/provider"
printf '%s' '{"feature":true}' > "$ENV_ROOT/seed/settings.json"
printf '%s' '{"schemaVersion":1,"providers":{"pi":{"entrypoint":"bin/provider","version":"test"}},"binDirs":["bin"],"env":{},"homeSeed":"seed"}' > "$ENV_MANIFEST_FILE"
`

func TestImmutableGenerationRejectsChangedTools(t *testing.T) {
	opts := fixture(t, workingScript)
	ref, err := Prepare(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	// A successful generation is reused even if the bootstrap source later vanishes.
	if err = os.Remove(opts.ScriptPath); err != nil {
		t.Fatal(err)
	}
	again, err := Prepare(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !ref.Equal(again) {
		t.Fatal("restart changed the committed environment")
	}
	write(t, filepath.Join(opts.Root, "bin/provider"), "#!/bin/sh\necho altered\n", 0755)
	if _, _, err = Check(opts.Root, opts.CoreRoot, opts.Input, &ref); err == nil {
		t.Fatal("worker accepted modified executable")
	}
	if _, err = Prepare(context.Background(), opts); err == nil {
		t.Fatal("preparer repaired an immutable generation")
	}
	if got, _ := os.ReadFile(filepath.Join(opts.Root, "bin/provider")); !strings.Contains(string(got), "altered") {
		t.Fatal("failed immutable check changed operator files")
	}
}
func TestFailedInstallRetriesWithoutMovingPrefix(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "first-attempt")
	script := "set -eu\nif test ! -e " + marker + "; then touch " + marker + "; touch \"$ENV_ROOT/partial\"; exit 7; fi\ntest ! -e \"$ENV_ROOT/partial\"\n" + workingScript + "printf '%s' \"$ENV_ROOT\" > \"$ENV_ROOT/prefix\"\n"
	opts := fixture(t, script)
	before, err := os.Stat(opts.Root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Prepare(context.Background(), opts); err == nil {
		t.Fatal("failed installation was published")
	}
	if _, _, err = Check(opts.Root, opts.CoreRoot, opts.Input, nil); err == nil {
		t.Fatal("consumed an incomplete installation")
	}
	if _, err = Prepare(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(opts.Root)
	if !os.SameFile(before, after) {
		t.Fatal("installation moved the mounted generation")
	}
	prefix, err := os.ReadFile(filepath.Join(opts.Root, "prefix"))
	if err != nil {
		t.Fatal(err)
	}
	if string(prefix) != opts.Root {
		t.Fatal("installed executable prefix changed")
	}
}

func TestScriptDigestMismatchPreventsInstallerExecution(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "executed")
	opts := fixture(t, "touch "+marker+"\n"+workingScript)
	if err := os.Remove(opts.ScriptPath); err != nil {
		t.Fatal(err)
	}
	write(t, opts.ScriptPath, workingScript, 0400)
	if _, err := Prepare(context.Background(), opts); err == nil {
		t.Fatal("executed mismatched script bytes")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("mismatched installer obtained execution")
	}
}

func TestHomeSeedDoesNotShareMutableFiles(t *testing.T) {
	opts := fixture(t, workingScript)
	ref, err := Prepare(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	_, manifest, err := Check(opts.Root, opts.CoreRoot, opts.Input, &ref)
	if err != nil {
		t.Fatal(err)
	}
	first, second := t.TempDir(), t.TempDir()
	for _, home := range []string{first, second} {
		if err = CopySeed(opts.Root, manifest, home); err != nil {
			t.Fatal(err)
		}
	}
	write(t, filepath.Join(first, "settings.json"), "changed by task", 0600)
	seed, _ := os.ReadFile(filepath.Join(opts.Root, "seed/settings.json"))
	other, _ := os.ReadFile(filepath.Join(second, "settings.json"))
	if strings.Contains(string(seed), "changed") || strings.Contains(string(other), "changed") {
		t.Fatal("task mutation escaped into the seed or another HOME")
	}
}
func TestPreparationHelper(t *testing.T) {
	path := os.Getenv("MULTICA_TEST_PREPARE")
	if path == "" {
		return
	}
	var opts Options
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &opts); err != nil {
		t.Fatal(err)
	}
	if _, err = Prepare(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
}
func startPreparation(t *testing.T, opts Options) (*exec.Cmd, *strings.Builder) {
	t.Helper()
	b, _ := json.Marshal(opts)
	path := filepath.Join(t.TempDir(), "options.json")
	write(t, path, string(b), 0600)
	cmd := exec.Command(os.Args[0], "-test.run=^TestPreparationHelper$")
	cmd.Env = append(os.Environ(), "MULTICA_TEST_PREPARE="+path)
	output := &strings.Builder{}
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return cmd, output
}
func TestChildWriterPreventsPublicationAndConcurrentConsumption(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux subreaper boundary; exercised in local Linux fixture")
	}
	gate := filepath.Join(t.TempDir(), "release")
	started := filepath.Join(t.TempDir(), "started")
	script := workingScript + "\nmkdir " + started + "\n(while test ! -f " + gate + "; do sleep 0.02; done; printf child-finished > \"$ENV_ROOT/child\") &\n"
	opts := fixture(t, script)
	first, firstOut := startPreparation(t, opts)
	deadline := time.After(5 * time.Second)
	for {
		if _, err := os.Stat(started); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("bootstrap did not start")
		case <-time.After(10 * time.Millisecond):
		}
	}
	second, secondOut := startPreparation(t, opts)
	if _, _, err := Check(opts.Root, opts.CoreRoot, opts.Input, nil); err == nil {
		t.Fatal("consumer accepted a generation with a live background writer")
	}
	write(t, gate, "finish", 0600)
	if err := first.Wait(); err != nil {
		t.Fatalf("first preparation: %v\n%s", err, firstOut)
	}
	if err := second.Wait(); err != nil {
		t.Fatalf("second preparation: %v\n%s", err, secondOut)
	}
	if _, _, err := Check(opts.Root, opts.CoreRoot, opts.Input, nil); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(opts.Root, "child"))
	if err != nil || !strings.Contains(string(b), "child-finished") {
		t.Fatal("published before the child committed its output")
	}
}
func TestToolsStoreRefusesForeignOwner(t *testing.T) {
	root := t.TempDir()
	id := strings.Repeat("3", 64)
	if err := Layout(root, "first", id); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(root, "generations", id, "user-data"), "preserve", 0600)
	if err := Layout(root, "foreign", id); err == nil {
		t.Fatal("foreign installation adopted the tools store")
	}
	if _, err := os.ReadFile(filepath.Join(root, "generations", id, "user-data")); err != nil {
		t.Fatal("ownership rejection damaged tools")
	}
}

func TestTimeoutReapsEscapedWriterBeforeReturning(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux descendant/subreaper boundary")
	}
	pidFile := filepath.Join(t.TempDir(), "writer.pid")
	script := workingScript + "setsid /bin/sh -c 'trap \"\" TERM; echo $$ > " + pidFile + "; while :; do sleep 0.02; done' &\n"
	opts := fixture(t, script)
	opts.Timeout = 2 * time.Second
	if _, err := Prepare(context.Background(), opts); err == nil {
		t.Fatal("published environment with a surviving escaped writer")
	}
	b, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal("escaped writer did not execute", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	if err = syscall.Kill(pid, 0); err != syscall.ESRCH {
		t.Fatal("preparation returned while escaped writer survived", err)
	}
	if _, _, err = Check(opts.Root, opts.CoreRoot, opts.Input, nil); err == nil {
		t.Fatal("consumer accepted timed out preparation")
	}
}

func TestInstalledHiddenHelperIsCoveredByIntegrityValidation(t *testing.T) {
	script := workingScript + `printf '%s\n' '#!/bin/sh' 'exit 0' > "$ENV_ROOT/.prepare.lock"
chmod 755 "$ENV_ROOT/.prepare.lock"
printf '%s\n' '#!/bin/sh' 'exec "$(dirname "$0")/../.prepare.lock"' > "$ENV_ROOT/bin/provider"
`
	opts := fixture(t, script)
	opts.LockPath = filepath.Join(t.TempDir(), "generation.lock")
	ref, err := Prepare(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(opts.Root, ".prepare.lock"), "#!/bin/sh\nexit 7\n", 0755)
	if _, _, err = Check(opts.Root, opts.CoreRoot, opts.Input, &ref); err == nil {
		t.Fatal("consumer accepted modified installed helper")
	}
}

func TestProviderMustRemainTheExecutableThatPassedItsProbe(t *testing.T) {
	cases := []struct{ name, setup, mutation string }{
		{"self-modification", "", `printf '%s\n' '#!/bin/sh' 'exit 9' >> "$dir/verified"`},
		{"replacement-after-probe", "", ""},
		{"symlink-retarget-after-probe", `ln -s verified "$ENV_ROOT/bin/provider"`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script := workingScript + `rm "$ENV_ROOT/bin/provider"
cat > "$ENV_ROOT/bin/verified" <<'PROVIDER'
#!/bin/sh
set -eu
dir=$(dirname "$0")
touch "$dir/probe-succeeded"
` + tc.mutation + `
exit 0
PROVIDER
chmod 755 "$ENV_ROOT/bin/verified"
`
			if tc.setup != "" {
				script += tc.setup + "\n"
			} else {
				script += `cp "$ENV_ROOT/bin/verified" "$ENV_ROOT/bin/provider"` + "\n"
			}
			if tc.name == "self-modification" {
				script += `rm "$ENV_ROOT/bin/provider"; ln -s verified "$ENV_ROOT/bin/provider"` + "\n"
			}
			script += `cp "$ENV_ROOT/bin/verified" "$ENV_ROOT/bin/replacement"
cat > "$ENV_ROOT/bin/check" <<'CHECK'
#!/bin/sh
set -eu
dir=$(dirname "$0")
test -f "$dir/probe-succeeded"
`
			switch tc.name {
			case "replacement-after-probe":
				script += `printf '%s\n' '#!/bin/sh' 'exit 9' > "$dir/provider"` + "\n"
			case "symlink-retarget-after-probe":
				script += `rm "$dir/provider"; ln -s replacement "$dir/provider"` + "\n"
			}
			script += `CHECK
chmod 755 "$ENV_ROOT/bin/check"
printf '%s' '{"schemaVersion":1,"providers":{"pi":{"entrypoint":"bin/provider","version":"test"}},"binDirs":["bin"],"env":{},"checks":[{"argv":["bin/check"]}]}' > "$ENV_MANIFEST_FILE"
`
			opts := fixture(t, script)
			if _, err := Prepare(context.Background(), opts); err == nil {
				t.Fatal("published a provider different from the executable that passed validation")
			}
			if _, err := os.Stat(filepath.Join(opts.Root, "bin/probe-succeeded")); err != nil {
				t.Fatal("fixture never reached the successful provider probe", err)
			}
			if _, _, err := Check(opts.Root, opts.CoreRoot, opts.Input, nil); err == nil {
				t.Fatal("consumer accepted changed provider after validation")
			}
		})
	}
}

func TestProbeUsesImagePathWithoutInstallerCredentials(t *testing.T) {
	imageBin := t.TempDir()
	proof := filepath.Join(t.TempDir(), "provider-executed")
	write(t, filepath.Join(imageBin, "image-provider"), "#!/bin/sh\nset -eu\ntest -z \"${INSTALL_TOKEN:-}\"\ntest ! -f \"$HOME/install-credential\"\nprintf ran > "+proof+"\n", 0755)
	script := workingScript + `printf '%s\n' '#!/bin/sh' 'exec image-provider "$@"' > "$ENV_ROOT/bin/provider"
`
	opts := fixture(t, script)
	opts.BaseEnv = []string{"PATH=" + imageBin + ":/usr/bin:/bin"}
	// The image-only executable is intentionally absent from installer PATH.
	if _, err := Prepare(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(proof); err != nil {
		t.Fatal("provider from the image PATH did not execute", err)
	}
}
