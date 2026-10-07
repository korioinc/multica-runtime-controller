package worker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/githubauth"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

const (
	taskTreeRetryTimeout  = 5 * time.Second
	taskTreeRetryInterval = 100 * time.Millisecond
	piMCPConfigPath       = ".pi/agent/mcp.json"
	piAdapterMCPPath      = ".pi/agent/mcp-adapter.json"
)

// Pi's builtin MCP and installed adapter read separate configuration files.
// Both receive the same prepared task payload, including an explicit empty map.
func configurePiMCP(home *os.Root, mcp json.RawMessage) error {
	if len(mcp) == 0 {
		mcp = json.RawMessage(`{"mcpServers":{}}`)
	}
	for _, path := range []string{piMCPConfigPath, piAdapterMCPPath} {
		if err := configuration.WriteFile(home, path, mcp, 0600); err != nil {
			return err
		}
	}
	return nil
}

func initializeTask(ctx context.Context, d runtimeimage.Descriptor, b wire.Bootstrap, run *wire.Run, baseline *managedHome) (environment map[string]string, resultErr error) {
	attributes := diagnostics.TaskAttributes(b.TaskID, b.AttemptID)
	finish := diagnostics.StartPhase("worker_task_environment", attributes...)
	defer func() { finish(resultErr) }()
	if run.Provider != b.Provider || run.Options.Cwd != b.TaskRoot+"/workdir" || run.Options.Timeout <= 0 {
		return nil, errors.New("execution differs from prepared task")
	}
	finishHome := diagnostics.StartPhase("worker_home_configuration", attributes...)
	err := configuration.ApplyHomeConfiguration(wire.Home, b.Configuration)
	finishHome(err)
	if err != nil {
		return nil, err
	}
	home, err := os.OpenRoot(wire.Home)
	if err != nil {
		return nil, err
	}
	defer home.Close()
	for _, directory := range []string{".multica", ".codex/plugins/cache"} {
		if err := home.MkdirAll(directory, 0700); err != nil {
			return nil, err
		}
	}
	if b.Provider == "pi" {
		if err := home.MkdirAll(".pi/agent", 0700); err != nil {
			return nil, err
		}
		if err := configurePiMCP(home, run.Options.McpConfig); err != nil {
			return nil, err
		}
		sessions := b.TaskRoot + "/pi-sessions"
		info, err := os.Lstat(sessions)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("prepared sessions are unavailable")
		}
		const link = ".multica/pi-sessions"
		if existing, err := home.Readlink(link); err == nil {
			if existing != sessions {
				return nil, errors.New("session link changed task identity")
			}
		} else {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			if err := home.Symlink(sessions, link); err != nil {
				return nil, err
			}
		}
	}
	if b.Provider == "claude" {
		if err := prepareClaudeSessions(home, b.TaskRoot); err != nil {
			return nil, err
		}
	}
	finishTree := diagnostics.StartPhase("worker_nfs_tree", attributes...)
	if run.NativeMetadata != nil || b.NativeMetadataDigest != "" {
		if baseline == nil || baseline.path != wire.Home {
			return nil, errors.New("native metadata requires the original HOME baseline")
		}
		err = materializeNativeMetadata(ctx, b, *run, home)
	} else {
		err = verifyTaskTreeWithRetry(ctx, func() error { return workspace.VerifyTaskTree(b.TaskRoot, b.AllowedLinks) }, attributes...)
	}
	var treeFailure []any
	if err != nil {
		reason := "task_tree_invalid"
		if errors.Is(err, syscall.ESTALE) {
			reason = "nfs_stale_handle"
		}
		treeFailure = []any{"reason", reason}
		var pathError *os.PathError
		if errors.As(err, &pathError) {
			treeFailure = append(treeFailure, "operation", pathError.Op)
		}
		var errno syscall.Errno
		if errors.As(err, &errno) {
			treeFailure = append(treeFailure, "errno", int(errno))
		}
	}
	finishTree(err, treeFailure...)
	if err != nil {
		return nil, err
	}
	if err := writeTaskConfig(d, b, *run); err != nil {
		return nil, err
	}
	env, err := runtimeimage.Vars(d, b.Environment, runtimeimage.Locations{Home: wire.Home, TmpDir: "/tmp", Workspace: b.TaskRoot})
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	for key, value := range run.Environment {
		values[key] = value
	}
	values["HOME"], values["PWD"], values["MULTICA_TASK_CONFIG_ROOT"] = wire.Home, run.Options.Cwd, wire.ControlRoot+"/task-config"
	if b.Provider == "pi" {
		values["PI_MCP_CONFIG_MODE"] = "exclusive"
		if run.Options.ServiceTier != "" {
			if err := configurePiServiceTier(home, d, run, values); err != nil {
				return nil, err
			}
		}
	}
	if b.Provider == "codex" {
		values["CODEX_HOME"] = b.TaskRoot + "/codex-home"
	}
	if b.Provider == "claude" {
		values["CLAUDE_CONFIG_DIR"] = filepath.Join(wire.Home, workspace.ClaudeConfigDir)
		values["CLAUDE_CODE_PROJECT_DIR_NAME"] = workspace.ClaudeProjectDir
	}
	if b.GitHubApp {
		values["PATH"] = wire.ControlRoot + "/bin:" + values["PATH"]
		entries, err := githubauth.GitEnvironment(wire.Environment(values))
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			key, value, ok := strings.Cut(entry, "=")
			if ok {
				values[key] = value
			}
		}
	}
	return values, nil
}

// Keep operator settings in the private HOME. Only conversation state belongs
// to the task volume, so another worker can resume without sharing credentials.
func prepareClaudeSessions(home *os.Root, taskRoot string) error {
	if err := home.Mkdir(workspace.ClaudeConfigDir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := home.Lstat(workspace.ClaudeConfigDir)
	if err != nil || !info.IsDir() {
		return errors.New("Claude configuration directory is unavailable or indirect")
	}
	sessions := filepath.Join(taskRoot, workspace.ClaudeSessionsDir)
	actual, err := filepath.EvalSymlinks(sessions)
	if err != nil || actual != sessions {
		return errors.New("prepared Claude sessions are unavailable or indirect")
	}
	info, err = os.Lstat(sessions)
	if err != nil || !info.IsDir() {
		return errors.New("prepared Claude sessions are unavailable")
	}
	link := filepath.Join(workspace.ClaudeConfigDir, "projects")
	if _, err := home.Lstat(link); errors.Is(err, os.ErrNotExist) {
		return home.Symlink(sessions, link)
	} else if err != nil {
		return err
	}
	existing, err := home.Readlink(link)
	if err != nil || existing != sessions {
		return errors.New("Claude session link changed task identity")
	}
	return nil
}

// Reopen and revalidate the whole tree after a stale NFS handle. The deadline
// bounds retries, but cannot interrupt a filesystem call on a hard NFS mount.
func verifyTaskTreeWithRetry(ctx context.Context, verify func() error, attributes ...any) error {
	deadline := time.Now().Add(taskTreeRetryTimeout)
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := verify()
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		if !errors.Is(err, syscall.ESTALE) {
			return err
		}
		wait := min(taskTreeRetryInterval, time.Until(deadline))
		if wait <= 0 {
			return err
		}
		slog.Warn("task tree scan retry scheduled", append(attributes, "reason", "nfs_stale_handle", "attempt", attempt)...)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if !time.Now().Before(deadline) {
			return err
		}
	}
}

// The installed extension wraps a native provider ID. Keep its configuration
// and credential reference in this worker's disposable HOME, never the task.
func configurePiServiceTier(home *os.Root, d runtimeimage.Descriptor, run *wire.Run, environment map[string]string) error {
	modelID, canonical := strings.CutPrefix(run.Options.Model, "openai/")
	if !canonical || modelID != "gpt-5.4" && modelID != "gpt-5.5" {
		return errors.New("Pi service tier requires a supported canonical OpenAI API-key model")
	}
	executable, err := filepath.EvalSymlinks(d.Providers["pi"].Path)
	if err != nil {
		return err
	}
	packageRoot := filepath.Dir(filepath.Dir(filepath.Dir(executable)))
	catalogPath, err := filepath.EvalSymlinks(filepath.Join(packageRoot, "node_modules/@earendil-works/pi-ai/dist/providers/data/openai.json"))
	if err != nil || !runtimeimage.ImmutablePath(catalogPath) {
		return errors.New("installed Pi OpenAI model catalog is unavailable")
	}
	raw, err := os.ReadFile(catalogPath)
	var catalog map[string]map[string]map[string]any
	if err != nil || json.Unmarshal(raw, &catalog) != nil {
		return errors.New("installed Pi OpenAI model catalog is invalid")
	}
	var model map[string]any
	for _, candidate := range catalog["openai-responses"] {
		if candidate["id"] == modelID {
			if model != nil {
				return errors.New("installed Pi OpenAI model identity is ambiguous")
			}
			model = candidate
		}
	}
	if model["id"] != modelID || model["provider"] != "openai" || model["api"] != "openai-responses" || model["baseUrl"] != "https://api.openai.com/v1" {
		return errors.New("installed Pi OpenAI model identity is unsupported")
	}
	const extensionPath = ".pi/agent/npm/node_modules/pi-openai-service-tier/index.ts"
	if info, err := home.Lstat(extensionPath); err != nil || !info.Mode().IsRegular() {
		return errors.New("installed Pi service-tier extension is unavailable")
	}
	readObject := func(name string) (map[string]any, error) {
		value := map[string]any{}
		raw, err := home.ReadFile(name)
		if errors.Is(err, os.ErrNotExist) {
			return value, nil
		}
		if err != nil || json.Unmarshal(raw, &value) != nil || value == nil {
			return nil, errors.New("invalid native Pi configuration")
		}
		return value, nil
	}
	models, err := readObject(".pi/agent/models.json")
	if err != nil {
		return err
	}
	providerValue, present := models["providers"]
	providers, ok := providerValue.(map[string]any)
	if !ok {
		if present {
			return errors.New("invalid native Pi model providers")
		}
		providers = map[string]any{}
		models["providers"] = providers
	}
	original := map[string]any{}
	if value, exists := providers["openai"]; exists {
		original, ok = value.(map[string]any)
		if !ok || original["oauth"] != nil {
			return errors.New("Pi service tier requires OpenAI API-key authentication")
		}
	}
	definition := false
	if value, exists := original["models"]; exists {
		entries, ok := value.([]any)
		if !ok {
			return errors.New("invalid native Pi model definitions")
		}
		for _, entry := range entries {
			candidate, ok := entry.(map[string]any)
			if !ok {
				return errors.New("invalid native Pi model definition")
			}
			if candidate["id"] == modelID {
				model, definition = maps.Clone(candidate), true
			}
		}
	}
	api, _ := model["api"].(string)
	if _, exists := model["api"]; exists && api == "" {
		return errors.New("invalid native Pi model API")
	}
	if api == "" && definition {
		api, _ = original["api"].(string)
		if _, exists := original["api"]; exists && api == "" {
			return errors.New("invalid native Pi provider API")
		}
	}
	if api == "" {
		api = "openai-responses"
	}
	if api != "openai-responses" {
		return errors.New("Pi service tier requires the OpenAI Responses API")
	}
	baseURL := ""
	if definition {
		baseURL, _ = model["baseUrl"].(string)
		if _, exists := model["baseUrl"]; exists && baseURL == "" {
			return errors.New("invalid native Pi model endpoint")
		}
	}
	if baseURL == "" {
		baseURL, _ = original["baseUrl"].(string)
		if _, exists := original["baseUrl"]; exists && baseURL == "" {
			return errors.New("invalid native Pi provider endpoint")
		}
	}
	if baseURL == "" {
		baseURL = environment["OPENAI_BASE_URL"]
	}
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	endpoint, err := url.Parse(baseURL)
	if err != nil || endpoint.Hostname() == "" || endpoint.Scheme != "http" && endpoint.Scheme != "https" || endpoint.User != nil || endpoint.Fragment != "" {
		return errors.New("invalid configured OpenAI API endpoint")
	}
	key := environment["OPENAI_API_KEY"]
	keyReference := false
	if value, exists := original["apiKey"]; exists {
		key, ok = value.(string)
		if !ok {
			return errors.New("invalid native Pi API key configuration")
		}
		keyReference = true
	}
	auth, err := readObject(".pi/agent/auth.json")
	if err != nil {
		return err
	}
	var credential map[string]any
	if value, exists := auth["openai"]; exists {
		credential, ok = value.(map[string]any)
		if !ok || credential["type"] != "api_key" {
			return errors.New("Pi service tier requires OpenAI API-key authentication")
		}
		key, _ = credential["key"].(string)
		keyReference = true
	}
	if keyReference && strings.HasPrefix(key, "$") {
		name := strings.TrimPrefix(key, "$")
		if strings.HasPrefix(name, "{") && strings.HasSuffix(name, "}") {
			name = name[1 : len(name)-1]
		}
		if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(name) {
			return errors.New("Pi service tier API key reference is unsupported")
		}
		key = environment[name]
	}
	if strings.TrimSpace(key) == "" || strings.ContainsAny(key, "$!\x00\r\n") {
		return errors.New("Pi service tier requires a usable OpenAI API key")
	}
	const providerID = "pi-openai-service-tier:openai-responses"
	alias := maps.Clone(original)
	model["api"], model["baseUrl"] = api, baseURL
	alias["api"], alias["baseUrl"], alias["models"] = api, baseURL, []any{model}
	alias["apiKey"] = "$MULTICA_PI_OPENAI_API_KEY"
	providers[providerID] = alias
	raw, err = json.Marshal(models)
	if err != nil {
		return err
	}
	if err := configuration.WriteFile(home, ".pi/agent/models.json", raw, 0600); err != nil {
		return err
	}
	settings, err := readObject(".pi/agent/settings.json")
	if err != nil {
		return err
	}
	extensions := []any{}
	if value, exists := settings["extensions"]; exists {
		extensions, ok = value.([]any)
		if !ok {
			return errors.New("invalid native Pi extension settings")
		}
	}
	extension := filepath.Join(home.Name(), extensionPath)
	found := false
	for _, value := range extensions {
		found = found || value == extension
	}
	if !found {
		settings["extensions"] = append(extensions, extension)
	}
	raw, err = json.Marshal(settings)
	if err != nil {
		return err
	}
	if err := configuration.WriteFile(home, ".pi/agent/settings.json", raw, 0600); err != nil {
		return err
	}
	// Native stored API-key credentials also carry a scoped header environment.
	// Preserve it in HOME auth storage, without overriding the worker environment.
	_, storedAlias := auth[providerID]
	if credential != nil {
		auth[providerID] = credential
	} else {
		delete(auth, providerID)
	}
	if credential != nil || storedAlias {
		raw, err = json.Marshal(auth)
		if err != nil {
			return err
		}
		if err := configuration.WriteFile(home, ".pi/agent/auth.json", raw, 0600); err != nil {
			return err
		}
	}
	environment["MULTICA_PI_OPENAI_API_KEY"] = key
	run.Options.Model = providerID + "/" + modelID
	return nil
}

func writeTaskConfig(d runtimeimage.Descriptor, b wire.Bootstrap, run wire.Run) error {
	controlRoot, err := os.OpenRoot(wire.ControlRoot)
	if err != nil {
		return err
	}
	defer controlRoot.Close()
	if b.GitHubApp {
		authorization := githubauth.TaskAuthorization{GatewayURL: b.GatewayURL, CacheCapability: b.CacheCapability}
		if err := authorization.Validate(); err != nil {
			return err
		}
		raw, err := json.Marshal(authorization)
		if err != nil {
			return err
		}
		if err := configuration.WriteFile(controlRoot, githubauth.TaskAuthorizationName, raw, 0600); err != nil {
			return err
		}
		var executable string
		for _, directory := range d.BinDirs {
			candidate := filepath.Join(directory, "gh")
			resolved, err := filepath.EvalSymlinks(candidate)
			if err != nil {
				continue
			}
			info, err := os.Stat(resolved)
			if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 && runtimeimage.ImmutablePath(resolved) {
				executable = resolved
				break
			}
		}
		if executable == "" {
			return errors.New("managed GitHub authentication requires installed gh")
		}
		if err := controlRoot.MkdirAll("bin", 0700); err != nil {
			return err
		}
		quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
		wrapper := "#!/bin/sh\nexec " + quote(wire.ControllerRoot+"/runtime") + " github gh " + quote(executable) + " \"$@\"\n"
		if err := configuration.WriteFile(controlRoot, "bin/gh", []byte(wrapper), 0700); err != nil {
			return err
		}
	}
	if err := controlRoot.MkdirAll("task-config", 0700); err != nil {
		return err
	}
	return configuration.WriteFile(controlRoot, "task-config/config.json", run.TaskConfig, 0600)
}
