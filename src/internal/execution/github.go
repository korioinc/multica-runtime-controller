package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/githubauth"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type taskTokenSource func(context.Context, []githubapp.Repository) (githubapp.Token, error)

// The capability check in startBroker must run before this handler. Repository
// grants come from the controller's observed claim, never from the caller.
func serveTaskGitHubToken(request wire.Request, source taskTokenSource, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if wire.Value(request.Env, githubauth.EnabledEnv) != "true" {
		http.Error(w, "GitHub App authentication is not configured", http.StatusServiceUnavailable)
		return
	}
	var input githubauth.Request
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid GitHub token request", http.StatusBadRequest)
		return
	}
	repositories, err := taskGitHubScope(request.RepositoryURLs, input.Repository)
	if err != nil {
		http.Error(w, "GitHub repository is outside the observed task scope", http.StatusForbidden)
		return
	}
	token, err := source(r.Context(), repositories)
	if err != nil {
		http.Error(w, "GitHub App token unavailable; check installation access and select a repository when the task spans owners", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(token)
}

func taskGitHubScope(grants []string, selected string) ([]githubapp.Repository, error) {
	var requested githubapp.Repository
	if selected != "" {
		var err error
		requested, err = githubapp.ParseRepositoryURL(selected)
		if err != nil {
			return nil, err
		}
	}
	repositories := []githubapp.Repository{}
	seen := map[githubapp.Repository]bool{}
	for _, grant := range grants {
		repository, err := githubapp.ParseRepositoryURL(grant)
		if err != nil {
			continue
		}
		if selected != "" && repository != requested {
			continue
		}
		if !seen[repository] {
			repositories = append(repositories, repository)
			seen[repository] = true
		}
	}
	if len(repositories) == 0 {
		return nil, errors.New("task has no matching GitHub repository grant")
	}
	return repositories, nil
}

func forwardGitHubToken(request wire.Request, origin, secret string, client *http.Client, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if wire.Value(request.Env, githubauth.EnabledEnv) != "true" {
		http.Error(w, "GitHub App authentication is not configured", http.StatusServiceUnavailable)
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		http.Error(w, "invalid GitHub token request", http.StatusBadRequest)
		return
	}
	upstream, err := http.NewRequestWithContext(r.Context(), http.MethodPost, origin+githubauth.Route, bytes.NewReader(raw))
	if err != nil {
		http.Error(w, "GitHub authentication unavailable", http.StatusBadGateway)
		return
	}
	upstream.Header.Set(wire.SecretHeader, secret)
	upstream.Header.Set(wire.TaskHeader, request.TaskID)
	upstream.Header.Set(wire.TokenHeader, wire.Value(request.Env, "MULTICA_TOKEN"))
	upstream.Header.Set("Content-Type", "application/json")
	response, err := client.Do(upstream)
	if err != nil {
		http.Error(w, "GitHub authentication transport unavailable", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(body) > 64<<10 {
		http.Error(w, "invalid GitHub authentication response", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(body)
}

// The wrapper lives in task-private PATH ahead of the real immutable gh binary.
// Only a fresh installation token enters the child gh process; task lifetime
// does not pin its token to the worker's initial environment.
func installGitHubCLIWrapper(bin string, manifest runtimeimage.Descriptor) error {
	var executable string
	for _, directory := range manifest.BinDirs {
		candidate := filepath.Join(directory, "gh")
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil || !runtimeimage.ImmutablePath(resolved) {
			continue
		}
		info, err := os.Stat(resolved)
		if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
			executable = resolved
			break
		}
	}
	if executable == "" {
		return errors.New("GitHub App authentication requires GitHub CLI in the runtime image")
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
	content := "#!/bin/sh\nexec " + quote(wire.ControllerRoot+"/runtime") + " github gh " + quote(executable) + " \"$@\"\n"
	return os.WriteFile(filepath.Join(bin, "gh"), []byte(content), 0700)
}
