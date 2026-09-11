package githubauth

import (
	"context"
	"errors"
	"net/url"
	"os/exec"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// NeedsAuthentication avoids issuing credentials for unambiguous local help
// and version commands. Other invocations use the managed authorization path.
func NeedsAuthentication(args []string) bool {
	if len(args) == 0 || args[0] == "help" {
		return false
	}
	if len(args) == 1 {
		switch args[0] {
		case "--version", "version", "--help", "-h":
			return false
		}
	}
	if len(args) <= 3 && (args[len(args)-1] == "--help" || args[len(args)-1] == "-h") {
		for _, arg := range args[:len(args)-1] {
			if strings.HasPrefix(arg, "-") {
				return true
			}
		}
		return false
	}
	return true
}

// PrepareGH is used only in App-managed mode. It supplies a current token to
// one gh invocation; a command running beyond token expiry is not replayed.
func PrepareGH(ctx context.Context, args, env []string, directory string) ([]string, error) {
	values := ghEnvironment(env)
	values["GH_HOST"] = "github.com"
	if !NeedsAuthentication(args) {
		return wire.Environment(values), nil
	}
	if host := wire.Value(env, "GH_HOST"); host != "" && !strings.EqualFold(host, "github.com") {
		return nil, errors.New("managed GitHub CLI authentication supports github.com only")
	}
	repository, err := resolveGHRepository(ctx, args, env, directory)
	if err != nil {
		return nil, err
	}
	token, err := RequestToken(ctx, repository)
	if err != nil {
		return nil, err
	}
	values["GH_TOKEN"] = token.Value
	// Keep gh's own default repository resolution aligned with the grant. An
	// explicit -R may legitimately override the caller's GH_REPO or current cwd.
	delete(values, "GH_REPO")
	if repository != "" {
		parsed, _ := githubapp.ParseRepositoryURL(repository)
		values["GH_REPO"] = parsed.Owner + "/" + parsed.Name
	}
	return wire.Environment(values), nil
}

func ghEnvironment(env []string) map[string]string {
	values := make(map[string]string)
	for _, entry := range WithoutAppCredentials(env) {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			continue
		}
		switch key {
		case "GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN", "GH_HOST", "GH_CONFIG_DIR", "GH_DEBUG", "GITHUB_WEBHOOK_SECRET":
			continue
		}
		values[key] = value
	}
	return values
}

func resolveGHRepository(ctx context.Context, args, env []string, directory string) (string, error) {
	hints, positional, err := ghArguments(args)
	if err != nil {
		return "", err
	}
	if len(positional) > 1 && positional[0] == "api" {
		repository, err := apiRepository(positional[1])
		if err != nil {
			return "", err
		}
		if repository != "" {
			hints = append(hints, repository)
		}
	} else if len(positional) > 2 {
		if positional[0] == "repo" && repositoryVerb(positional[1]) {
			hints = append(hints, positional[2])
		} else {
			for _, value := range positional[2:] {
				if !strings.Contains(value, "://") {
					continue
				}
				repository, err := repositoryLink(value)
				if err != nil {
					return "", err
				}
				hints = append(hints, repository)
			}
		}
	}
	selected := ""
	for _, hint := range hints {
		repository, err := parseRepositoryHint(hint)
		if err != nil {
			return "", err
		}
		if selected != "" && selected != repository {
			return "", errors.New("GitHub CLI repository targets conflict; select one repository")
		}
		selected = repository
	}
	if selected != "" {
		return selected, nil
	}
	if repository := wire.Value(env, "GH_REPO"); repository != "" {
		return parseRepositoryHint(repository)
	}
	if len(positional) > 0 && positional[0] == "auth" {
		return "", nil
	}
	// GraphQL expressions do not declare a reliably parseable repository. Let
	// the task broker enforce a single-installation grant over observed repos.
	if len(positional) > 1 && positional[0] == "api" {
		endpoint, _ := url.Parse(positional[1]) // apiRepository already validated it.
		if endpoint != nil && strings.Trim(endpoint.Path, "/") == "graphql" {
			return "", nil
		}
	}
	var output hintOutput
	cmd := exec.CommandContext(ctx, "git", "config", "--get", "remote.origin.url")
	cmd.Dir, cmd.Env, cmd.Stdout = directory, env, &output
	err = cmd.Run()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return "", nil
		}
		return "", errors.New("GitHub CLI repository could not be determined from Git configuration")
	}
	if output.oversized {
		return "", errors.New("GitHub CLI repository configuration is too large")
	}
	return parseRepositoryHint(strings.TrimSuffix(output.value.String(), "\n"))
}

// ghArguments extracts repository/host authority while leaving flag payloads
// such as issue bodies and API headers out of positional target detection.
func ghArguments(args []string) (hints, positional []string, err error) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		name, value, assigned := strings.Cut(arg, "=")
		if strings.HasPrefix(arg, "-R") && arg != "-R" {
			name, value, assigned = "-R", strings.TrimPrefix(strings.TrimPrefix(arg, "-R"), "="), true
		}
		if strings.HasPrefix(arg, "-h") && arg != "-h" && len(positional) > 0 && positional[0] == "auth" {
			name, value, assigned = "-h", strings.TrimPrefix(strings.TrimPrefix(arg, "-h"), "="), true
		}
		hostname := name == "--hostname" || name == "--host" || name == "-h" && len(positional) > 0 && positional[0] == "auth"
		if name == "--repo" || name == "-R" || hostname {
			if !assigned {
				i++
				if i == len(args) {
					return nil, nil, errors.New("GitHub CLI target flag is missing its value")
				}
				value = args[i]
			}
			if hostname {
				if !strings.EqualFold(value, "github.com") {
					return nil, nil, errors.New("managed GitHub CLI authentication supports github.com only")
				}
			} else {
				hints = append(hints, value)
			}
			continue
		}
		if strings.HasPrefix(arg, "-") {
			if !assigned && ghValueFlag(name) {
				i++
				if i == len(args) {
					return nil, nil, errors.New("GitHub CLI option is missing its value")
				}
			}
			continue
		}
		positional = append(positional, arg)
	}
	return hints, positional, nil
}

func ghValueFlag(name string) bool {
	switch name {
	case "--method", "-X", "--field", "-F", "--raw-field", "-f", "--header", "-H", "--input", "--cache", "--preview", "-p",
		"--jq", "-q", "--template", "-t", "--json", "--body", "-b", "--body-file", "--title", "-T", "--base", "-B", "--head", "--head-repo",
		"--label", "-l", "--assignee", "-a", "--reviewer", "-r", "--milestone", "-m", "--project", "--state", "-s", "--limit", "-L", "--author", "--search", "-S":
		return true
	}
	return false
}

func repositoryVerb(verb string) bool {
	switch verb {
	case "view", "clone", "fork", "delete", "sync", "archive", "unarchive", "edit", "create", "set-default":
		return true
	}
	return false
}

func parseRepositoryHint(value string) (string, error) {
	if strings.IndexFunc(value, func(r rune) bool { return r <= 0x20 || r == 0x7f }) >= 0 {
		return "", errors.New("invalid GitHub CLI repository target")
	}
	if strings.HasPrefix(strings.ToLower(value), "git@github.com:") {
		value = "https://github.com/" + value[len("git@github.com:"):]
	} else if strings.HasPrefix(strings.ToLower(value), "ssh://git@github.com/") {
		value = "https://github.com/" + value[len("ssh://git@github.com/"):]
	} else if strings.HasPrefix(strings.ToLower(value), "github.com/") {
		value = "https://" + value
	}
	var repository githubapp.Repository
	var err error
	if strings.Contains(value, "://") {
		repository, err = githubapp.ParseRepositoryURL(value)
	} else {
		repository, err = githubapp.ParseRepository(strings.TrimSuffix(value, ".git"))
	}
	if err != nil {
		return "", errors.New("GitHub CLI target must identify a github.com repository")
	}
	return repositoryURL(repository), nil
}

func repositoryLink(value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || !strings.EqualFold(u.Host, "github.com") || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(value, "%#") {
		return "", errors.New("GitHub CLI repository link must use HTTPS on github.com")
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 {
		return "", errors.New("GitHub CLI repository link is incomplete")
	}
	return parseRepositoryHint(parts[0] + "/" + parts[1])
}

func apiRepository(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || u.Opaque != "" || u.Fragment != "" || strings.IndexFunc(endpoint, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return "", errors.New("invalid GitHub API endpoint")
	}
	if (u.Scheme != "" || u.Host != "") && (u.Scheme != "https" || !strings.EqualFold(u.Host, "api.github.com")) {
		return "", errors.New("managed GitHub API requests must use api.github.com")
	}
	parts := strings.Split(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	if len(parts) >= 3 && parts[0] == "repos" {
		if u.Path == "/repos/{owner}/{repo}" || strings.HasPrefix(u.Path, "/repos/{owner}/{repo}/") ||
			u.Path == "repos/{owner}/{repo}" || strings.HasPrefix(u.Path, "repos/{owner}/{repo}/") {
			return "", nil
		}
		return parseRepositoryHint(parts[1] + "/" + parts[2])
	}
	return "", nil
}

type hintOutput struct {
	value     strings.Builder
	oversized bool
}

func (b *hintOutput) Write(value []byte) (int, error) {
	if b.value.Len()+len(value) > 4096 {
		b.oversized = true
	} else {
		_, _ = b.value.Write(value)
	}
	return len(value), nil
}
