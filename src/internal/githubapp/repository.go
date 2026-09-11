package githubapp

import (
	"errors"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

// Repository identifies a repository within its GitHub installation owner.
type Repository struct {
	Owner string
	Name  string
}

var (
	ownerPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$`)
	repoPattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)
)

// ParseRepository parses an owner/repository name, without a URL or Git suffix.
func ParseRepository(value string) (Repository, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return Repository{}, errors.New("GitHub repository must have an owner and name")
	}
	return canonicalRepository(Repository{Owner: parts[0], Name: parts[1]})
}

// ParseRepositoryURL accepts only unambiguous HTTPS github.com repository URLs.
// Credentials, encoded paths, query strings and fragments are never accepted.
func ParseRepositoryURL(value string) (Repository, error) {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || !strings.EqualFold(u.Host, "github.com") ||
		u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery ||
		u.Fragment != "" || strings.ContainsAny(value, "#%") || u.RawPath != "" {
		return Repository{}, errors.New("GitHub repository URL must use HTTPS on github.com without credentials, encoding, query or fragment")
	}
	path := strings.TrimSuffix(strings.TrimPrefix(u.Path, "/"), "/")
	if strings.HasSuffix(strings.ToLower(path), ".git") {
		path = path[:len(path)-4]
	}
	return ParseRepository(path)
}

func canonicalRepository(repository Repository) (Repository, error) {
	if !ownerPattern.MatchString(repository.Owner) || !repoPattern.MatchString(repository.Name) ||
		repository.Name == "." || repository.Name == ".." {
		return Repository{}, errors.New("invalid GitHub repository owner or name")
	}
	repository.Owner = strings.ToLower(repository.Owner)
	repository.Name = strings.ToLower(repository.Name)
	return repository, nil
}

func canonicalScope(repositories []Repository) ([]Repository, string, error) {
	if len(repositories) == 0 || len(repositories) > 500 {
		return nil, "", errors.New("GitHub token scope requires between 1 and 500 repositories")
	}
	unique := make(map[string]Repository, len(repositories))
	owner := ""
	for _, repository := range repositories {
		canonical, err := canonicalRepository(repository)
		if err != nil {
			return nil, "", err
		}
		if owner != "" && owner != canonical.Owner {
			return nil, "", errors.New("GitHub token scope cannot span installation owners; request each repository separately")
		}
		owner = canonical.Owner
		unique[canonical.Owner+"/"+canonical.Name] = canonical
	}
	keys := make([]string, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	scope := make([]Repository, 0, len(keys))
	for _, key := range keys {
		scope = append(scope, unique[key])
	}
	return scope, strings.Join(keys, ","), nil
}
