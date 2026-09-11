package githubauth

import (
	"context"
	"strings"
	"testing"
)

func TestGHRefusesConflictingRepositoryAuthority(t *testing.T) {
	_, err := resolveGHRepository(context.Background(), []string{
		"api", "repos/team/destination/issues", "--repo", "team/another-repository",
	}, nil, t.TempDir())
	if err == nil {
		t.Fatal("conflicting CLI targets could choose an unrelated repository grant")
	}
}

func TestGHExplicitRepositoryOverridesUnrelatedDefaults(t *testing.T) {
	selected, err := resolveGHRepository(context.Background(), []string{
		"pr", "view", "https://github.com/team/selected/pull/123", "-R", "team/selected",
	}, []string{"GH_REPO=team/unrelated-default"}, t.TempDir())
	if err != nil || selected != "https://github.com/team/selected.git" {
		t.Fatal("the requested repository did not own the credential selection")
	}
}

func TestGHDoesNotSelectCredentialDestinationsOnOtherHosts(t *testing.T) {
	secret := "untrusted-url-password"
	for _, args := range [][]string{
		{"api", "user", "--hostname", "other.example"},
		{"api", "--hostname=other.example", "user"},
		{"auth", "status", "-hother.example"},
		{"api", "https://other.example/repos/team/repository"},
		{"api", "https://user:" + secret + "@api.github.com/repos/team/repository"},
		{"repo", "view", "other.example/team/repository"},
	} {
		_, err := resolveGHRepository(context.Background(), args, nil, t.TempDir())
		if err == nil {
			t.Fatal("an external host was accepted as a GitHub credential destination")
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatal("a refused URL disclosed its credential in an error")
		}
	}
}
