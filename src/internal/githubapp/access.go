package githubapp

import (
	"context"
	"errors"
	"fmt"
)

// Access couples current repository authority with a stable cache identity.
// Identity contains no credential and survives installation token renewal.
type Access struct {
	Token    Token
	Identity string
}

// RepositoryAccess rechecks App membership and token access on every call,
// including cache hits. A recreated repository or installation gets a new key.
func (m *Manager) RepositoryAccess(ctx context.Context, repository Repository) (Access, error) {
	repository, err := canonicalRepository(repository)
	if err != nil {
		return Access{}, err
	}
	token, err := m.Token(ctx, []Repository{repository})
	if err != nil {
		return Access{}, err
	}
	installation, _, err := m.installation(ctx, repository)
	if err != nil {
		return Access{}, err
	}
	if token.installationID != installation.ID {
		return Access{}, errors.New("GitHub App installation changed during authorization")
	}
	identity, err := m.repositoryIdentity(ctx, repository, token)
	if err != nil {
		return Access{}, err
	}
	if identity.ID <= 0 {
		return Access{}, errors.New("GitHub repository immutable identity is missing")
	}
	return Access{
		Token:    token,
		Identity: fmt.Sprintf("github-app:%s:installation:%d:repository:%d", m.appID, installation.ID, identity.ID),
	}, nil
}
