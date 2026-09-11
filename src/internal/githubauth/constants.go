// Package githubauth connects Git and GitHub CLI requests to controller-owned
// GitHub App credentials. Workers only receive task-scoped installation tokens.
package githubauth

import "github.com/korioinc/multica-runtime-controller/internal/githubapp"

const (
	Route      = "/github/token"
	SocketPath = "/run/multica/github-auth.sock"
	EnabledEnv = "MULTICA_GITHUB_APP_AUTH"
)

// Request selects one repository, or the current task's complete GitHub scope.
// Only the task broker may expand an empty selection into an authorized scope.
type Request struct {
	Repository string `json:"repository,omitempty"`
}

// PrivateRequest is used only inside the controller's private socket boundary.
type PrivateRequest struct {
	Repositories []githubapp.Repository `json:"repositories"`
}
