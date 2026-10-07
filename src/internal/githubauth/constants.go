// Package githubauth connects Git and GitHub CLI requests to controller-owned
// GitHub App credentials. Workers only receive task-scoped installation tokens.
package githubauth

const Route = "/github/token"

// Request selects one repository, or the current task's complete GitHub scope.
// Only the task broker may expand an empty selection into an authorized scope.
type Request struct {
	Repository string `json:"repository,omitempty"`
}
