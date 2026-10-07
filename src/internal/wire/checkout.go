package wire

import "time"

const (
	CheckoutTaskAuthorizationHeader = "X-Multica-Task-Authorization"
	CheckoutTimeout                 = 10 * time.Minute
)

// CheckoutResult identifies the independent checkout already visible over NFS.
type CheckoutResult struct {
	Path       string `json:"path"`
	BranchName string `json:"branch_name"`
	Kept       string `json:"kept,omitempty"`
}
