package workspace

import (
	"errors"
	"path/filepath"
	"regexp"
	"strings"
)

var pathSeparators = regexp.MustCompile(`[^a-z0-9]+`)

// TaskRoot follows the pinned CLI's PredictRootDir/readablePathSegment rule.
// The complete UUIDs, not these shortened names, establish journal ownership.
func TaskRoot(workspacesRoot, workspaceID, taskID, workspaceSlug, issueIdentifier string) (string, error) {
	if !canonicalPath(workspacesRoot) || !canonicalUUID(workspaceID) || !canonicalUUID(taskID) {
		return "", errors.New("canonical workspace path and complete UUIDs required")
	}
	return filepath.Join(workspacesRoot, readablePathSegment(workspaceSlug, "workspace", workspaceID), readablePathSegment(issueIdentifier, "task", taskID)), nil
}

func readablePathSegment(label, fallback, id string) string {
	prefix := strings.ToLower(strings.TrimSpace(label))
	prefix = strings.Trim(pathSeparators.ReplaceAllString(prefix, "-"), "-")
	if prefix == "" {
		prefix = fallback
	}
	suffix := id[len(id)-12:]
	// The native CLI limits a readable path segment to 24 ASCII characters.
	if limit := 24 - len(suffix) - 1; len(prefix) > limit {
		prefix = strings.TrimRight(prefix[:limit], "-")
	}
	return prefix + "-" + suffix
}

// ValidateTaskRoot checks a frozen path without deriving it from a task's
// current display label. Callers also bind it to the full journal/Pod identity.
func ValidateTaskRoot(workspacesRoot, root, workspaceID, taskID string) error {
	if !canonicalPath(root) || !canonicalUUID(workspaceID) || !canonicalUUID(taskID) {
		return errors.New("invalid task root")
	}
	suffix := "-" + taskID[len(taskID)-12:]
	workspaceSuffix := "-" + workspaceID[len(workspaceID)-12:]
	leaf := filepath.Base(root)
	parent := filepath.Base(filepath.Dir(root))
	if !strings.HasSuffix(leaf, suffix) || !strings.HasSuffix(parent, workspaceSuffix) {
		return errors.New("task root does not match task identity")
	}
	expected, err := TaskRoot(workspacesRoot, workspaceID, taskID, strings.TrimSuffix(parent, workspaceSuffix), strings.TrimSuffix(leaf, suffix))
	if err != nil || expected != root {
		return errors.New("task root is not a canonical native path")
	}
	return nil
}
