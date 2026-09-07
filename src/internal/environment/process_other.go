//go:build !linux

package environment

import "syscall"

// Production images are Linux. This fallback permits native developer checks;
// escaped process-session supervision is verified by the Linux fixture.
func beginSupervision() (map[int]bool, error) { return map[int]bool{}, nil }
func supervisedChildren(root int, _, _ map[int]bool) []int {
	if syscall.Kill(-root, 0) == nil {
		return []int{root}
	}
	return nil
}
func reapChildren(_ []int, _ int) {}
