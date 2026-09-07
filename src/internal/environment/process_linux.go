//go:build linux

package environment

import (
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func processParents() map[int]int {
	result := map[int]int{}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return result
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + entry.Name() + "/status")
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "PPid:") {
				ppid, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "PPid:")))
				result[pid] = ppid
				break
			}
		}
	}
	return result
}
func beginSupervision() (map[int]bool, error) {
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return nil, err
	}
	baseline := map[int]bool{}
	for pid, ppid := range processParents() {
		if ppid == os.Getpid() {
			baseline[pid] = true
		}
	}
	return baseline, nil
}
func supervisedChildren(root int, baseline, tracked map[int]bool) []int {
	parents := processParents()
	selected := map[int]bool{root: true}
	for pid := range tracked {
		selected[pid] = true
	}
	for pid, ppid := range parents {
		if ppid == os.Getpid() && !baseline[pid] {
			selected[pid] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for pid, ppid := range parents {
			if selected[ppid] && !selected[pid] {
				selected[pid] = true
				changed = true
			}
		}
	}
	out := []int{}
	for pid := range selected {
		if _, ok := parents[pid]; ok {
			out = append(out, pid)
		}
	}
	return out
}
func reapChildren(pids []int, main int) {
	for _, pid := range pids {
		if pid == main {
			continue
		}
		var status unix.WaitStatus
		_, _ = unix.Wait4(pid, &status, unix.WNOHANG, nil)
	}
}
