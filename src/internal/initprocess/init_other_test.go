//go:build !linux

package initprocess

import "testing"

func TestInitUnsupportedPlatform(t *testing.T) {
	for _, args := range [][]string{{"controller"}, {"worker", "run", "private-socket"}, {"init", "controller"}} {
		if code := Run(args); code != exitFailure {
			t.Fatalf("Run(%v) = %d, want unsupported failure", args, code)
		}
	}
}
