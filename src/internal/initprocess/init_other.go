//go:build !linux

package initprocess

// Run rejects init on platforms without Linux child-subreaper support.
func Run(args []string) int {
	selected := role(args)
	if selected == "" {
		return failure(reasonInvalidArguments, "", 0, exitFailure)
	}
	return failure(reasonUnsupported, selected, 0, exitFailure)
}
