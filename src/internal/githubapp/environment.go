package githubapp

import "strings"

// ControllerEnvironmentKey identifies App configuration that must remain in the
// controller and must not be inherited by the official daemon or task workers.
func ControllerEnvironmentKey(name string) bool {
	name = strings.TrimPrefix(strings.ToUpper(name), "MULTICA_OPERATOR_")
	return strings.HasPrefix(name, "GITHUB_APP_") || name == "GITHUB_WEBHOOK_SECRET"
}
