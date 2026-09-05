package intercept

import (
	"net/http"
	"path"
	"strings"
)

const maxDaemonCheckoutBody = 1 << 20

func prepareDaemonRequest(w http.ResponseWriter, r *http.Request) bool {
	// The daemon uses ServeMux, which unescapes and redirects some paths.
	// Refuse alternate spellings so its route selection cannot bypass ours.
	if r.URL.Opaque != "" || r.URL.Path == "" || !strings.HasPrefix(r.URL.Path, "/") ||
		r.URL.RawPath != "" || path.Clean(r.URL.Path) != r.URL.Path || strings.Contains(r.URL.Path, "\\") {
		http.Error(w, "ambiguous daemon request path", http.StatusBadRequest)
		return false
	}
	if (r.Method == http.MethodPost && r.URL.Path == "/repo/checkout") || (r.Method == http.MethodGet && r.URL.Path == "/health") {
		return true
	}
	http.Error(w, "daemon operation is not available to task workers", http.StatusForbidden)
	return false
}
