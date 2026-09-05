package intercept

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
)

const maxDaemonCheckoutBody = 1 << 20

// prepareDaemonRequest permits only checkout planning in the trusted daemon.
// Git must run in the worker's mount namespace, including repeated checkouts
// of worker-owned metadata. A worker can send HTTP directly to this gateway.
func prepareDaemonRequest(w http.ResponseWriter, r *http.Request) bool {
	// The daemon uses ServeMux, which unescapes and redirects some paths.
	// Refuse alternate spellings so its route selection cannot bypass ours.
	if r.URL.Opaque != "" || r.URL.Path == "" || !strings.HasPrefix(r.URL.Path, "/") ||
		r.URL.RawPath != "" || path.Clean(r.URL.Path) != r.URL.Path || strings.Contains(r.URL.Path, "\\") {
		http.Error(w, "ambiguous daemon request path", http.StatusBadRequest)
		return false
	}
	if r.Method != http.MethodPost || r.URL.Path != "/repo/checkout" {
		return true
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxDaemonCheckoutBody)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "invalid or oversized checkout request", http.StatusBadRequest)
		return false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		http.Error(w, "checkout request must be a JSON object", http.StatusBadRequest)
		return false
	}
	// encoding/json matches struct fields case-insensitively. Leaving an alias
	// such as CHECKOUT_MODE could override the canonical field on the daemon.
	for key := range fields {
		if strings.EqualFold(key, "checkout_mode") {
			delete(fields, key)
		}
	}
	fields["checkout_mode"] = json.RawMessage(`"worker-plan"`)
	body, err := json.Marshal(fields)
	if err != nil {
		http.Error(w, "encode worker checkout plan request", http.StatusInternalServerError)
		return false
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.TransferEncoding = nil
	r.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return true
}
