package worker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type enrollmentTransport func(*http.Request) (*http.Response, error)

func (f enrollmentTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSessionEnrollmentRetriesOnlyPendingObservation(t *testing.T) {
	for _, scenario := range []string{"pending then admitted", "identity denied", "cancelled while pending"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				bootstrap := wire.SessionBootstrap{WorkerSessionID: uuid.NewString(), GatewayURL: "http://controller.local", ControlCapability: "enrollment-test-token"}
				identity := wire.SessionAdmission{WorkerSessionID: bootstrap.WorkerSessionID, PodUID: uuid.NewString(),
					PVCUID: uuid.NewString(), PublicKey: make(ed25519.PublicKey, ed25519.PublicKeySize)}
				wantBody, err := json.Marshal(identity)
				if err != nil {
					t.Fatal(err)
				}
				calls := 0
				client := &http.Client{Transport: enrollmentTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					body, err := io.ReadAll(r.Body)
					_ = r.Body.Close()
					if err != nil || !bytes.Equal(body, wantBody) || r.Method != http.MethodPost ||
						r.URL.Path != "/internal/worker-sessions/"+bootstrap.WorkerSessionID+"/admit" ||
						r.Header.Get("Authorization") != "Bearer "+bootstrap.ControlCapability {
						t.Fatal("retry changed the original enrollment identity", err)
					}
					status := http.StatusServiceUnavailable
					if scenario == "identity denied" {
						status = http.StatusForbidden
					} else if scenario == "cancelled while pending" {
						cancel()
					} else if calls == 2 {
						status = http.StatusOK
					}
					return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
				})}
				gateway := sessionGateway{bootstrap: bootstrap, identity: identity, client: client}
				err = gateway.enroll(ctx)
				switch scenario {
				case "pending then admitted":
					if err != nil || calls != 2 {
						t.Fatal("pending observation terminated enrollment", err, calls)
					}
				case "identity denied":
					if err == nil || errors.Is(err, errGatewayUnavailable) || calls != 1 {
						t.Fatal("identity denial was retried as pending observation", err, calls)
					}
				case "cancelled while pending":
					if !errors.Is(err, context.Canceled) || calls != 1 {
						t.Fatal("enrollment retry ignored cancellation", err, calls)
					}
				}
			})
		})
	}
}
