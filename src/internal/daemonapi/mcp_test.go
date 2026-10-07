package daemonapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/multica-ai/multica/server/pkg/remotemcp"
	"github.com/multica-ai/multica/server/pkg/remotemcp/remotemcptest"
)

func TestMCPPreparationRejectsRequiredSchemaDrift(t *testing.T) {
	remote := remotemcptest.NewServer()
	defer remote.Close()
	t.Setenv(remotemcp.DevOriginsEnv, remote.URL)
	headers := http.Header{"Authorization": []string{"Bearer " + remotemcptest.Credential}}
	approved, _, err := remotemcp.Discover(context.Background(), remote.URL, nil, nil, headers)
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"credential_header": "Authorization", "credential": "Bearer " + remotemcptest.Credential})
	}))
	defer upstream.Close()
	client, err := NewClient(upstream.URL, "controller-fixture", fixtureRef().Daemon.Version, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	claim, _, _, _ := executionFixture(t, nil)
	var input map[string]any
	if err := json.Unmarshal(claim.Envelope, &input); err != nil {
		t.Fatal(err)
	}
	connections := []remotemcp.Connection{
		{InstallationID: "first-installation", ContributionID: "first-server", ContributionKey: "first", Endpoint: remote.URL, CredentialHeader: "Authorization", ApprovedTools: approved, FailurePolicy: "required"},
		{InstallationID: "second-installation", ContributionID: "second-server", ContributionKey: "second", Endpoint: remote.URL, CredentialHeader: "Authorization", ApprovedTools: slices.Clone(approved), FailurePolicy: "required"},
	}
	input["remote_mcp_daemon_token"] = "controller-mcp-fixture"
	input["remote_mcp_connections"] = connections
	claim.Envelope, _ = json.Marshal(input)
	claim, err = ParseClaim(claim.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PrepareMCP(context.Background(), claim); err != nil {
		t.Fatal("approved remote tools were not admitted", err)
	}
	// One successful discovery must not authorize a different required tool
	// whose current schema no longer matches the approved capability.
	connections[1].ApprovedTools[0].InputSchema = json.RawMessage(`{"type":"object","required":["changed-permission"]}`)
	connections[1].ApprovedTools[0].SchemaDigest = remotemcp.DigestBytes(connections[1].ApprovedTools[0].InputSchema)
	claim.Envelope, _ = json.Marshal(input)
	if _, err := client.PrepareMCP(context.Background(), claim); err == nil {
		t.Fatal("a changed required tool acquired task execution authority")
	}
}
