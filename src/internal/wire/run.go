package wire

import (
	"encoding/json"

	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

// Run is prepared by the controller after binding the task and its workspace.
// Provider launch identity comes from the admitted image descriptor at the worker.
type Run struct {
	Provider       string                    `json:"provider"`
	Prompt         string                    `json:"prompt"`
	Options        agent.ExecOptions         `json:"options"`
	Environment    map[string]string         `json:"environment"`
	TaskConfig     json.RawMessage           `json:"taskConfig"`
	RuntimeBrief   string                    `json:"runtimeBrief"`
	NativeMetadata *workspace.NativeMetadata `json:"nativeMetadata,omitempty"`
}

// CheckoutRequest is the installed CLI's local daemon request.
type CheckoutRequest struct {
	URL          string `json:"url"`
	WorkspaceID  string `json:"workspace_id"`
	WorkDir      string `json:"workdir"`
	Ref          string `json:"ref"`
	AgentName    string `json:"agent_name"`
	TaskID       string `json:"task_id"`
	CheckoutMode string `json:"checkout_mode"`
	RetryBusy    bool   `json:"retry_busy"`
	Fresh        bool   `json:"fresh"`
}
