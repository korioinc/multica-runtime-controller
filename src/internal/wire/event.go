package wire

import (
	"github.com/multica-ai/multica/server/pkg/agent"
	"github.com/multica-ai/multica/server/pkg/redact"
)

type ProviderEvent struct {
	Sequence uint64                      `json:"sequence"`
	Message  *agent.Message              `json:"message,omitempty"`
	Usage    map[string]agent.TokenUsage `json:"usage,omitempty"`
}

func (e ProviderEvent) Redacted() ProviderEvent {
	if e.Message != nil {
		m := *e.Message
		m.Input = redact.InputMap(m.Input)
		m.Content, m.Output = redact.Text(m.Content), redact.Text(m.Output)
		e.Message = &m
	}
	return e
}

type ProviderResult struct {
	Result agent.Result `json:"result"`
}
