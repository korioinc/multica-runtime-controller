package worker

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/multica-ai/multica/server/pkg/agent"
)

// This input forwards provider execution configuration, not bootstrap
// capabilities, result signing keys or workspace authority.
type runnerRequest struct {
	Provider       string
	ExecutablePath string
	CLIVersion     string
	Environment    map[string]string
	Prompt         string
	Options        agent.ExecOptions
	TaskID         string
	RuntimeID      string
	DaemonVersion  string
	CodexVersion   string
}

type runnerFrame struct {
	Kind    string         `json:"kind"`
	Message *agent.Message `json:"message,omitempty"`
	Result  *agent.Result  `json:"result,omitempty"`
}

const runnerInputLimit = 16 << 20
const runnerFrameLimit = 64 * wire.MaxRequestBytes

func writeRunnerPacket(w io.Writer, value any, limit uint32) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(raw) == 0 || uint64(len(raw)) > uint64(limit) {
		return errors.New("provider runner packet exceeds limit")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(raw)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err = w.Write(raw)
	return err
}

func readRunnerPacket(r io.Reader, value any, limit uint32) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > limit {
		return errors.New("provider runner packet exceeds limit")
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return err
	}
	return json.Unmarshal(raw, value)
}
