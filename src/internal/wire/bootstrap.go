package wire

import (
	"errors"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"net"
	"net/url"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

const RelayURL = "http://127.0.0.1:9080"

// WorkerTerminationSeconds includes provider drain, writer shutdown, two
// in-flight event deliveries, receipt delivery, and a small kubelet margin.
func WorkerTerminationSeconds(grace int64) int64 { return 2*grace + 125 }

// Bootstrap contains only this attempt's authority. The official task envelope
// is offered separately by the controller after live Pod/workspace admission.
type Bootstrap struct {
	WorkerSessionID         string               `json:"workerSessionID,omitempty"`
	TurnSequence            uint64               `json:"turnSequence,omitempty"`
	WorkspaceAnchorTaskID   string               `json:"workspaceAnchorTaskID,omitempty"`
	OwnerID                 string               `json:"ownerID"`
	TaskID                  string               `json:"taskID"`
	AttemptID               string               `json:"attemptID"`
	StorageID               string               `json:"storageID"`
	RuntimeID               string               `json:"runtimeID"`
	WorkspaceID             string               `json:"workspaceID"`
	AgentID                 string               `json:"agentID"`
	Generation              uint64               `json:"generation"`
	TaskRoot                string               `json:"taskRoot"`
	NFSServer               string               `json:"nfsServer"`
	Provider                string               `json:"provider"`
	AllowedLinks            map[string]string    `json:"allowedLinks"`
	PreparedDigest          string               `json:"preparedDigest"`
	NativeMetadataDigest    string               `json:"nativeMetadataDigest,omitempty"`
	PVCName                 string               `json:"pvcName"`
	PVCUID                  string               `json:"pvcUID"`
	RuntimeRef              runtimeimage.Ref     `json:"runtimeRef"`
	GatewayURL              string               `json:"gatewayURL"`
	APICapability           string               `json:"apiCapability"`
	SupervisorCapability    string               `json:"supervisorCapability"`
	StopCapability          string               `json:"stopCapability"`
	CacheCapability         string               `json:"cacheCapability"`
	ExpiresAt               string               `json:"expiresAt"`
	Configuration           configuration.Bundle `json:"configuration"`
	Environment             []string             `json:"environment"`
	GitHubApp               bool                 `json:"githubApp"`
	TerminationGraceSeconds int                  `json:"terminationGraceSeconds"`
}

type SealCommand struct {
	RequestDigest string `json:"requestDigest"`
	Nonce         string `json:"nonce"`
	Cancel        bool   `json:"cancel"`
}

// StopCommand carries no execution authority. Revision and nonce identify one
// durable stop request, independently of any provider result.
type StopCommand struct {
	Cancel   bool   `json:"cancel"`
	Revision uint64 `json:"revision"`
	Nonce    string `json:"nonce"`
}

func (b Bootstrap) Validate() error {
	if b.Generation == 0 || !UUID(b.OwnerID) || !UUID(b.TaskID) || !UUID(b.AttemptID) || !UUID(b.StorageID) || !UUID(b.RuntimeID) || !UUID(b.WorkspaceID) || !UUID(b.AgentID) || b.PVCName == "" || b.PVCUID == "" || b.TerminationGraceSeconds < 1 {
		return errors.New("invalid worker bootstrap identity")
	}
	anchor := b.TaskID
	if b.WorkerSessionID != "" {
		if !UUID(b.WorkerSessionID) || b.TurnSequence == 0 || !UUID(b.WorkspaceAnchorTaskID) {
			return errors.New("invalid worker turn binding")
		}
		anchor = b.WorkspaceAnchorTaskID
	} else if b.TurnSequence != 0 || b.WorkspaceAnchorTaskID != "" {
		return errors.New("turn binding requires a worker session")
	}
	if workspace.ValidateTaskRoot(WorkspaceRoot, b.TaskRoot, b.WorkspaceID, anchor) != nil || net.ParseIP(b.NFSServer) == nil || !core.ValidSHA(b.PreparedDigest) || !workspace.SupportedProvider(b.Provider) {
		return errors.New("invalid prepared task mount")
	}
	if b.NativeMetadataDigest != "" && (b.WorkerSessionID == "" || !core.ValidSHA(b.NativeMetadataDigest)) {
		return errors.New("invalid native metadata binding")
	}
	u, err := url.Parse(b.GatewayURL)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("worker gateway requires a fixed HTTP origin")
	}
	for _, token := range []string{b.APICapability, b.SupervisorCapability, b.StopCapability, b.CacheCapability} {
		if len(token) < 32 || strings.ContainsAny(token, "\x00\r\n ") {
			return errors.New("invalid attempt capability")
		}
	}
	if err := b.RuntimeRef.Validate(); err != nil {
		return err
	}
	if err := b.Configuration.Validate(); err != nil {
		return err
	}
	if configuration.ExecutionDigest(b.Configuration, b.Environment) != b.RuntimeRef.ConfigurationDigest {
		return errors.New("configuration differs from admitted image reference")
	}
	return nil
}

func DecodeBootstrap(raw []byte) (Bootstrap, error) {
	var b Bootstrap
	if len(raw) > MaxRequestBytes {
		return b, errors.New("worker bootstrap exceeds Secret payload limit")
	}
	if err := runtimeimage.Decode(raw, &b); err != nil {
		return b, errors.New("invalid worker bootstrap")
	}
	return b, b.Validate()
}
