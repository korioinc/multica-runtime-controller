package workspace

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"
)

type Capability struct {
	WorkerSessionID string    `json:"workerSessionID,omitempty"`
	TurnSequence    uint64    `json:"turnSequence,omitempty"`
	Token           string    `json:"token"`
	AttemptID       string    `json:"attemptID"`
	Audience        string    `json:"audience"`
	Generation      uint64    `json:"generation"`
	PodUID          string    `json:"podUID,omitempty"`
	ExpiresAt       time.Time `json:"expiresAt"`
}

// IssueCapability persists the secret before returning it for a task Secret.
// BindPod later binds the initial capability to the observed Kubernetes UID.
func (s *Store) IssueCapability(id, audience string, expiry time.Time) (string, error) {
	if !validOpaque(audience) || !expiry.After(time.Now()) {
		return "", invalid("capability")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", err
	}
	token := "mtc_" + base64.RawURLEncoding.EncodeToString(secret)
	err := s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if (g.State != "intent" && g.State != "ready") || g.ExecutionRevoked {
			return ErrUnauthorized
		}
		if g.WorkerSessionID != "" && !currentSessionTurn(st, *g) {
			return ErrUnauthorized
		}
		for _, c := range st.Capabilities {
			if c.AttemptID == id && c.Audience == audience {
				return errors.New("capability audience already issued")
			}
		}
		st.Capabilities[digest([]byte(token))] = Capability{Token: token, AttemptID: id, Audience: audience, Generation: g.Generation, PodUID: g.PodUID, ExpiresAt: expiry.UTC(), WorkerSessionID: g.WorkerSessionID, TurnSequence: g.TurnSequence}
		return nil
	})
	if err != nil {
		return "", err
	}
	return token, nil
}
func (s *Store) CapabilityToken(id, audience string) (string, error) {
	st, err := s.snapshot()
	if err != nil {
		return "", err
	}
	for _, c := range st.Capabilities {
		if c.AttemptID == id && c.Audience == audience {
			return c.Token, nil
		}
	}
	return "", ErrUnauthorized
}
func (s *Store) Authorize(token, audience string) (TaskGrant, error) {
	grant, _, err := s.AuthorizeWithExpiry(token, audience)
	return grant, err
}

// AuthorizeWithExpiry binds a long-lived response to the same capability expiry
// used for its authorization decision.
func (s *Store) AuthorizeWithExpiry(token, audience string) (TaskGrant, time.Time, error) {
	if audience == "stop" {
		return TaskGrant{}, time.Time{}, ErrUnauthorized
	}
	st, err := s.snapshot()
	if err != nil {
		return TaskGrant{}, time.Time{}, err
	}
	c, ok := st.Capabilities[digest([]byte(token))]
	if !ok || c.Token != token || c.Audience != audience || !time.Now().Before(c.ExpiresAt) {
		return TaskGrant{}, time.Time{}, ErrUnauthorized
	}
	g, ok := st.Grants[c.AttemptID]
	if !ok || g.Generation != c.Generation || g.PodUID == "" || g.PodUID != c.PodUID {
		return TaskGrant{}, time.Time{}, ErrUnauthorized
	}
	if c.WorkerSessionID != g.WorkerSessionID || c.TurnSequence != g.TurnSequence || g.WorkerSessionID != "" && !currentSessionTurn(&st, g) {
		return TaskGrant{}, time.Time{}, ErrUnauthorized
	}
	if g.ExecutionRevoked && audience != "supervisor" {
		return TaskGrant{}, time.Time{}, ErrUnauthorized
	}
	storage := st.Storages[g.StorageID]
	if storage.ActiveAttempt != g.AttemptID || storage.Quarantined {
		return TaskGrant{}, time.Time{}, ErrUnauthorized
	}
	if audience == "supervisor" && g.State == "intent" {
		return g, c.ExpiresAt, nil
	}
	if len(g.SupervisorKey) == 0 {
		return TaskGrant{}, time.Time{}, ErrUnauthorized
	}
	switch g.State {
	case "assigned":
		if g.WorkerSessionID != "" && g.TurnAccepted && audience == "supervisor" {
			return g, c.ExpiresAt, nil
		}
	case "ready", "offered", "starting", "started":
		return g, c.ExpiresAt, nil
	case "terminal_received":
		if audience == "supervisor" {
			return g, c.ExpiresAt, nil
		}
	}
	return TaskGrant{}, time.Time{}, ErrUnauthorized
}

// AuthorizeStop grants only stop reporting access. Its caller exposes no run,
// task API, input, or credential operations, including for quarantined attempts.
func (s *Store) AuthorizeStop(token string) (TaskGrant, error) {
	st, err := s.snapshot()
	if err != nil {
		return TaskGrant{}, err
	}
	c, ok := st.Capabilities[digest([]byte(token))]
	if !ok || c.Token != token || c.Audience != "stop" || !time.Now().Before(c.ExpiresAt) {
		return TaskGrant{}, ErrUnauthorized
	}
	g, ok := st.Grants[c.AttemptID]
	if !ok || g.Generation != c.Generation || g.PodUID != c.PodUID || !stopGenerationCurrent(&st, g) {
		return TaskGrant{}, ErrUnauthorized
	}
	if c.WorkerSessionID != g.WorkerSessionID || c.TurnSequence != g.TurnSequence || g.WorkerSessionID != "" && !currentSessionTurn(&st, g) {
		return TaskGrant{}, ErrUnauthorized
	}
	if g.PodUID == "" {
		return g, ErrPodBindingPending
	}
	return g, nil
}
