package execution

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

const attemptSchemaVersion = 3

type attempt struct {
	SchemaVersion int                  `json:"schemaVersion"`
	OwnerID       string               `json:"ownerID"`
	Created       time.Time            `json:"created"`
	Ref           kubernetes.Reference `json:"ref"`
	SecretStarted bool                 `json:"secretStarted"`
	PodStarted    bool                 `json:"podStarted"`
}
type journal struct{ directory, owner string }

func openJournal(directory, owner string) (*journal, error) {
	if !wire.UUID(owner) || !filepath.IsAbs(directory) {
		return nil, errors.New("invalid attempt store owner")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	canonical, err := filepath.EvalSymlinks(directory)
	if err != nil || canonical != directory {
		return nil, errors.New("attempt store must be canonical")
	}
	j := &journal{directory: directory, owner: owner}
	_, err = j.readAll()
	return j, err
}
func (j *journal) validate(a attempt) error {
	r := a.Ref
	if !core.ValidSHA(r.PodDigest) {
		return errors.New("attempt Pod payload fingerprint required")
	}
	if a.SchemaVersion != attemptSchemaVersion || a.OwnerID != j.owner || a.Created.IsZero() || !wire.UUID(r.TaskID) || !wire.UUID(r.StorageID) || !wire.UUID(r.AttemptID) || r.Namespace == "" || r.Owner.Name == "" || r.Owner.UID == "" || !core.ValidSHA(r.RequestDigest) || r.PodName != "task-worker-"+r.StorageID || r.SecretName != "task-request-"+r.AttemptID || a.PodStarted && (!a.SecretStarted || r.SecretUID == "") || r.PodUID != "" && !a.PodStarted || r.SecretUID != "" && !a.SecretStarted {
		return errors.New("corrupt or foreign attempt journal; schema 3 required")
	}
	if err := r.RuntimeRef.Validate(); err != nil {
		return err
	}
	return nil
}
func (j *journal) save(a *attempt) error {
	if err := j.validate(*a); err != nil {
		return err
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if len(raw) > wire.MaxRequestBytes {
		return errors.New("attempt exceeds supported configuration metadata size")
	}
	return durableWrite(filepath.Join(j.directory, a.Ref.AttemptID+".json"), raw)
}
func (j *journal) read(id string) (result *attempt, err error) {
	if !wire.UUID(id) {
		return nil, errors.New("invalid attempt name")
	}
	name := filepath.Join(j.directory, id+".json")
	reason := "attempt_record_read_failed"
	defer func() {
		if err != nil {
			err = &diagnostics.Error{Reason: reason, Path: name, AttemptID: id, Cause: err}
		}
	}()
	st, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		reason = "attempt_record_not_regular"
		return nil, errors.New("attempt record is not regular")
	}
	raw, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	if len(raw) > wire.MaxRequestBytes {
		reason = "attempt_record_too_large"
		return nil, errors.New("attempt record too large")
	}
	reason = "attempt_record_invalid_json"
	var a attempt
	if err := runtimeimage.Decode(raw, &a); err != nil {
		return nil, err
	}
	if a.Ref.AttemptID != id {
		reason = "attempt_identity_mismatch"
		return nil, errors.New("attempt filename mismatch")
	}
	reason = "attempt_record_invalid"
	return &a, j.validate(a)
}

func (j *journal) readAll() ([]attempt, error) {
	entries, err := os.ReadDir(j.directory)
	if err != nil {
		return nil, diagnostics.AtPath("attempt_store_read_failed", j.directory, err)
	}
	var result []attempt
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".pending-") {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".json") || !wire.UUID(strings.TrimSuffix(entry.Name(), ".json")) {
			// Unrecognized names can contain caller data. A digest locates the
			// offending entry without treating its name as trusted task metadata.
			return nil, &diagnostics.Error{Reason: "attempt_entry_unknown", Path: j.directory, EntrySHA256: wire.Digest([]byte(entry.Name())), Cause: errors.New("unknown attempt data")}
		}
		a, err := j.read(strings.TrimSuffix(entry.Name(), ".json"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result = append(result, *a)
	}
	return result, nil
}
func (j *journal) remove(a *attempt) error {
	if _, err := j.read(a.Ref.AttemptID); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(j.directory, a.Ref.AttemptID+".json")); err != nil {
		return err
	}
	return syncDir(j.directory)
}
func syncDir(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
func durableWrite(destination string, data []byte) error {
	if st, err := os.Lstat(destination); err == nil && !st.Mode().IsRegular() {
		return errors.New("durable record is not a regular file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	name := filepath.Join(filepath.Dir(destination), ".pending-"+uuid.NewString())
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(name)
	_, err = f.Write(data)
	if err := errors.Join(err, f.Sync(), f.Close()); err != nil {
		return err
	}
	if err := os.Rename(name, destination); err != nil {
		return err
	}
	return syncDir(filepath.Dir(destination))
}
