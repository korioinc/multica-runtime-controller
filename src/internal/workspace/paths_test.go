package workspace

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestReadableTaskNamesPreserveOwnershipAndFrozenStorage(t *testing.T) {
	s, options := testStore(t)
	defer func() { _ = s.Close() }()
	input := testGrant()
	input.WorkspaceID = "11111111-1111-4111-8111-abcdef123456"
	input.TaskID = "11111111-1111-4111-8111-123456abcdef"
	input.Envelope = json.RawMessage(`{"workspace_slug":"KOR / IO","issue_identifier":"KOR / 219"}`)
	first, err := s.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	other := input
	other.TaskID = "22222222-2222-4222-8222-123456abcdef"
	other.Envelope = json.RawMessage(`{"workspace_slug":"kor-io","issue_identifier":"kor-219"}`)
	if _, err := s.Create(other); !errors.Is(err, ErrConflict) {
		t.Fatal("normalized labels and a shortened UUID collision shared task storage", err)
	}
	other.WorkspaceID = "22222222-2222-4222-8222-abcdef123456"
	if _, err := s.Create(other); !errors.Is(err, ErrConflict) {
		t.Fatal("another workspace acquired task storage through shortened UUID collisions", err)
	}
	if _, err := s.RequestStop(first.AttemptID, "cancelled_before_preparation"); err != nil {
		t.Fatal(err)
	}
	if err := s.ObserveStop(first.AttemptID, StopEvidence{Kind: "no-worker", PVCUID: first.PVCUID, ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseStopped(first.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkCleaned(first.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	input.Envelope = json.RawMessage(`{"workspace_slug":"renamed-workspace","issue_identifier":"KOR-220"}`)
	retry, err := s.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	if retry.StorageID != first.StorageID || retry.TaskRoot != first.TaskRoot || retry.Generation <= first.Generation {
		t.Fatal("renamed labels abandoned owned task storage or execution generation")
	}
}
