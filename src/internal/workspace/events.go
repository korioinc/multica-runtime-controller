package workspace

import (
	"bytes"
	"encoding/json"
	"errors"
)

type Event struct {
	Sequence         uint64          `json:"sequence"`
	UpstreamSequence uint64          `json:"upstreamSequence"`
	Body             json.RawMessage `json:"body"`
	State            string          `json:"state"`
}

// ReceiveEvent owns the redacted event before acknowledging its sequence.
func (s *Store) ReceiveEvent(id string, sequence uint64, body json.RawMessage) (Event, error) {
	if sequence == 0 || !json.Valid(body) || int64(len(body)) > s.options.MaxRecordBytes {
		return Event{}, invalid("provider event")
	}
	body = compactJSON(body)
	var event Event
	err := s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.State != "started" {
			return ErrConflict
		}
		if sequence <= uint64(len(g.Events)) {
			event = g.Events[sequence-1]
			if !bytes.Equal(event.Body, body) {
				return ErrConflict
			}
			return nil
		}
		if sequence != uint64(len(g.Events))+1 {
			return ErrConflict
		}
		var size int64
		for _, prior := range g.Events {
			size += int64(len(prior.Body))
		}
		if size+int64(len(body)) > s.options.MaxRecordBytes {
			return errors.New("provider event journal budget exhausted")
		}
		storage := st.Storages[g.StorageID]
		storage.EventSequence++
		if storage.EventSequence == 0 {
			return ErrConflict
		}
		event = Event{Sequence: sequence, UpstreamSequence: storage.EventSequence, Body: body, State: "received"}
		g.Events = append(g.Events, event)
		st.Storages[g.StorageID] = storage
		return nil
	})
	return event, err
}

func (s *Store) EventDelivery(id string, sequence uint64, from, to string) error {
	return s.updateGrant(id, func(_ *registry, g *TaskGrant) error {
		if sequence == 0 || sequence > uint64(len(g.Events)) {
			return ErrConflict
		}
		e := &g.Events[sequence-1]
		if e.State != from || !(from == "received" && (to == "forwarding" || to == "local") || from == "forwarding" && (to == "received" || to == "delivered" || to == "uncertain")) {
			return ErrConflict
		}
		e.State = to
		return nil
	})
}
