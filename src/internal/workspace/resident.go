package workspace

import "time"

// admitResident runs inside the caller's journal transaction. A denial may
// drain the oldest idle session, but never grants another physical Pod slot.
func admitResident(st *registry, limit int, now time.Time) bool {
	resident, draining := 0, 0
	var oldest *WorkerSession
	for _, current := range st.Sessions {
		if current.State == SessionClosed && current.ResourcesCleaned {
			continue
		}
		resident++
		if current.State == SessionDraining || current.State == SessionClosed {
			draining++
		}
		if current.State == SessionIdle && (oldest == nil || current.IdleSince.Before(oldest.IdleSince) ||
			current.IdleSince.Equal(oldest.IdleSince) && current.ID < oldest.ID) {
			copy := current
			oldest = &copy
		}
	}
	for _, legacy := range st.Grants {
		// A live legacy preparation may still provision its original Pod.
		if legacy.WorkerSessionID == "" && legacy.StorageID != "" && !(legacy.State == "closed" && legacy.CleanupComplete) &&
			(!noWorkerCreated(legacy) || legacy.State == "intent" && legacy.Stop == nil) {
			resident++
			if legacy.Stop != nil || legacy.State == "closed" {
				draining++
			}
		}
	}
	if resident < limit {
		return true
	}
	if oldest != nil && draining < resident-limit+1 {
		requestSessionStop(st, oldest, "resident_capacity", now)
		st.Sessions[oldest.ID] = *oldest
	}
	return false
}
