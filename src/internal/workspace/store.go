// Package workspace owns controller-only task authority and durable terminal intent.
package workspace

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// SchemaVersion covers the whole persisted journal, including nested task records.
const SchemaVersion = 16

const preparationDirectoryName = "preparation"

var (
	ErrAdmissionBudget   = errors.New("task admission budget exhausted; existing data retained")
	ErrStorageBusy       = errors.New("task storage has an active or uncertain writer")
	ErrUnauthorized      = errors.New("task capability is unavailable")
	ErrPodBindingPending = errors.New("worker Pod identity has not been observed")
	ErrConflict          = errors.New("task authority conflicts with durable state")
	ErrResidentCapacity  = errors.New("resident Pod capacity is reserved until termination and cleanup")
)

type Options struct {
	Directory, OwnerID            string
	MaxRecordBytes, MaxStoreBytes int64
	MaxTasks, MaxPendingResults   int
	MaxPendingResultBytes         int64
	// Now supplies the controller clock. Time is sampled after acquiring the journal lock.
	Now func() time.Time
}

type Store struct {
	attemptGates      sync.Map
	options           Options
	mu                sync.Mutex
	lock              *os.File
	failed            error
	committed         registry
	journalInfo       os.FileInfo
	controlChallenges map[string]map[string]SessionChallenge
}

// PreparationDirectory keeps installed-helper scratch on the controller's
// private PVC. Scratch contents carry no journal or workspace authority.
func (s *Store) PreparationDirectory() string {
	return filepath.Join(s.options.Directory, preparationDirectoryName)
}

// ExecutionGate orders a terminal against this attempt's in-flight task APIs,
// while allowing independent task requests and event persistence to progress.
func (s *Store) ExecutionGate(id string) *sync.RWMutex {
	gate, _ := s.attemptGates.LoadOrStore(id, new(sync.RWMutex))
	return gate.(*sync.RWMutex)
}

type registry struct {
	SchemaVersion  int                      `json:"schemaVersion"`
	OwnerID        string                   `json:"ownerID"`
	Grants         map[string]TaskGrant     `json:"grants"`
	Storages       map[string]Storage       `json:"storages"`
	NFSServer      string                   `json:"nfsServer"`
	WorkspaceClaim string                   `json:"workspaceClaim"`
	WorkspaceUID   string                   `json:"workspaceUID"`
	Terminals      map[string]Terminal      `json:"terminals"`
	Capabilities   map[string]Capability    `json:"capabilities"`
	Conversations  map[string]Conversation  `json:"conversations"`
	Sessions       map[string]WorkerSession `json:"sessions"`
}

func (s *Store) now() time.Time {
	if s.options.Now != nil {
		return s.options.Now().UTC()
	}
	return time.Now().UTC()
}

// Open holds a nonblocking process-wide lock until Close. Losing a worker is
// never equivalent to acquiring this controller metadata lock.
func Open(options Options) (*Store, error) {
	if !filepath.IsAbs(options.Directory) || filepath.Clean(options.Directory) != options.Directory || !canonicalUUID(options.OwnerID) {
		return nil, errors.New("controller store requires canonical directory and owner UUID")
	}
	if options.MaxTasks == 0 {
		options.MaxTasks = 10000
	}
	if options.MaxPendingResults == 0 {
		options.MaxPendingResults = 1000
	}
	if options.MaxPendingResultBytes == 0 {
		options.MaxPendingResultBytes = 32 << 20
	}
	if options.MaxTasks < 1 || options.MaxPendingResults < 1 || options.MaxPendingResultBytes < 1 {
		return nil, errors.New("invalid admission budget")
	}
	if options.MaxRecordBytes == 0 {
		options.MaxRecordBytes = 8 << 20
	}
	if options.MaxStoreBytes == 0 {
		options.MaxStoreBytes = 64 << 20
	}
	if options.MaxRecordBytes < 1 || options.MaxStoreBytes < options.MaxRecordBytes {
		return nil, errors.New("invalid controller storage budget")
	}
	if err := os.MkdirAll(options.Directory, 0700); err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(options.Directory)
	if err != nil || resolved != options.Directory {
		return nil, errors.New("controller store directory contains a symlink")
	}
	info, err := os.Stat(options.Directory)
	if err != nil || info.Mode().Perm()&0007 != 0 {
		return nil, errors.New("controller store directory must deny world access")
	}
	fd, err := unix.Open(filepath.Join(options.Directory, "controller.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "controller.lock")
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("controller metadata already has an active owner")
	}
	s := &Store{options: options, lock: lock}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()
	entries, err := os.ReadDir(options.Directory)
	if err != nil {
		return nil, err
	}
	exists := false
	for _, e := range entries {
		if e.Name() == preparationDirectoryName && e.IsDir() {
			root, err := os.OpenRoot(options.Directory)
			if err != nil {
				return nil, err
			}
			preparation, err := OpenNativeDirectory(root, preparationDirectoryName)
			root.Close()
			if err != nil {
				return nil, err
			}
			info, statErr := preparation.Stat(".")
			preparation.Close()
			if statErr != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
				return nil, errors.New("controller preparation directory must remain private")
			}
			continue
		}
		if e.Name() == "journal.json" && e.Type().IsRegular() {
			exists = true
			continue
		}
		if e.Name() == "controller.lock" && e.Type().IsRegular() {
			continue
		}
		// A crashed publication may leave an uncommitted temporary file. It carries
		// no authority and is retained, never adopted or automatically deleted.
		if len(e.Name()) > len(".pending-") && e.Name()[:len(".pending-")] == ".pending-" && e.Type().IsRegular() {
			continue
		}
		return nil, errors.New("populated metadata is not a current controller journal; use fresh storage")
	}
	if !exists {
		if len(entries) > 1 {
			return nil, errors.New("unpublished metadata requires operator recovery")
		}
		st := registry{SchemaVersion: SchemaVersion, OwnerID: options.OwnerID, Grants: map[string]TaskGrant{}, Storages: map[string]Storage{}, Terminals: map[string]Terminal{}, Capabilities: map[string]Capability{}, Conversations: map[string]Conversation{}, Sessions: map[string]WorkerSession{}}
		if err := s.write(st); err != nil {
			return nil, err
		}
	} else {
		st, err := s.readJournal()
		if err != nil {
			return nil, err
		}
		changed := st.SchemaVersion != SchemaVersion
		st.SchemaVersion = SchemaVersion
		for id, t := range st.Terminals {
			if t.State == "forwarding" {
				t.State = "uncertain"
				st.Terminals[id] = t
				changed = true
			}
			if t.RecoveryFailure != nil && t.RecoveryFailure.State == "forwarding" {
				t.RecoveryFailure.State = "uncertain"
				st.Terminals[id] = t
				changed = true
			}
		}
		for id, g := range st.Grants {
			for i := range g.Events {
				if g.Events[i].State == "forwarding" {
					g.Events[i].State = "uncertain"
					changed = true
				}
			}
			st.Grants[id] = g
		}
		if changed {
			if err := s.checkJournal(); err != nil {
				return nil, err
			}
			if err := s.write(st); err != nil {
				return nil, err
			}
		} else {
			if err := syncDirectory(options.Directory); err != nil {
				return nil, err
			}
			if err := s.checkJournal(); err != nil {
				return nil, err
			}
			s.committed = cloneRegistry(st)
		}
	}
	ok = true
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	err := s.lock.Close()
	s.lock = nil
	s.committed = registry{}
	s.journalInfo = nil
	s.controlChallenges = nil
	return err
}

// The exclusive owner reads validated committed state from memory. A candidate
// becomes visible only after its complete journal publication is durable.
func (s *Store) transaction(fn func(*registry) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return errors.New("controller store is closed")
	}
	if s.failed != nil {
		return errors.New("controller store publication failed; reconciliation requires restart")
	}
	if err := s.checkJournal(); err != nil {
		return err
	}
	st := cloneRegistry(s.committed)
	if err := fn(&st); err != nil {
		return err
	}
	if reflect.DeepEqual(st, s.committed) {
		return nil
	}
	return s.write(st)
}

func (s *Store) snapshot() (registry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || s.failed != nil {
		return registry{}, errors.New("controller store is unavailable")
	}
	if err := s.checkJournal(); err != nil {
		return registry{}, err
	}
	return cloneRegistry(s.committed), nil
}

// External changes are never adopted by the live owner. Reopening performs the
// full integrity and recovery checks before the journal can authorize work.
func (s *Store) checkJournal() error {
	info, err := os.Lstat(filepath.Join(s.options.Directory, "journal.json"))
	if err != nil || s.journalInfo == nil || !os.SameFile(info, s.journalInfo) ||
		info.Size() != s.journalInfo.Size() || info.Mode() != s.journalInfo.Mode() ||
		!info.ModTime().Equal(s.journalInfo.ModTime()) {
		s.failed = errors.New("controller journal changed outside its owner")
		return s.failed
	}
	return nil
}

func (s *Store) readJournal() (registry, error) {
	var st registry
	fd, err := unix.Open(filepath.Join(s.options.Directory, "journal.json"), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return st, err
	}
	f := os.NewFile(uintptr(fd), "journal.json")
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0007 != 0 || info.Size() > s.options.MaxStoreBytes {
		return st, errors.New("invalid controller journal file or budget")
	}
	raw, err := io.ReadAll(io.LimitReader(f, s.options.MaxStoreBytes+1))
	if err != nil {
		return st, err
	}
	var header struct {
		SchemaVersion int `json:"schemaVersion"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return st, err
	}
	if header.SchemaVersion != SchemaVersion && !(header.SchemaVersion >= 6 && header.SchemaVersion <= 15) {
		return st, fmt.Errorf("unsupported journal schema %d; schemas 6 through 15 may migrate to %d, earlier authority requires offline recovery", header.SchemaVersion, SchemaVersion)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&st); err != nil {
		return st, err
	}
	if dec.Decode(new(any)) != io.EOF {
		return st, errors.New("invalid controller journal suffix")
	}
	if st.SchemaVersion <= 13 {
		if len(st.Sessions) != 0 || len(st.Conversations) != 0 {
			return st, invalid("previous conversation authority")
		}
		for _, g := range st.Grants {
			if g.Legacy || g.SessionProtocol || g.WorkerSessionID != "" || g.Conversation != (ConversationKey{}) || g.TurnSequence != 0 ||
				g.WorkspaceAnchorTaskID != "" || g.CompatibilityDigest != "" || g.WorkspaceCompatibilityDigest != "" || g.Compatibility != nil || len(g.Assignment) != 0 || g.InputDigest != "" ||
				g.TurnAccepted || g.TurnComplete || g.TurnReceipt != nil || g.TurnExecutionReceipt != nil || g.TurnExecutionProof != nil || g.AcceptProof != nil || g.CompletionWitness != nil || g.Selection != nil || g.BackendSelection != nil || g.PendingResume != nil {
				return st, invalid("previous turn authority")
			}
			if g.Prepared != nil && (g.Prepared.Conversation != nil || g.Prepared.WorkspaceAnchorTaskID != "") {
				return st, invalid("previous native anchor authority")
			}
		}
		for _, storage := range st.Storages {
			if storage.Conversation != (ConversationKey{}) || storage.WorkspaceAnchorTaskID != "" || storage.CompatibilityDigest != "" || storage.WorkspaceCompatibilityDigest != "" ||
				storage.WriterSessionID != "" || storage.LatestWriter != (CheckpointSource{}) || storage.Checkpoint != nil || len(storage.RetiredSessions) != 0 {
				return st, invalid("previous conversation storage")
			}
			if storage.Prepared != nil && (storage.Prepared.Conversation != nil || storage.Prepared.WorkspaceAnchorTaskID != "") {
				return st, invalid("previous retained anchor authority")
			}
		}
		st.Sessions = map[string]WorkerSession{}
		st.Conversations = map[string]Conversation{}
	}
	if st.SchemaVersion == 14 {
		if err := migrateWorkspaceCompatibility(&st); err != nil {
			return st, err
		}
	}
	if st.SchemaVersion < SchemaVersion {
		if err := restoreHistoricalSessionProtocol(&st); err != nil {
			return st, err
		}
	}
	if st.SchemaVersion < 12 {
		for _, terminal := range st.Terminals {
			if terminal.ResultReceipt != nil {
				return st, invalid("previous provider result authority")
			}
		}
	}
	if st.SchemaVersion < 11 {
		for id, g := range st.Grants {
			if g.StartConfirmed || st.Terminals[id].RecoveryFailure != nil {
				return st, invalid("previous result recovery authority")
			}
			// Historical MarkStarted followed a successful backend start. No
			// equivalent proof survives in terminal or closed legacy records.
			g.StartConfirmed = g.State == "started"
			st.Grants[id] = g
		}
	}
	if st.SchemaVersion < 10 {
		for _, g := range st.Grants {
			if g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil {
				return st, invalid("previous checkout authority")
			}
		}
	}
	if st.SchemaVersion == 6 {
		// This transition is additive only. Old records cannot supply newly
		// introduced authority, and no stop evidence or keys are synthesized.
		for _, g := range st.Grants {
			if g.Stop != nil || len(g.StopKey) != 0 {
				return st, invalid("schema 6 stop authority")
			}
		}
		for _, storage := range st.Storages {
			if storage.Dirty {
				return st, invalid("schema 6 data safety")
			}
		}
	}
	s.journalInfo = info
	if st.SchemaVersion != SchemaVersion {
		// Older journals only authorized fallback workspace parents; schemas
		// 6 and 7 also required fallback task names. Validate those contracts
		// before accepting wider native naming, without adopting renamed data.
		for _, storage := range st.Storages {
			if st.SchemaVersion >= 9 {
				continue
			}
			root, err := TaskRoot("/workspace", storage.WorkspaceID, storage.TaskID, "", "")
			if err != nil || filepath.Dir(root) != filepath.Dir(storage.TaskRoot) || st.SchemaVersion < 8 && root != storage.TaskRoot {
				return st, invalid("previous task root")
			}
		}
		current := st
		current.SchemaVersion = SchemaVersion
		if err := s.validateRegistry(current, true); err != nil {
			return st, err
		}
		// Validate the original evidence before draining can change a lifecycle
		// state. Open publishes this stop intent before returning any authority.
		for id, session := range st.Sessions {
			if session.State != SessionClosed {
				requestSessionStop(&st, &session, "session_protocol_upgrade", s.now())
				st.Sessions[id] = session
			}
		}
		if st.SchemaVersion <= 13 {
			for id, g := range st.Grants {
				g.Legacy = true
				st.Grants[id] = g
			}
		}
		return st, nil
	}
	return st, s.validate(st)
}

func (s *Store) write(st registry) error {
	if err := s.validate(st); err != nil {
		return err
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(st); err != nil {
		return err
	}
	raw := encoded.Bytes()
	if int64(len(raw)) > s.options.MaxStoreBytes {
		return ErrAdmissionBudget
	}
	if err := s.checkBudget(int64(len(raw))); err != nil {
		return err
	}
	info, err := atomicWrite(filepath.Join(s.options.Directory, "journal.json"), raw)
	if err != nil {
		s.failed = err
		return err
	}
	s.journalInfo = info
	if err := s.checkJournal(); err != nil {
		return err
	}
	// Callbacks can return values and retain inputs that alias their candidate.
	// Publication owns another copy before those values leave the store.
	s.committed = cloneRegistry(st)
	return nil
}

func atomicWrite(path string, raw []byte) (os.FileInfo, error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".pending-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	var info os.FileInfo
	if err == nil {
		info, err = f.Stat()
	}
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return nil, err
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return info, nil
}
func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func digest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func canonicalUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}
func fingerprint(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == sha256.Size && hex.EncodeToString(raw) == value
}
func sameJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}
func invalid(what string) error { return fmt.Errorf("invalid controller %s authority", what) }

// Budget includes retained interrupted publications and the new atomic copy.
func (s *Store) checkBudget(publication int64) error {
	entries, err := os.ReadDir(s.options.Directory)
	if err != nil {
		return err
	}
	total := publication
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		if total > s.options.MaxStoreBytes {
			return ErrAdmissionBudget
		}
	}
	return nil
}
