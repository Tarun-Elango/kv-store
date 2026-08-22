package replication

import (
	"bytes"
	"fmt"
	"kvStore/proto"
	"kvStore/store"
	"kvStore/wal"
	"sync"
)

// gets detail from leader
type Follower struct {
	wal   *wal.Log
	store *store.Store[string, []byte]

	leaderID    string
	lastApplied uint64
	entries     map[uint64]Entry

	mu     sync.Mutex // so only apply/append happen
	closed bool
}

// LeaderId only allowed to send replication request
func NewFollower(
	walPath string,
	st *store.Store[string, []byte],
	leaderID string,
) (*Follower, error) {
	if st == nil {
		return nil, fmt.Errorf("replication: follower store cannot be nil")
	}
	if leaderID == "" {
		return nil, fmt.Errorf("replication: leader ID cannot be empty")
	}

	log, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("open follower WAL: %w", err)
	}

	f := &Follower{
		wal:      log,
		store:    st,
		leaderID: leaderID,
		entries:  make(map[uint64]Entry),
	}

	fmt.Printf("Creating follower and replaying log\n")
	err = log.Replay(func(rec wal.Record) error {
		// check index is sequential, is last is 0, first record must be 1
		if rec.Index != f.lastApplied+1 {
			return fmt.Errorf(
				"follower WAL index gap: got %d, want %d",
				rec.Index,
				f.lastApplied+1,
			)
		}

		entry, err := recordToEntry(rec) // record to entry
		if err != nil {
			return err
		}

		applyCommand(f.store, entry.Command) // to followers memory

		// store copy of entry in memory, later used to recongnize safe duplicates retries from leader
		// for fast duplicate/retries checking
		f.entries[entry.Index] = cloneEntry(entry)
		f.lastApplied = entry.Index

		return nil
	})

	if err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("replay follower WAL: %w", err)
	}

	return f, nil
}

// validate
//
//		check leader
//		check prevIndex
//	 	validate all entries before changing anything
//
// write changes to wal
// apply changes to in memory store
// tell leader i am at index x
func (f *Follower) ApplyAppend(req AppendRequest) AppendResponse {
	f.mu.Lock()
	defer f.mu.Unlock()

	fmt.Printf("applying append from leaderid: %s and previndex:%d\n", req.LeaderID, req.PrevIndex)
	fail := func(code AppendErrorCode, err error) AppendResponse {
		return AppendResponse{
			Success:   false,
			LastIndex: f.lastApplied,
			Code:      code,
			Error:     err.Error(),
		}
	}

	if f.closed {
		return fail(AppendErrorInternal, fmt.Errorf("follower is closed"))
	}

	if req.LeaderID == "" {
		return fail(AppendErrorInvalid, fmt.Errorf("leader ID is required"))
	}

	if req.LeaderID != f.leaderID {
		return fail(AppendErrorInvalid, fmt.Errorf(
			"unauthorized leader ID: got %q want %q",
			req.LeaderID,
			f.leaderID,
		))
	}

	if req.PrevIndex > f.lastApplied {
		return fail(
			AppendErrorGap,
			fmt.Errorf("prev index is ahead of follower: got %d want at most %d",
				req.PrevIndex,
				f.lastApplied,
			))
	}

	if req.PrevIndex > 0 {
		previous, ok := f.entries[req.PrevIndex]
		if !ok {
			return fail(AppendErrorGap, fmt.Errorf(
				"previous index %d is not present",
				req.PrevIndex,
			))
		}
		if req.PrevEntry == nil || req.PrevEntry.Index != req.PrevIndex {
			return fail(AppendErrorInvalid, fmt.Errorf(
				"previous entry is required for index %d",
				req.PrevIndex,
			))
		}
		if !sameEntry(previous, *req.PrevEntry) {
			return fail(AppendErrorConflict, fmt.Errorf(
				"conflicting previous entry at index %d",
				req.PrevIndex,
			))
		}
	} else if req.PrevEntry != nil {
		return fail(AppendErrorInvalid, fmt.Errorf(
			"previous entry must be nil when previous index is zero",
		))
	}

	// empty append is only valid, when it refers to current index
	if len(req.Entries) == 0 {
		if req.PrevIndex != f.lastApplied {
			return fail(AppendErrorGap, fmt.Errorf(
				"empty append has stale prev index: got %d want %d",
				req.PrevIndex,
				f.lastApplied,
			))
		}

		return AppendResponse{
			Success:   true,
			LastIndex: f.lastApplied,
		}
	}

	// validate full request before writing anything
	expectedRequestIndex := req.PrevIndex + 1
	simulatedLastIndex := f.lastApplied
	newEntries := make([]Entry, 0, len(req.Entries))

	for _, entry := range req.Entries {
		if entry.Index != expectedRequestIndex {
			return fail(AppendErrorInvalid, fmt.Errorf(
				"entry sequence mismatch: got index %d want %d",
				entry.Index,
				expectedRequestIndex,
			))
		}
		expectedRequestIndex++

		if err := validateCommand(entry.Command); err != nil {
			return fail(AppendErrorInvalid, err)
		}

		// already applied entries are allowed so a lost resp can be retried
		if entry.Index <= f.lastApplied {
			existing, ok := f.entries[entry.Index] // check tracker
			if !ok {
				return fail(AppendErrorGap, fmt.Errorf(
					"entry %d is already applied but not available for comparison",
					entry.Index,
				))
			}

			// if the previously stored is not same as new request
			if !sameEntry(existing, entry) {
				return fail(AppendErrorConflict, fmt.Errorf(
					"conflicting entry at index %d",
					entry.Index,
				))
			}
			continue
		}

		if entry.Index != simulatedLastIndex+1 {
			return fail(AppendErrorGap, fmt.Errorf(
				"entry index gap: got %d want %d",
				entry.Index,
				simulatedLastIndex+1,
			))
		}

		// collect new entries
		newEntries = append(newEntries, cloneEntry(entry))
		simulatedLastIndex = entry.Index
	}

	// persist first, then apply to memory
	// wal, apply to store, entries to
	for _, entry := range newEntries {
		record := entryToRecord(entry)

		if err := f.wal.Append(record); err != nil {
			return fail(AppendErrorInternal, fmt.Errorf("append follower WAL: %w", err))
		}

		applyCommand(f.store, entry.Command)
		f.entries[entry.Index] = cloneEntry(entry)
		f.lastApplied = entry.Index
	}
	return AppendResponse{
		Success:   true,
		LastIndex: f.lastApplied,
	}
}

func (f *Follower) LastIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.lastApplied
}

func (f *Follower) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}

	f.closed = true
	return f.wal.Close()
}

func validateCommand(cmd proto.Command) error {
	switch cmd.Op {
	case proto.OpSet:
		if len(cmd.Key) > wal.MaxKeyLen {
			return wal.ErrKeyTooLarge
		}
		if len(cmd.Value) > wal.MaxValueLen {
			return wal.ErrValueTooLarge
		}
	case proto.OpDel:
		if len(cmd.Key) > wal.MaxKeyLen {
			return wal.ErrKeyTooLarge
		}
	default:
		return fmt.Errorf("unsupported replicated command op: %d", cmd.Op)
	}
	return nil
}

func entryToRecord(entry Entry) wal.Record {
	record := wal.Record{
		Index: entry.Index,
		Key:   append([]byte(nil), entry.Command.Key...),
		Value: append([]byte(nil), entry.Command.Value...),
	}

	switch entry.Command.Op {
	case proto.OpSet:
		record.Op = byte(wal.OpSet)
	case proto.OpDel:
		record.Op = byte(wal.OpDel)
	}

	return record
}

func recordToEntry(record wal.Record) (Entry, error) {
	var op byte

	switch record.Op {
	case byte(wal.OpSet):
		op = proto.OpSet
	case byte(wal.OpDel):
		op = proto.OpDel
	default:
		return Entry{}, fmt.Errorf("unsupported WAL operation: %d", record.Op)
	}

	return Entry{
		Index: record.Index,
		Command: proto.Command{
			Op:    op,
			Key:   append([]byte(nil), record.Key...),
			Value: append([]byte(nil), record.Value...),
		},
	}, nil
}

func applyCommand(
	st *store.Store[string, []byte],
	cmd proto.Command,
) {
	switch cmd.Op {
	case proto.OpSet:
		st.Set(string(cmd.Key), append([]byte(nil), cmd.Value...))
	case proto.OpDel:
		st.Delete(string(cmd.Key))
	}
}

func cloneEntry(entry Entry) Entry {
	return Entry{
		Index: entry.Index,
		Command: proto.Command{
			Op:    entry.Command.Op,
			Key:   append([]byte(nil), entry.Command.Key...),
			Value: append([]byte(nil), entry.Command.Value...),
		},
	}
}

func sameEntry(a, b Entry) bool {
	return a.Index == b.Index &&
		a.Command.Op == b.Command.Op &&
		bytes.Equal(a.Command.Key, b.Command.Key) &&
		bytes.Equal(a.Command.Value, b.Command.Value)
}
