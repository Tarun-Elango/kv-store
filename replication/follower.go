package replication

import (
	"fmt"
	"kvStore/proto"
	"kvStore/store"
	"kvStore/wal"
	"sync"
)

// gets detail from leader
type Follower struct {
	wal         *wal.Log
	store       *store.Store[string, string]
	lastApplied uint64
	mu          sync.Mutex // so only apply/append happen
}

func NewFollower() *Follower {
	w, err := wal.Open("data/follower.wal")
	if err != nil {
		panic(err)
	}
	return &Follower{
		wal:   w,
		store: store.NewStore[string, string](),
	}
}

// when receives the appendrequest
// lock mutex
// check req.previndex
// for each : wal, store, updatelastapplied
func (f *Follower) ApplyAppend(req AppendRequest) AppendResponse {
	f.mu.Lock()
	defer f.mu.Unlock()

	if req.PrevIndex != f.lastApplied {
		return AppendResponse{
			Success:   false,
			LastIndex: f.lastApplied,
			Error:     fmt.Sprintf("prev index mismatch: got %d want %d", req.PrevIndex, f.lastApplied),
		}
	}

	for _, entry := range req.Entries {
		expectedIndex := f.lastApplied + 1
		if entry.Index != expectedIndex {
			return AppendResponse{
				Success:   false,
				LastIndex: f.lastApplied,
				Error:     fmt.Sprintf("entry index mismatch: got %d want %d", entry.Index, expectedIndex),
			}
		}
		cmd := entry.Command

		var rec wal.Record

		// write to wal first
		switch cmd.Op {
		case proto.OpSet:
			rec = wal.Record{
				Op:    byte(wal.OpSet),
				Key:   append([]byte(nil), cmd.Key...),
				Value: append([]byte(nil), cmd.Value...),
			}
		case proto.OpDel:
			rec = wal.Record{
				Op:  byte(wal.OpDel),
				Key: append([]byte(nil), cmd.Key...),
			}

		default:
			return AppendResponse{
				Success:   false,
				LastIndex: f.lastApplied,
				Error:     fmt.Sprintf("unsupported replicated command op: %d", cmd.Op),
			}
		}
		if err := f.wal.Append(rec); err != nil {
			return AppendResponse{
				Success:   false,
				LastIndex: f.lastApplied,
				Error:     fmt.Sprintf("failed to append follower WAL: %v", err),
			}
		}

		// then write to memory
		switch cmd.Op {
		case proto.OpSet:
			f.store.Set(string(cmd.Key), string(cmd.Value))

		case proto.OpDel:
			f.store.Delete(string(cmd.Key))
		}
		f.lastApplied = entry.Index
	}

	return AppendResponse{
		Success:   true,
		LastIndex: f.lastApplied,
	}
}
