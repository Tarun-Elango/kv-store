package replication

import (
	"path/filepath"
	"testing"

	"kvStore/proto"
	"kvStore/store"
)

// TestFollowerRejectsConflictingPreviousEntry checks that a follower detects when the leader's previous log entry does not match its own.
func TestFollowerRejectsConflictingPreviousEntry(t *testing.T) {
	follower, err := NewFollower(
		filepath.Join(t.TempDir(), "follower.wal"),
		store.NewStore[string, []byte](),
		"leader-1",
	)
	if err != nil {
		t.Fatalf("create follower: %v", err)
	}
	defer follower.Close()

	first := Entry{
		Index: 1,
		Command: proto.Command{
			Op:    proto.OpSet,
			Key:   []byte("key"),
			Value: []byte("leader-value"),
		},
	}
	if resp := follower.ApplyAppend(AppendRequest{
		LeaderID: "leader-1",
		Entries:  []Entry{first},
	}); !resp.Success {
		t.Fatalf("append first entry failed: %#v", resp)
	}

	conflictingPrevious := cloneEntry(first)
	conflictingPrevious.Command.Value = []byte("different-value")
	resp := follower.ApplyAppend(AppendRequest{
		LeaderID:  "leader-1",
		PrevIndex: 1,
		PrevEntry: &conflictingPrevious,
		Entries: []Entry{{
			Index: 2,
			Command: proto.Command{
				Op:    proto.OpSet,
				Key:   []byte("next"),
				Value: []byte("value"),
			},
		}},
	})

	if resp.Success {
		t.Fatal("conflicting previous entry was accepted")
	}
	if resp.Code != AppendErrorConflict {
		t.Fatalf("error code = %q, want %q", resp.Code, AppendErrorConflict)
	}
	if resp.LastIndex != 1 {
		t.Fatalf("last index = %d, want 1", resp.LastIndex)
	}
}
