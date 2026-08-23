package replication

import (
	"path/filepath"
	"strings"
	"testing"

	"kvStore/proto"
	"kvStore/store"
)

func TestNewFollowerRejectsOversizedLeaderID(t *testing.T) {
	_, err := NewFollower(
		filepath.Join(t.TempDir(), "follower.wal"),
		store.NewStore[string, []byte](),
		strings.Repeat("l", maxLeaderIDLength+1),
	)
	if err == nil {
		t.Fatal("NewFollower accepted a leader ID longer than the wire limit")
	}
}

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

func TestFollowerReplacesDivergentSuffix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "follower.wal")
	st := store.NewStore[string, []byte]()
	follower, err := NewFollower(path, st, "leader-1")
	if err != nil {
		t.Fatalf("create follower: %v", err)
	}

	first := Entry{Index: 1, Command: proto.Command{
		Op:    proto.OpSet,
		Key:   []byte("shared"),
		Value: []byte("prefix"),
	}}
	oldSuffix := []Entry{
		{Index: 2, Command: proto.Command{
			Op:    proto.OpSet,
			Key:   []byte("stale"),
			Value: []byte("old"),
		}},
		{Index: 3, Command: proto.Command{
			Op:    proto.OpSet,
			Key:   []byte("also-stale"),
			Value: []byte("old"),
		}},
	}

	if resp := follower.ApplyAppend(AppendRequest{
		LeaderID: "leader-1",
		Entries:  append([]Entry{first}, oldSuffix...),
	}); !resp.Success {
		t.Fatalf("append old suffix: %#v", resp)
	}

	newSuffix := []Entry{
		{Index: 2, Command: proto.Command{
			Op:  proto.OpDel,
			Key: []byte("stale"),
		}},
		{Index: 3, Command: proto.Command{
			Op:    proto.OpSet,
			Key:   []byte("fresh"),
			Value: []byte("new"),
		}},
	}

	resp := follower.ApplyAppend(AppendRequest{
		LeaderID:  "leader-1",
		PrevIndex: first.Index,
		PrevEntry: &first,
		Entries:   newSuffix,
	})
	if !resp.Success || resp.LastIndex != 3 {
		t.Fatalf("replace divergent suffix response = %#v, want success through 3", resp)
	}
	if _, ok := st.Get("stale"); ok {
		t.Fatal("stale key from removed suffix is still present")
	}
	if _, ok := st.Get("also-stale"); ok {
		t.Fatal("key from removed suffix is still present")
	}
	if got, ok := st.Get("fresh"); !ok || string(got) != "new" {
		t.Fatalf("fresh key = %q, present=%t; want new, true", got, ok)
	}

	if err := follower.Close(); err != nil {
		t.Fatalf("close follower: %v", err)
	}

	restartedStore := store.NewStore[string, []byte]()
	restarted, err := NewFollower(path, restartedStore, "leader-1")
	if err != nil {
		t.Fatalf("restart follower: %v", err)
	}
	defer restarted.Close()

	if restarted.LastIndex() != 3 {
		t.Fatalf("restarted last index = %d, want 3", restarted.LastIndex())
	}
	if _, ok := restartedStore.Get("also-stale"); ok {
		t.Fatal("restarted store restored a removed suffix key")
	}
	if got, ok := restartedStore.Get("fresh"); !ok || string(got) != "new" {
		t.Fatalf("restarted fresh key = %q, present=%t; want new, true", got, ok)
	}
}
