package replication

import (
	"context"
	"strings"
	"testing"
	"time"

	"kvStore/proto"
)

type conflictThenSuccessClient struct {
	requests chan AppendRequest
	calls    int
}

func (c *conflictThenSuccessClient) Append(_ context.Context, req AppendRequest) (AppendResponse, error) {
	c.calls++
	c.requests <- req

	if c.calls == 1 {
		return AppendResponse{
			Success: false,
			Code:    AppendErrorConflict,
			Error:   "previous entry differs",
		}, nil
	}

	lastIndex := req.PrevIndex
	if len(req.Entries) > 0 {
		lastIndex = req.Entries[len(req.Entries)-1].Index
	}
	return AppendResponse{Success: true, LastIndex: lastIndex}, nil
}

func (c *conflictThenSuccessClient) Close() error { return nil }

// TestNewLeaderRejectsEmptyID checks that a leader cannot be created without a node ID.
func TestNewLeaderRejectsEmptyID(t *testing.T) {
	if _, err := NewLeader("", nil, nil); err == nil {
		t.Fatal("NewLeader accepted an empty leader ID")
	}
}

func TestNewLeaderRejectsOversizedID(t *testing.T) {
	if _, err := NewLeader(strings.Repeat("l", maxLeaderIDLength+1), nil, nil); err == nil {
		t.Fatal("NewLeader accepted a leader ID longer than the wire limit")
	}
}

// TestNewLeaderRejectsInvalidRecoveredCommand checks that recovered log entries only contain supported write commands.
func TestNewLeaderRejectsInvalidRecoveredCommand(t *testing.T) {
	_, err := NewLeader("leader-1", []Entry{{
		Index: 1,
		Command: proto.Command{
			Op:  proto.OpGet,
			Key: []byte("key"),
		},
	}}, nil)
	if err == nil {
		t.Fatal("NewLeader accepted a recovered GET command")
	}
}

// TestReplicateRejectsInvalidCommandWithoutAdvancingLog checks that an invalid command is rejected and does not consume its log index.
func TestReplicateRejectsInvalidCommandWithoutAdvancingLog(t *testing.T) {
	leader, err := NewLeader("leader-1", nil, nil)
	if err != nil {
		t.Fatalf("create leader: %v", err)
	}
	defer leader.Close()

	if err := leader.Replicate(context.Background(), Entry{
		Index: 1,
		Command: proto.Command{
			Op: proto.OpPing,
		},
	}); err == nil {
		t.Fatal("Replicate accepted a PING command")
	}

	if err := leader.Replicate(context.Background(), Entry{
		Index: 1,
		Command: proto.Command{
			Op:    proto.OpSet,
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}); err != nil {
		t.Fatalf("valid entry was rejected after invalid entry: %v", err)
	}
}

func TestFollowerWorkerBacktracksAfterPreviousEntryConflict(t *testing.T) {
	entries := []Entry{
		{Index: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("one"), Value: []byte("1")}},
		{Index: 2, Command: proto.Command{Op: proto.OpSet, Key: []byte("two"), Value: []byte("2")}},
	}
	leader, err := NewLeader("leader-1", entries, nil)
	if err != nil {
		t.Fatalf("create leader: %v", err)
	}

	client := &conflictThenSuccessClient{requests: make(chan AppendRequest, 2)}
	follower := &FollowerState{
		Addr:      "test-follower",
		NextIndex: 2,
		Client:    client,
		wake:      make(chan struct{}, 1),
	}

	leader.mu.Lock()
	leader.followers[follower.Addr] = follower
	leader.wg.Add(1)
	leader.mu.Unlock()
	go leader.followerWorker(follower)
	defer leader.Close()

	for want, previousIndex := range []uint64{1, 0} {
		select {
		case req := <-client.requests:
			if req.PrevIndex != previousIndex {
				t.Fatalf("request %d previous index = %d, want %d", want, req.PrevIndex, previousIndex)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for request %d", want)
		}
	}

	leader.mu.Lock()
	nextIndex := follower.NextIndex
	leader.mu.Unlock()
	if nextIndex != 3 {
		t.Fatalf("next index after successful retry = %d, want 3", nextIndex)
	}
}
