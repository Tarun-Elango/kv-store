package replication

import (
	"context"
	"strings"
	"testing"

	"kvStore/proto"
)

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
