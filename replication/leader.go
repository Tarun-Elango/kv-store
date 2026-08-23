package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrReplicationConflict = errors.New("replication: follower log conflict")

const (
	replicationBatchSize = 64
	initialRetryBackoff  = 50 * time.Millisecond
	maxRetryBackoff      = 2 * time.Second
	rpcTimeout           = 5 * time.Second
)

const maxAppendPayloadSize = maxReplicationFrame - 2 // protocol byte + message-type byte

func encodedReplicationEntrySize(entry Entry) int {
	// index + op + key length + key + value length + value
	return 8 + 1 + 4 + len(entry.Command.Key) + 4 + len(entry.Command.Value)
}

// each follower state( stored in lader)
type FollowerState struct {
	Addr      string
	NextIndex uint64
	Client    PeerClient
	Stopped   bool
	LastError string

	// wake tells the worker leader has new entries
	wake chan struct{}
}

// keeps track of followers
type Leader struct {
	ID string

	mu        sync.Mutex
	entries   []Entry
	followers map[string]*FollowerState

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// creates new leader with entries recoverd from its wal
// intial entries need to be continguous - starting at 1
func NewLeader(
	nodeId string,
	initialEntries []Entry,
	followerAddrs []string) (*Leader, error) {
	if nodeId == "" {
		return nil, fmt.Errorf("replication: leader ID cannot be empty")
	}
	if len(nodeId) > maxLeaderIDLength {
		return nil, fmt.Errorf(
			"replication: leader ID is too long: got %d bytes, max %d",
			len(nodeId),
			maxLeaderIDLength,
		)
	}

	entries := make([]Entry, len(initialEntries)) // this is leaders private in memory log

	// rebuild from wal to entries
	for i, entry := range initialEntries {
		expectedIndex := uint64(i + 1)
		if entry.Index != expectedIndex {
			return nil, fmt.Errorf(
				"leader log is not contiguous: got index %d, want %d",
				entry.Index,
				expectedIndex,
			)
		}
		if err := validateCommand(entry.Command); err != nil {
			return nil, fmt.Errorf(
				"replication: invalid recovered entry %d: %w",
				entry.Index,
				err,
			)
		}
		entries[i] = cloneEntry(entry) //
	}

	// Validate the complete follower configuration before starting any
	// workers. Otherwise, an invalid address later in the slice could cause
	// this constructor to return while workers for earlier addresses are still
	// running.
	for _, addr := range followerAddrs {
		if addr == "" {
			return nil, fmt.Errorf("replication: follower address is empty")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	//create leader
	leader := &Leader{
		ID:        nodeId,
		entries:   entries,
		followers: make(map[string]*FollowerState),
		ctx:       ctx,
		cancel:    cancel,
	}

	// for each follower, create connection, nextIndex+1, create wake signal
	// start replciation goroutine
	for _, addr := range followerAddrs {
		if _, exists := leader.followers[addr]; exists {
			continue
		}

		follower := &FollowerState{
			Addr:      addr,
			NextIndex: 1,
			Client:    NewTCPPeerClient(addr),
			wake:      make(chan struct{}, 1),
		}
		leader.followers[addr] = follower
		leader.wg.Add(1) // done when followerworker defer

		// goroutine
		fmt.Printf("	starting go routine for follower: %s\n", follower.Addr)
		go leader.followerWorker(follower)
	}

	return leader, nil

}

// syncs one follower
// performs initial sync, then waits for new entries, failed retry forever
func (l *Leader) followerWorker(follower *FollowerState) {
	defer l.wg.Done()
	backoff := initialRetryBackoff

	for {
		more, err := l.replicateNextBatch(follower)
		if err != nil {
			if errors.Is(err, ErrReplicationConflict) {
				l.mu.Lock()
				follower.Stopped = true
				follower.LastError = err.Error()
				l.mu.Unlock()
				return
			}
			if !l.waitForRetry(backoff) {
				return
			}
			backoff *= 2
			if backoff > maxRetryBackoff {
				backoff = maxRetryBackoff
			}
			continue
		}
		backoff = initialRetryBackoff
		if more {
			// follower still behind, send next batch
			continue
		}
		select {
		case <-l.ctx.Done():
			return
		case <-follower.wake:
			// new leader entry is available
		}
	}
}

// Replicate publishes an indexed entry to the leader log
//
// waits for entry to be queued locally, it does not wait for followers
// as thats async
func (l *Leader) Replicate(ctx context.Context, entry Entry) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := validateCommand(entry.Command); err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	select {
	case <-l.ctx.Done():
		return fmt.Errorf("replication leader closed")
	default:
	}

	expectedIndex := uint64(len(l.entries) + 1)
	if entry.Index != expectedIndex {
		return fmt.Errorf(
			"leader entry index mismatch: got %d, want %d",
			entry.Index,
			expectedIndex,
		)
	}
	l.entries = append(l.entries, cloneEntry(entry))

	// coalesce notifications.
	// worker will inspect full log and send missing entry
	for _, follower := range l.followers {
		select {
		case follower.wake <- struct{}{}: // leader.go followerWorker() wakes up follower -> replicate next batch
		default:
		}
	}

	return nil
}

// shutdown system
func (l *Leader) Close() {
	// tell workers to stop
	// wait for them to stop
	// close network clients
	l.closeOnce.Do(func() {
		l.cancel()
		// peer client should honor cancel context
		l.wg.Wait()

		l.mu.Lock()
		defer l.mu.Unlock()

		for _, follower := range l.followers {
			if follower.Client != nil {
				_ = follower.Client.Close()
			}
		}
	})
}

func (l *Leader) waitForRetry(delay time.Duration) bool {
	timer := time.NewTicker(delay)
	defer timer.Stop()

	select {
	case <-l.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// called by followerWorker()
// figure what follower missing
// grab from leaders logs
// send to follower, and check response
// update follower.nextindex
func (l *Leader) replicateNextBatch(follower *FollowerState) (bool, error) {
	l.mu.Lock()

	nextIndex := follower.NextIndex
	if nextIndex == 0 {
		nextIndex = 1
		follower.NextIndex = nextIndex
	}
	logLength := uint64(len(l.entries))

	if nextIndex > logLength+1 {
		l.mu.Unlock()
		return false, fmt.Errorf(
			"follower %s is ahead of leader: next index %d, leader last index %d",
			follower.Addr,
			nextIndex,
			logLength,
		)
	}

	prevIndex := nextIndex - 1
	leaderID := l.ID

	var prevEntry *Entry
	if prevIndex > 0 {
		previous := cloneEntry(l.entries[prevIndex-1])
		prevEntry = &previous
	}

	// Bytes before the request's Entries array:
	//
	// leader-ID length + leader ID + previous index +
	// previous-entry flag + entry count
	payloadSize := 2 + len(leaderID) + 8 + 1 + 4

	if prevEntry != nil {
		payloadSize += encodedReplicationEntrySize(*prevEntry)
	}

	if payloadSize > maxAppendPayloadSize {
		l.mu.Unlock()
		return false, fmt.Errorf("replication request metadata exceeds frame limit")
	}

	// Add entries only while they fit in the frame.
	entries := make([]Entry, 0, replicationBatchSize)

	for index := nextIndex; index <= logLength && len(entries) < replicationBatchSize; index++ {
		entry := cloneEntry(l.entries[index-1])
		entrySize := encodedReplicationEntrySize(entry)

		if payloadSize+entrySize > maxAppendPayloadSize {
			// Send what already fits. The next worker iteration sends this entry.
			if len(entries) > 0 {
				break
			}

			// This should not happen with the current 16 MiB value limit,
			// but prevents an endless oversized request in the future.
			l.mu.Unlock()
			return false, fmt.Errorf(
				"replication entry %d is too large for one frame",
				entry.Index,
			)
		}

		entries = append(entries, entry)
		payloadSize += entrySize
	}

	l.mu.Unlock()

	req := AppendRequest{
		LeaderID:  leaderID,
		PrevIndex: prevIndex,
		PrevEntry: prevEntry,
		Entries:   entries,
	}

	rpcCtx, cancel := context.WithTimeout(l.ctx, rpcTimeout)
	fmt.Printf("replicating to %s with prevIndex:%d \n", follower.Addr, req.PrevIndex)
	resp, err := follower.Client.Append(rpcCtx, req)
	cancel()

	if err != nil {
		fmt.Printf("replication to %s failed: %w",
			follower.Addr,
			err)
		return false, fmt.Errorf(
			"replication to %s failed: %w",
			follower.Addr,
			err,
		)
	}

	if !resp.Success {
		if resp.Code == AppendErrorConflict {
			return false, fmt.Errorf(
				"%w: follower %s rejected append: %s",
				ErrReplicationConflict,
				follower.Addr,
				resp.Error,
			)
		}
		if resp.Code != AppendErrorGap {
			return false, fmt.Errorf(
				"follower %s rejected append (%s): %s",
				follower.Addr,
				resp.Code,
				resp.Error,
			)
		}
		// Only a gap is recoverable. Resume from the follower's reported
		// position; invalid requests and conflicts require intervention.
		l.mu.Lock()
		if resp.LastIndex > uint64(len(l.entries)) {
			l.mu.Unlock()
			return false, fmt.Errorf(
				"follower %s reported gap position %d beyond leader last index %d",
				follower.Addr,
				resp.LastIndex,
				len(l.entries),
			)
		}
		follower.NextIndex = resp.LastIndex + 1
		l.mu.Unlock()
		return false, fmt.Errorf(
			"follower %s rejected append: %s; retrying from index %d",
			follower.Addr,
			resp.Error,
			resp.LastIndex+1,
		)
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	// follower cant be behind prevINdex
	if resp.LastIndex < prevIndex {
		return false, fmt.Errorf(
			"follower %s returned invalid last index %d; expected at least %d",
			follower.Addr,
			resp.LastIndex,
			prevIndex,
		)
	}

	// follower must have accepted entries we sent
	// if we send 4,5,6, response should be 6 not 4
	// follower worker gets error and we retry from 4 again
	if len(entries) > 0 {
		expectedLastIndex := entries[len(entries)-1].Index
		if resp.LastIndex < expectedLastIndex {
			return false, fmt.Errorf(
				"follower %s acknowledged index %d; expected at least %d",
				follower.Addr,
				resp.LastIndex,
				expectedLastIndex,
			)
		}
	}

	// follower cant be ahed of leader
	if resp.LastIndex > uint64(len(l.entries)) {
		return false, fmt.Errorf(
			"follower %s reported index %d, beyond leader last index %d",
			follower.Addr,
			resp.LastIndex,
			len(l.entries),
		)
	}

	follower.NextIndex = resp.LastIndex + 1
	//More entries may have been appended while the RPC was in flight.
	return follower.NextIndex <= uint64(len(l.entries)), nil // still some left
}

// func cloneEntry(entry Entry) Entry{
// 	return Entry{
// 		Index: entry.Index,
// 		Command: cloneCommand(entry.Command)
// 	}
// }

// func cloneCommand(command proto.Command) proto.Command{
// 	return proto.Command{
// 		Op: command.Op,
// 		Key: append([]byte(nil), command.Key...),
// 		Value: applyCommand([]byte(nil), command.Value...),
// 	}
// }
