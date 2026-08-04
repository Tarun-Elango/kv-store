package replication

import (
	"context"
	"fmt"
	"sync"
)

// each follower state( stored in lader)
type FollowerState struct {
	Addr       string
	NextIndex  uint64
	Client     PeerClient
	InFlightCh chan Entry
}

// keeps track of followers
type Leader struct {
	followers map[string]*FollowerState
	mu        sync.Mutex
	closed    chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	ID        string
}

func NewLeader(followerAddrs []string) *Leader {
	// create one follower state per follower address
	// create a network client for each follower
	// create worker goroutine per follower
	const inFlightBufferSize = 28

	leader := &Leader{
		followers: make(map[string]*FollowerState),
		closed:    make(chan struct{}),
	}

	for _, addr := range followerAddrs {
		fs := &FollowerState{
			Addr:       addr,
			NextIndex:  1,
			Client:     NewTCPPeerClient(addr),
			InFlightCh: make(chan Entry, inFlightBufferSize),
		}

		leader.followers[addr] = fs
		leader.wg.Add(1)

		go leader.followerWorker(fs)
	}

	return leader
}

func (l *Leader) followerWorker(fs *FollowerState) {
	defer l.wg.Done()
	for {
		select {
		case <-l.closed: // closed is channel (indicate leader shutting down)
			return
		case entry, ok := <-fs.InFlightCh: // entry has channel value, ok tells if still open
			if !ok {
				return
			}
			_ = entry
		}
	}
}

// when leader has to replicate a write
// wraps as entry obj - and sends to all follower queue
func (l *Leader) Replicate(ctx context.Context, entry Entry) error {
	// for each follower
	// 	send entry to each follower worker queue
	// no need to spawn unbounded go routines
	// wait for ack ( depend on policy )
	l.mu.Lock()
	followers := make([]*FollowerState, 0, len(l.followers))
	for _, fs := range l.followers {
		followers = append(followers, fs)
	}
	l.mu.Unlock()
	var wg sync.WaitGroup
	errCh := make(chan error, len(followers))

	// loop followers
	for _, fs := range followers {
		fs := fs
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.replicateToFollower(ctx, fs, entry); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait() // wait till all followers
	close(errCh)

	var firstErr error
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// send one entry to follower
func (l *Leader) replicateToFollower(ctx context.Context, fs *FollowerState, entry Entry) error {
	//Build appendrequest
	// send to follower
	// wait for response
	// if success: follower.nextindex
	// else: retry / handle error
	if fs == nil || fs.Client == nil {
		return fmt.Errorf("replication: nil follower state/client")
	}

	prevIndex := uint64(0)
	if fs.NextIndex > 0 {
		prevIndex = fs.NextIndex - 1
	}

	req := AppendRequest{
		LeaderID:  l.ID,
		PrevIndex: prevIndex,
		Entries:   []Entry{entry},
	}
	resp, err := fs.Client.Append(ctx, req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("replication failed for %s: %s", fs.Addr, resp.Error)
	}

	l.mu.Lock()
	fs.NextIndex = resp.LastIndex + 1
	l.mu.Unlock()
	return nil
}

// shutdown system
func (l *Leader) Close() {
	// tell workers to stop
	// wait for them to stop
	// close network clients
	l.closeOnce.Do(func() {
		close(l.closed) // send signal to channel
		l.wg.Wait()

		l.mu.Lock()
		defer l.mu.Unlock()
		for _, fs := range l.followers {
			if fs != nil && fs.Client != nil {
				_ = fs.Client.Close()
			}
		}
	})
}
