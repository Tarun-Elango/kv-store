package replication

// This file moves replication messages between leader and follower over TCP.
// In simple terms: the leader sends a write, and the follower sends back an answer.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type PeerClient interface {
	Append(ctx context.Context, req AppendRequest) (AppendResponse, error)
	Close() error
}

type TCPPeerClient struct {
	addr string

	mu      sync.Mutex
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
	closed  bool
}

func NewTCPPeerClient(addr string) *TCPPeerClient {
	// dial later or lazily
	return &TCPPeerClient{
		addr: addr,
	}
}

// sends replication request from leader to follower
// lock client
// check if client closed expired
// lazy connect c.conn == nil
// register context cancellation -AfterFunc
// apply context deadline
// encode and send appendrequest
// decode appendresponse
// if fails call invalidateLocked
// send resp to leader
func (c *TCPPeerClient) Append(ctx context.Context, req AppendRequest) (AppendResponse, error) {
	if c == nil {
		return AppendResponse{}, errors.New("replication: nil TCP peer client")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return AppendResponse{}, errors.New("replication: peer client is closed")
	}
	if err := ctx.Err(); err != nil {
		return AppendResponse{}, err
	}

	if c.conn == nil {
		dialer := net.Dialer{}

		// c is follower, this func called by leader
		conn, err := dialer.DialContext(ctx, "tcp", c.addr)
		if err != nil {
			if ctx.Err() != nil {
				return AppendResponse{}, ctx.Err()
			}
			return AppendResponse{}, fmt.Errorf(
				"replication: dial follower %s: %w",
				c.addr,
				err,
			)
		}
		c.conn = conn
		c.encoder = json.NewEncoder(conn)
		c.decoder = json.NewDecoder(conn)
	}

	conn := c.conn

	stopCancelWatcher := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stopCancelWatcher()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			c.invalidateLocked(conn)
			return AppendResponse{}, err
		}
	}

	defer conn.SetDeadline(time.Time{})

	if err := c.encoder.Encode(req); err != nil {
		c.invalidateLocked(conn)

		if ctx.Err() != nil {
			return AppendResponse{}, ctx.Err()
		}
		return AppendResponse{}, fmt.Errorf(
			"replication: write append request to %s: %w",
			c.addr,
			err,
		)
	}

	var resp AppendResponse
	if err := c.decoder.Decode(&resp); err != nil {
		c.invalidateLocked(conn)

		if ctx.Err() != nil {
			return AppendResponse{}, ctx.Err()
		}
		return AppendResponse{}, fmt.Errorf(
			"replication: read append response from %s: %w",
			c.addr,
			err,
		)
	}

	if err := ctx.Err(); err != nil {
		return AppendResponse{}, err
	}

	return resp, nil

}

// makes current connection unusable
// to recover from broken connection
func (c *TCPPeerClient) invalidateLocked(conn net.Conn) {
	if c.conn == conn {
		c.conn = nil
		c.encoder = nil
		c.decoder = nil
	}

	_ = conn.Close()
}

// Close closes the TCP connection used for replication.
func (c *TCPPeerClient) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.conn = nil
	c.encoder = nil
	c.decoder = nil
	return err
}

// replication server owns replication listener and all peer connections
type ReplicationServer struct {
	listener net.Listener
	follower *Follower

	ctx    context.Context
	cancel context.CancelFunc

	mu          sync.Mutex
	connections map[net.Conn]struct{}
	closed      bool

	wg         sync.WaitGroup
	closedOnce sync.Once
}

// NewReplicationServer binds the replication listener immediately.
// Binding errors are returned to the caller instead of being printed and ignored.
func NewReplicationServer(
	ctx context.Context,
	addr string,
	follower *Follower,
) (*ReplicationServer, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if follower == nil {
		return nil, errors.New("replication: follower cannot be nil")
	}
	if addr == "" {
		return nil, errors.New("replication: listen address cannot be empty")
	}

	// listne on port
	fmt.Printf("creating replication server listening on %s \n", addr)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf(
			"replication: listen on %s: %w",
			addr,
			err,
		)
	}

	serverCtx, cancel := context.WithCancel(ctx) // child context

	server := &ReplicationServer{
		listener:    listener,
		follower:    follower,
		ctx:         serverCtx,
		cancel:      cancel,
		connections: make(map[net.Conn]struct{}),
	}

	// do nothing for this server until its context is canceled then close server
	go func() {
		<-serverCtx.Done() // done sends a receive only channel
		_ = server.Close()
	}()
	return server, nil

}

// accepts replication connections until close / context cancel
func (s *ReplicationServer) Serve() error {
	if s == nil {
		return errors.New("replication: nil server")
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// if follower shutting down
			if s.isClosed() ||
				errors.Is(err, net.ErrClosed) ||
				s.ctx.Err() != nil {
				return nil
			}

			// wait 100ms
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				timer := time.NewTimer(100 * time.Millisecond)

				select {
				case <-s.ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
					continue
				}
			}

			_ = s.Close()
			return fmt.Errorf("replication: accept connection: %w", err)
		}

		s.mu.Lock()

		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}

		s.connections[conn] = struct{}{}
		s.wg.Add(1)

		s.mu.Unlock()

		go s.serveConnection(conn)
	}
}

func (s *ReplicationServer) serveConnection(conn net.Conn) {
	defer func() {
		_ = conn.Close()

		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()

		s.wg.Done()
	}()

	serveReplicationConnection(s.ctx, conn, s.follower)
}

// stop accepting connections and close all peer connections
func (s *ReplicationServer) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	s.closedOnce.Do(func() {
		s.cancel()

		s.mu.Lock()
		s.closed = true
		listener := s.listener
		connections := make([]net.Conn, 0, len(s.connections))
		for conn := range s.connections {
			connections = append(connections, conn)
		}
		s.mu.Unlock()

		if listener != nil {
			closeErr = listener.Close()
		}
		// Closing connections interrupts blocked JSON reads/writes.
		for _, conn := range connections {
			_ = conn.Close()
		}

		s.wg.Wait()
	})
	return closeErr
}

func (s *ReplicationServer) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// ServerReplication is kept as a compatibility wrapper.
func ServerReplication(conn net.Conn, follower *Follower) {
	ServeReplication(conn, follower)
}

// ServeReplication is the standalone connection handler required by replication
// transport api
func ServeReplication(conn net.Conn, follower *Follower) {
	serveReplicationConnection(context.Background(), conn, follower)
}

// follower side loop for 1 tcp connection ( handle 1 tcp conn)
// listen for request from the same leader conn
func serveReplicationConnection(
	ctx context.Context,
	conn net.Conn,
	follower *Follower,
) {
	if conn == nil {
		return
	}
	defer conn.Close()

	if ctx == nil {
		ctx = context.Background()
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		var req AppendRequest
		if err := decoder.Decode(&req); err != nil { // <- blocking, until appendrequest arrives
			if errors.Is(err, io.EOF) {
				return
			}

			if ctx.Err() != nil {
				return
			}

			_ = encoder.Encode(AppendResponse{
				Success: false,
				Error: fmt.Sprintf(
					"replication: decode append request: %v",
					err,
				),
			})
			return
		}

		if follower == nil {
			_ = encoder.Encode(AppendResponse{
				Success: false,
				Error:   "replication: nil follower",
			})
			return
		}

		resp := follower.ApplyAppend(req) // actual append work

		if err := encoder.Encode(resp); err != nil {
			return
		}
	}
}
