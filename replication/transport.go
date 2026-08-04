package replication

// This file moves replication messages between leader and follower over TCP.
// In simple terms: the leader sends a write, and the follower sends back an answer.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

type PeerClient interface {
	Append(ctx context.Context, req AppendRequest) (AppendResponse, error)
	Close() error
}

type TCPPeerClient struct {
	addr string
	conn net.Conn
}

func NewTCPPeerClient(addr string) *TCPPeerClient {
	// dial later or lazily
	return &TCPPeerClient{
		addr: addr,
		conn: nil,
	}
}

// Append sends one replication request to a follower.
// Simple flow:
// 1. connect to the follower if needed
// 2. send the request as JSON
// 3. wait for the follower's reply
// 4. return that reply to the leader
func (c *TCPPeerClient) Append(ctx context.Context, req AppendRequest) (AppendResponse, error) {
	if c == nil {
		return AppendResponse{}, fmt.Errorf("nil TCPPeerClient")
	}

	if c.conn == nil {
		d := net.Dialer{}
		conn, err := d.DialContext(ctx, "tcp", c.addr)
		if err != nil {
			return AppendResponse{}, err
		}
		c.conn = conn
	}

	// honor deadline
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}

	enc := json.NewEncoder(c.conn)
	dec := json.NewDecoder(c.conn)

	if err := enc.Encode(req); err != nil {
		_ = c.conn.Close()
		c.conn = nil
		return AppendResponse{}, err
	}
	var resp AppendResponse
	if err := dec.Decode(&resp); err != nil {
		_ = c.conn.Close()
		c.conn = nil
		return AppendResponse{}, err
	}
	return resp, nil
}

// Close closes the TCP connection used for replication.
func (c *TCPPeerClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// ServerReplication reads replication messages from a leader,
// applies them to the follower, and sends back the result.
// One TCP connection can carry many append requests.
func ServerReplication(conn net.Conn, follower *Follower) {
	if conn == nil {
		return
	}
	defer conn.Close()

	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)

	for {
		var req AppendRequest
		if err := dec.Decode(&req); err != nil {
			if err == io.EOF {
				return
			}
			_ = enc.Encode(AppendResponse{
				Success: false,
				Error:   fmt.Sprintf("decode append request: %v", err),
			})
			return
		}

		if follower == nil {
			_ = enc.Encode(AppendResponse{
				Success: false,
				Error:   "nil follower",
			})
			return
		}

		resp := follower.ApplyAppend(req)
		if err := enc.Encode(resp); err != nil {
			return
		}
	}
}

// ListenAndServeReplication starts a TCP server for follower replication.
// It accepts leader connections and hands each one to ServerReplication.
func ListenAndServeReplication(addr string, follower *Follower) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("replication listen error on %s: %v\n", addr, err)
		return
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			fmt.Printf("replication accept stopped: %v\n", err)
			return
		}

		go ServerReplication(conn, follower)
	}
}

func (l *Leader) Close() {

}
