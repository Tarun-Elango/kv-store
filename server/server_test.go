package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"kvStore/proto"
	"kvStore/store"
	"kvStore/wal"
	"net"
	"testing"
	"time"
)

func newTestServer() (*Server, error) {
	log, err := wal.Open("data/wal-test.log")
	if err != nil {
		return nil, err
	}
	return New(store.NewStore[string, []byte](), log), nil
}

// TestDispatch verifies each command's store-facing behavior without requiring
// a TCP listener. It also covers the only response statuses dispatch can return.
func TestDispatch(t *testing.T) {
	s, err := newTestServer()
	if err != nil {
		fmt.Print("wal open error:", err)
	}

	response, err := s.dispatch(proto.Command{Op: proto.OpGet, Key: []byte("missing")})
	if response.Status != proto.StatusNotFound {
		t.Fatalf("missing GET status = %d, want %d", response.Status, proto.StatusNotFound)
	}

	// SET persists bytes, and GET returns those exact bytes.
	response, err = s.dispatch(
		proto.Command{
			Op:    proto.OpSet,
			Key:   []byte("language"),
			Value: []byte("go"),
		})
	if response.Status != proto.StatusOk {
		t.Fatalf("SET status = %d, want %d", response.Status, proto.StatusOk)
	}

	response, err = s.dispatch(proto.Command{Op: proto.OpLen, Key: []byte("language")})
	if response.Status != proto.StatusOk || string(response.Value) != "1" {
		t.Fatalf("LEN response = %#v, want OK with value %q", response, "1")
	}

	// DELETE removes the key; a later GET must report it as absent.
	response, err = s.dispatch(proto.Command{Op: proto.OpDel, Key: []byte("language")})
	if response.Status != proto.StatusOk {
		t.Fatalf("DEL status = %d, want %d", response.Status, proto.StatusOk)
	}
	response, err = s.dispatch(proto.Command{Op: proto.OpGet, Key: []byte("language")})
	if response.Status != proto.StatusNotFound {
		t.Fatalf("GET after DEL status = %d, want %d", response.Status, proto.StatusNotFound)
	}

	// PING has no store side effect and always acknowledges the client.
	response, err = s.dispatch(proto.Command{Op: proto.OpPing})
	if response.Status != proto.StatusOk {
		t.Fatalf("PING status = %d, want %d", response.Status, proto.StatusOk)
	}

	// LEN reflects the current number of keys, not the total number of writes.
	response, err = s.dispatch(proto.Command{Op: proto.OpLen})
	if response.Status != proto.StatusOk || string(response.Value) != "0" {
		t.Fatalf("LEN response = %#v, want OK with value %q", response, "0")
	}

	// unknown opcode
	response, err = s.dispatch(proto.Command{Op: 99})
	if response.Status != proto.StatusError {
		t.Fatalf("unknown opcode status = %d, want %d", response.Status, proto.StatusError)
	}
}

// TestHandleConnMultipleCommands verifies that one client connection can carry
// several encoded requests and receives responses in the same order.
func TestHanldeConnMultipleCommands(t *testing.T) {
	s, err := newTestServer()
	if err != nil {
		fmt.Print("wal open error:", err)
	}
	serverConn, clientConn := net.Pipe() // duplex connection
	defer clientConn.Close()

	done := make(chan error, 1) // buffered channel error type, 1 size

	// handle connection async
	// send error to done channel
	// closes after handling finishes
	go func() {
		done <- s.handleConn(serverConn)
		_ = serverConn.Close()
	}()

	writer := bufio.NewWriter(clientConn)
	reader := bufio.NewReader(clientConn)

	commands := []proto.Command{
		{Op: proto.OpSet, Key: []byte("answer"), Value: []byte("42")},
		{Op: proto.OpGet, Key: []byte("answer")},
		{Op: proto.OpLen},
	}

	wants := []proto.Response{
		{Status: proto.StatusOk},
		{Status: proto.StatusOk, Value: []byte("42")},
		{Status: proto.StatusOk, Value: []byte("1")},
	}

	for i, command := range commands {
		if err := proto.EncodeCommand(writer, command); err != nil {
			t.Fatalf("encode command %d: %v", i, err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatalf("flush command %d: %v", i, err)
		}
		got, err := proto.DecodeResponse(reader)
		if err != nil {
			t.Fatalf("decode response %d: %v", i, err)
		}
		if got.Status != wants[i].Status || !bytes.Equal(got.Value, wants[i].Value) {
			t.Errorf("response %d = %#v, want %#v", i, got, wants[i])
		}
	}

	clientConn.Close()
	if err := <-done; err != nil {
		t.Fatalf("handleConn returned %v after clean close, want nil", err)
	}

}

// TestHandleConnRejectsMalformedRequest ensures an invalid frame ends handling
// instead of allowing the connection to continue with an undefined request.
func TestHandleConnRejectsMalformedRequest(t *testing.T) {
	s, err := newTestServer()
	if err != nil {
		fmt.Print("wal open error:", err)
	}
	serverConn, clientConn := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- s.handleConn(serverConn)
		_ = serverConn.Close()
	}()

	// not valid opcode
	if _, err := clientConn.Write([]byte("99")); err != nil {
		t.Fatalf("write malformed request: %v", err)
	}

	err = <-done
	if !errors.Is(err, proto.ErrUnknownOpcode) {
		t.Fatalf("handleConn error = %v, want ErrUnknownOpcode", err)
	}
}

// TestShutdownStopsActiveHandler verifies cancellation closes an in-flight
// connection and waits until its handler has finished cleanup.
func TestShutdownStopsActiveHandler(t *testing.T) {
	s, err := newTestServer()
	if err != nil {
		fmt.Print("wal open error:", err)
	}
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	s.mu.Lock()
	s.connections[serverConn] = struct{}{}
	s.handlers.Add(1)
	s.mu.Unlock()

	go s.serveConn(context.Background(), serverConn) // go routine

	finished := make(chan struct{}) // used to send signal
	go func() {
		s.shutdown()
		close(finished)
	}()

	select {
	case <-finished: // unblocks when shutdown complete, and channel closed
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after closing the active connection")
	}

	s.mu.Lock()
	_, stillTracked := s.connections[serverConn]
	s.mu.Unlock()

	if stillTracked {
		t.Fatal("closed connection is still tracked by the server")
	}

	// peer must observe that its connection cannot be used anymore
	_, err = clientConn.Write([]byte{proto.OpPing})
	if err == nil {
		t.Fatal("write to connection after shutdown succeeded, want an error")
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write after shutdown error = %v, want io.ErrClosedPipe", err)
	}
}
