package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"kvStore/proto"
	"kvStore/store"
	"kvStore/wal"
	"net"
	"strconv"
	"sync"
	"time"
)

// reusable code
// that defines type Server struct{...} and its methods

// server will keep the store, contructor will create the struct,
// prevents every function receiving the store
type Server struct {
	store *store.Store[string, []byte]
	log   *wal.Log // pointer to wal.log ( has file descriptor and mutex)

	listener    net.Listener          // lets shutdown() close it, cause shutdown can be called from different routine,
	connections map[net.Conn]struct{} // clientConn1: {}, clientConn2: {}

	mu           sync.Mutex     // prevet race while accept/handler/shutdown - lock for shared server data
	shuttingDown bool           // prevent accepting/starting while shutting down
	handlers     sync.WaitGroup // lets shutdown() wait for all goroutines to finish
	shutDownOnce sync.Once      // helps ensure shutdown only happens once, sync.once guarantees once execution - used in shutdown
}

// constructor, takes store, returns server pointer
func New(s *store.Store[string, []byte], log *wal.Log) *Server {
	return &Server{
		store:       s,
		log:         log,
		connections: make(map[net.Conn]struct{}), // make a set, we keep value as empty struct - no mem alloc
	}

}

// listen for connections, accept, handle
func (s *Server) Serve(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	// safely save the tcp listener
	// //prevetn shutdown from listening when changes are being made
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	stopWatcher := make(chan struct{}) // to help stop the below routine
	defer close(stopWatcher)

	// go routine to watch ctx.done(), call shutdown when we see it
	go func() {
		select {
		case <-ctx.Done(): // when ctrl + c di psressed or sigter,
			s.shutdown()
		case <-stopWatcher:
		}
	}()

	// for each accepted connection, we set variables and serve
	for {
		conn, err := ln.Accept() // pauses here till client connects
		if err != nil {
			// Shutdown closes the listener, which makes Accept return.
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				s.shutdown() // waits for active handlers before returning
				return nil
			}
			return err
		}

		s.mu.Lock()
		if s.shuttingDown { //reject new
			s.mu.Unlock()
			conn.Close()
			continue
		}

		s.connections[conn] = struct{}{} // a set, using map with empty value
		s.handlers.Add(1)                // counter
		s.mu.Unlock()
		fmt.Printf("client connected: %s\n", conn.RemoteAddr())
		go s.serveConn(ctx, conn) // new routine serve
	}
}

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer func() {
		// cleanup variables from struct - when a client disconnets
		conn.Close() // close connection
		s.mu.Lock()
		delete(s.connections, conn) // delete from pa
		s.mu.Unlock()
		s.handlers.Done() //decrememnt counter by 1
		fmt.Printf("Disconnected %s \n", conn.RemoteAddr())
	}()
	if err := s.handleConn(conn); err != nil && ctx.Err() == nil {
		fmt.Printf("connection %s error: %v\n", conn.RemoteAddr(), err)
	}
}

func (s *Server) shutdown() {
	s.shutDownOnce.Do(func() { //run once
		s.mu.Lock()
		s.shuttingDown = true
		ln := s.listener
		connections := make([]net.Conn, 0, len(s.connections)) //empty slice type net.conn, capacity len(s.connections)
		for conn := range s.connections {
			connections = append(connections, conn) // pulls keys from s.connnections map and add to new slice
		}
		s.mu.Unlock()
		// makes accept return
		if ln != nil {
			_ = ln.Close()
		}

		deadline := time.Now()
		for _, conn := range connections {
			_ = conn.SetDeadline(deadline) // immediately stop any live conn, by forcing timeout error
		}
		s.handlers.Wait() // wait till all goroutine have decreemented till 0
	})
}

// loop forever, every iteration 1 command
// decode call dispatch, encode resp - written back to client
func (s *Server) handleConn(conn net.Conn) error {
	// add a small memory buffer around the tcp connection - need to flush to send
	// reason - read more effecient, bufio pulls more from conn than asked
	// write can be chunked before flushing together.
	reader := bufio.NewReader(conn) // buffer 1, not blocking , blocking only when read
	writer := bufio.NewWriter(conn) // buffer 2
	for {
		// when decoder asks for byte[0], bufio pull a larger block
		// if buffer empty reads more
		cmd, err := proto.DecodeCommand(reader) // bufio implements io.reader
		if err == io.EOF {
			return nil // clean close
		}
		if err != nil {
			return err
		}

		resp, err := s.dispatch(cmd)
		if err != nil {
			return err
		}
		if err := proto.EncodeResponse(writer, resp); err != nil {
			return err
		}

		// flush will send bytes to conn
		if err := writer.Flush(); err != nil {
			return err
		}
	}
}

// perform based on command, return response obj
func (s *Server) dispatch(cmd proto.Command) (proto.Response, error) {
	switch cmd.Op {
	case proto.OpGet:
		val, ok := s.store.Get(string(cmd.Key))
		if !ok {
			return proto.Response{Status: proto.StatusNotFound}, nil
		}
		return proto.Response{Status: proto.StatusOk, Value: val}, nil
	case proto.OpSet:
		rec := wal.Record{
			Op:    byte(wal.OpSet),
			Key:   append([]byte(nil), cmd.Key...), // deep copy
			Value: append([]byte(nil), cmd.Value...),
		}
		if err := s.log.Append(rec); err != nil {
			return proto.Response{Status: proto.StatusError}, err
		}

		s.store.Set(string(cmd.Key), cmd.Value)
		return proto.Response{Status: proto.StatusOk}, nil

	case proto.OpDel:

		rec := wal.Record{
			Op:  byte(wal.OpDel),
			Key: append([]byte(nil), cmd.Key...), // deep copy
		}

		if err := s.log.Append(rec); err != nil {
			return proto.Response{Status: proto.StatusError}, err
		}
		s.store.Delete(string(cmd.Key))
		return proto.Response{Status: proto.StatusOk}, nil

	case proto.OpPing:
		return proto.Response{Status: proto.StatusOk}, nil

	case proto.OpLen:
		//	do operation (get len), and send response object back
		return proto.Response{Status: proto.StatusOk, Value: []byte(strconv.Itoa(s.store.Len()))}, nil
	default:
		// shouldn't happen — Decode already validates opcode
		return proto.Response{Status: proto.StatusError}, nil

	}
}
