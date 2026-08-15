package main

import (
	"context"
	"flag"
	"fmt"
	"kvStore/proto"
	"kvStore/replication"
	"kvStore/server"
	"kvStore/store"
	"kvStore/wal"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// imports the server package,
// wires up its dependencies (creates a store.Store,
// creates a server.Server, decides on :9000),
// and calls .Serve(). It's glue, not logic.

type options struct {
	nodeID          string
	role            string
	clientAddr      string
	replicationAddr string
	walPath         string
	leaderID        string
	leaderAddr      string
	followers       string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var opts options

	// cmd options, user input will be assigned to theses

	flag.StringVar(&opts.nodeID, "node-id", "", "unique node ID")
	flag.StringVar(&opts.role, "role", "leader", "node role: leader or follower")
	flag.StringVar(&opts.clientAddr, "client-addr", ":9000", "client listen address")
	flag.StringVar(&opts.replicationAddr, "replication-addr", ":9001", "follower replication listen address")
	flag.StringVar(&opts.walPath, "wal", "data/wal.log", "WAL file path")
	flag.StringVar(&opts.leaderID, "leader-id", "", "expected leader ID; required for followers")
	flag.StringVar(&opts.leaderAddr, "leader-addr", "", "leader client address returned to clients")
	flag.StringVar(&opts.followers, "followers", "", "comma-separated follower replication addresses")
	flag.Parse() // parse the cmd line arguments

	if opts.nodeID == "" {
		return fmt.Errorf("-node-id is required")
	}
	role, err := parseRole(opts.role)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	switch role {
	case replication.RoleLeader:
		return runLeader(ctx, opts)
	case replication.RoleFollower:
		return runFollower(ctx, opts)
	default:
		return fmt.Errorf("unsupported role : %s", opts.role)
	}

}

func runLeader(ctx context.Context, opts options) error {
	st := store.NewStore[string, []byte]()

	// open wal
	log, err := wal.Open(opts.walPath)
	if err != nil {
		return fmt.Errorf("open leader WAL: %w", err)
	}

	entries := make([]replication.Entry, 0)

	// replay and apply each command from log
	err = log.Replay(func(rec wal.Record) error {
		entry, err := entryFromRecord(rec)
		if err != nil {
			return err
		}
		applyCommand(st, entry.Command)
		entries = append(entries, entry)
		return nil
	})

	if err != nil {
		_ = log.Close()
		return fmt.Errorf("replay leader WAL: %w", err)
	}

	// get all followers addrs
	followerAddrs, err := parseAddresses(opts.followers)
	if err != nil {
		_ = log.Close()
		return err
	}

	// create leader
	leaderReplicator, err := replication.NewLeader(
		opts.nodeID,
		entries,
		followerAddrs,
	)
	if err != nil {
		_ = log.Close()
		return fmt.Errorf("create leader replicator: %w", err)
	}

	// once leader created and its memory updated
	// create a server to listen
	srv := server.NewWithConfig(
		st,
		log,
		&server.Config{
			Role:             replication.RoleLeader,
			NextIndex:        log.LastIndex() + 1,
			LeaderReplicator: leaderReplicator,
		},
	)

	fmt.Printf(
		"starting leader node=%s client=%s followers=%v\n",
		opts.nodeID,
		opts.clientAddr,
		followerAddrs,
	)

	// serve
	serveErr := srv.Serve(ctx, opts.clientAddr)

	leaderReplicator.Close()
	_ = log.Close()

	if serveErr != nil {
		return fmt.Errorf("leader server: %w", serveErr)
	}

	return nil
}

// runFollower()
//
//	└─ NewReplicationServer(...)
//	     └─ peerServer.Serve()        // listens for TCP connections
//	          └─ s.serveConnection(conn)
//	               └─ serveReplicationConnection(...)
//	                    └─ for loop
//	                         ├─ waits for AppendRequest
//	                         ├─ calls follower.ApplyAppend(req)
//	                         └─ sends AppendResponse
func runFollower(ctx context.Context, opts options) error {
	if opts.leaderID == "" {
		return fmt.Errorf("-leader-id is required for followers")
	}
	st := store.NewStore[string, []byte]() // follower store

	// opens followers wal, replays, returns follower that can apply replication request
	follower, err := replication.NewFollower(
		opts.walPath,
		st,
		opts.leaderID,
	)
	if err != nil {
		return fmt.Errorf("create follower: %w", err)
	}

	// ctx is process level ( sigterm, sigint)
	// child to coordianate the followers two server
	// flow sigterm -> ctx cancelled -> runctx cancelled -> stopped
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// start replication server
	peerServer, err := replication.NewReplicationServer(
		runCtx,
		opts.replicationAddr,
		follower,
	)
	if err != nil {
		_ = follower.Close()
		return fmt.Errorf("create replication server: %w", err)
	}

	srv := server.NewWithConfig(
		st,
		nil, // follower WAL is owned by replication.Follower
		&server.Config{
			Role:               replication.RoleFollower,
			LeaderAddr:         opts.leaderAddr,
			FollowerReplicator: follower,
		},
	)

	peerErrCh := make(chan error, 1)

	// run replicator listener in goroutine
	go func() {
		err := peerServer.Serve()
		if err != nil && runCtx.Err() == nil {
			cancel()
		}

		peerErrCh <- err
	}()
	fmt.Printf(
		"starting follower node=%s client=%s replication=%s leader=%s\n",
		opts.nodeID,
		opts.clientAddr,
		opts.replicationAddr,
		opts.leaderID,
	)

	serveErr := srv.Serve(runCtx, opts.clientAddr)
	cancel()
	_ = peerServer.Close()

	peerErr := <-peerErrCh
	_ = follower.Close()
	if serveErr != nil {
		return fmt.Errorf("follower server: %w", serveErr)
	}

	if peerErr != nil && ctx.Err() == nil {
		return fmt.Errorf("replication server: %w", peerErr)
	}

	return nil
}

func parseRole(value string) (replication.Role, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "leader":
		return replication.RoleLeader, nil
	case "follower":
		return replication.RoleFollower, nil
	default:
		return replication.Role(0), fmt.Errorf(
			"invalid -role %q: expected leader or follower",
			value,
		)
	}
}

func parseAddresses(value string) ([]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}

	parts := strings.Split(value, ",")
	addresses := make([]string, 0, len(parts))

	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr == "" {
			return nil, fmt.Errorf("followers contains an empty address")
		}

		addresses = append(addresses, addr)
	}
	return addresses, nil
}

func entryFromRecord(rec wal.Record) (replication.Entry, error) {
	var op byte

	switch rec.Op {
	case byte(wal.OpSet):
		op = proto.OpSet
	case byte(wal.OpDel):
		op = proto.OpDel
	default:
		return replication.Entry{}, fmt.Errorf(
			"unsupported WAL operation: %d",
			rec.Op,
		)
	}

	return replication.Entry{
		Index: rec.Index,
		Command: proto.Command{
			Op:    op,
			Key:   append([]byte(nil), rec.Key...),
			Value: append([]byte(nil), rec.Value...),
		},
	}, nil
}

func applyCommand(
	st *store.Store[string, []byte],
	cmd proto.Command,
) {
	switch cmd.Op {
	case proto.OpSet:
		st.Set(string(cmd.Key), append([]byte(nil), cmd.Value...))
	case proto.OpDel:
		st.Delete(string(cmd.Key))
	}
}
