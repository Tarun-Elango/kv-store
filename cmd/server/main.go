package main

import (
	"context"
	"fmt"
	"kvStore/server"
	"kvStore/store"
	"kvStore/wal"
	"os"
	"os/signal"
	"syscall"
)

// imports the server package,
// wires up its dependencies (creates a store.Store,
// creates a server.Server, decides on :9000),
// and calls .Serve(). It's glue, not logic.

func main() {
	// context cancelled with ctrl-c, or when os asks to stop (sigterm)
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	st := store.NewStore[string, []byte]() // pointer to store
	log, err := wal.Open("data/wal.log")
	if err != nil {
		fmt.Println("wal open error:", err)
		return
	}
	defer log.Close()

	// replaying log
	if err := log.Replay(func(rec wal.Record) error {
		//	fmt.Printf("op=%d key=%q value=%q\n", rec.Op, rec.Key, rec.Value)
		switch rec.Op {
		case byte(wal.OpSet):
			st.Set(string(rec.Key), rec.Value)
		case byte(wal.OpDel):
			st.Delete(string(rec.Key))
		default:
			return wal.ErrUnknownOp
		}
		return nil
	}); err != nil {
		fmt.Println("wal replay error:", err)
		return
	}
	//nextIndex := log.LastIndex() + 1

	srv := server.New(st, log)
	addr := ":9000"
	fmt.Printf("listening on %s\n", addr)
	if err := srv.Serve(ctx, addr); err != nil {
		fmt.Println("server error:", err)
	}
}
