package replication

// flow
//

import "kvStore/proto"

// contract for leader follower transport
// shared replication data structure - log entries, roles, append req/res

type Role int

const (
	RoleLeader   Role = iota // gets 0
	RoleFollower             // this will get 1
)

// one replication operation
// index 5
// command SET user:1 Tarun
type Entry struct {
	Index   uint64
	Command proto.Command
}

// what leader sents to followers
type AppendRequest struct {
	LeaderID  string
	PrevIndex uint64
	Entries   []Entry
}

// follower sends back
type AppendResponse struct {
	Success   bool
	LastIndex uint64
	Error     string
}
