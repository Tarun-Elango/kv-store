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

func (r Role) String() string {
	switch r {
	case RoleLeader:
		return "leader"
	case RoleFollower:
		return "follower"
	default:
		return "unknown"
	}
}

func (r Role) Valid() bool {
	return r == RoleLeader || r == RoleFollower
}

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
	// PrevEntry is the entry the leader believes exists at PrevIndex. It is
	// required when PrevIndex > 0 so the follower can detect divergent logs,
	// not merely missing indexes.
	PrevEntry *Entry
	Entries   []Entry
}

type AppendErrorCode string

const (
	AppendErrorNone     AppendErrorCode = ""
	AppendErrorInvalid  AppendErrorCode = "invalid"
	AppendErrorGap      AppendErrorCode = "gap"
	AppendErrorConflict AppendErrorCode = "conflict"
	AppendErrorInternal AppendErrorCode = "internal"
)

// follower sends back
type AppendResponse struct {
	Success   bool
	LastIndex uint64
	Code      AppendErrorCode
	Error     string
}
