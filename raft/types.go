package raft

type Role uint8

const (
	Follower Role = iota
	Leader
	Candidate
)
