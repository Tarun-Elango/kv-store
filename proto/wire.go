package proto

import "errors"

// opcodes
const (
	OpGet  byte = 1
	OpSet  byte = 2
	OpDel  byte = 3
	OpPing byte = 4
	OpLen  byte = 5
)

// status codes for responses
const (
	StatusOk        byte = 0
	StatusNotFound  byte = 1
	StatusError     byte = 2
	StatusNotLeader byte = 3
)

const (
	MaxKeyLen   = 65535            // uint16 max (65,535 bytes)
	MaxValueLen = 16 * 1024 * 1024 // 16 MB - uint32, 16 mil bytes = 16 Mb ( uint32 can hold the number 16 mil)
)

var (
	ErrUnknownOpcode = errors.New("proto: unknown opcode")
	ErrUnknownStatus = errors.New("proto: unknown status")
	ErrKeyTooLarge   = errors.New("proto: key exceeds max length")
	ErrValueTooLarge = errors.New("proto: value exceeds max length")
)

type Command struct {
	Op    byte
	Key   []byte
	Value []byte
}

type Response struct {
	Status byte
	Value  []byte
}

func ValidOpcode(op byte) bool {
	switch op {
	case OpSet, OpDel, OpGet, OpPing, OpLen:
		return true
	default:
		return false
	}
}

func validStatus(r byte) bool {
	switch r {
	case StatusError, StatusNotFound, StatusOk, StatusNotLeader:
		return true
	default:
		return false
	}
}

/*

 cmd := Command{
     Op: OpSet,
     Key: []byte("name"),
     Value: []byte("Tarun"),
 }

 02
 00 00 00 04
 6E 61 6D 65
 00 00 00 05
 54 61 72 75 6E

 02
 │
 └── Opcode
     OpSet = 2


 00 00 00 04
 │
 └── Key length
     uint32 = 4 bytes


 6E 61 6D 65
 │
 └── Key bytes
     "name"


 00 00 00 05
 │
 └── Value length
     uint32 = 5 bytes


 54 61 72 75 6E
 │
 └── Value bytes
     "Tarun"

*/
