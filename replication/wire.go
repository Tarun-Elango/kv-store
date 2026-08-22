package replication

const (
	messageProtocol byte = 1

	//	when we read message type byte, we can decode either req or resp
	messageAppendRequest  byte = 1
	messageAppendResponse byte = 2

	maxReplicationFrame = 64 << 20 // 64 * 2^20 = 64 MB ( max message size )
)

/*
u32 frame length
u8 protocol version
u8 message type
message payload
*/

// appendrequest struct in types.go

// append response struct in types.go
