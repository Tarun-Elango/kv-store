package replication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"kvStore/proto"
	"math"
)

//*
//u32 framelength
// u8 protocollength
// message type
// message payload
//

const (
	maxLeaderIDLength     = 256
	maxErrorMessageLength = 64 << 10
	maxEntriesPerRequest  = 1024
)

const maxAppendRequestPayload = maxReplicationFrame - 2 // max payload 64MiB -2 bytes

// -2 because the frame also contains:
// 1 byte protocol version + 1 byte message type

func encodedEntrySize(entry Entry) int {
	// index + operation + key length + key + value length + value
	return 8 + 1 + 4 + len(entry.Command.Key) + 4 + len(entry.Command.Value)
}

// if i append theis appendrequest, how many bytes will payload take ?, i.e checks size
func appendRequestPayloadSize(req AppendRequest) int {
	// leader ID length + leader ID + previous index +
	// previous-entry flag + entry count
	size := 2 + len(req.LeaderID) + 8 + 1 + 4

	if req.PrevEntry != nil {
		size += encodedEntrySize(*req.PrevEntry)
	}

	for _, entry := range req.Entries {
		size += encodedEntrySize(entry)
	}

	return size
}

// EncodeAppendRequest writes:
//
//	u32 frame length
//	u8  protocol version
//	u8  message type
//	u16 leader ID length
//	leader ID bytes
//	u64 previous index
//	u8  has previous entry
//	previous entry, if present
//	u32 entry count
//	entries
func EncodeAppendRequest(w io.Writer, req AppendRequest) error {
	if err := validateAppendRequest(req); err != nil {
		return err
	}

	// check len of entries
	if len(req.Entries) > maxEntriesPerRequest {
		return fmt.Errorf(
			"too many entries: got %d max %d",
			len(req.Entries),
			maxEntriesPerRequest,
		)
	}

	// check is payload of request valid
	payloadSize := appendRequestPayloadSize(req)
	if payloadSize > maxAppendRequestPayload {
		return fmt.Errorf(
			"append request is too large: got %d bytes, max %d",
			payloadSize,
			maxAppendRequestPayload,
		)
	}

	var payload bytes.Buffer

	if err := writeU16(&payload, uint16(len(req.LeaderID))); err != nil {
		return err
	}

	if err := writeFull(&payload, []byte(req.LeaderID)); err != nil {
		return err
	}

	if err := writeU64(&payload, req.PrevIndex); err != nil {
		return err
	}

	if req.PrevEntry == nil { // its pointer
		// 0 byte
		if err := writeU8(&payload, 0); err != nil {
			return err
		}
	} else {
		if err := writeU8(&payload, 1); err != nil {
			return err
		}

		if err := encodeEntry(&payload, *req.PrevEntry); err != nil {
			return err
		}
	}

	if err := writeU32(&payload, uint32(len(req.Entries))); err != nil {
		return err
	}

	for _, entry := range req.Entries {
		if err := encodeEntry(&payload, entry); err != nil {
			return err
		}
	}

	return writeFrame(w, messageAppendRequest, payload.Bytes())

}

func DecodeAppendRequest(r io.Reader) (AppendRequest, error) {
	frame, err := readFrame(r)
	if err != nil {
		return AppendRequest{}, err
	}

	if frame.protocolVersion != messageProtocol {
		return AppendRequest{}, fmt.Errorf(
			"unsupported protocol version: got %d want %d",
			frame.protocolVersion,
			messageProtocol,
		)
	}

	if frame.messageType != messageAppendRequest {
		return AppendRequest{}, fmt.Errorf(
			"unexpected message type: got %d want append request",
			frame.messageType,
		)
	}

	reader := bytes.NewReader(frame.payload) // byte slice to reader, so we can read fields sequentially

	leaderIDLength, err := readU16(reader)
	if err != nil {
		return AppendRequest{}, fmt.Errorf(
			"read leader ID length: %w",
			err,
		)
	}

	if leaderIDLength > maxLeaderIDLength {
		return AppendRequest{}, errors.New("leader ID is too long")
	}

	leaderIDBytes, err := readBytes(reader, uint32(leaderIDLength))
	if err != nil {
		return AppendRequest{}, fmt.Errorf(
			"read leader ID: %w",
			err,
		)
	}

	prevIndex, err := readU64(reader)
	if err != nil {
		return AppendRequest{}, fmt.Errorf(
			"read previous index: %w",
			err,
		)
	}
	hasPreviousEntry, err := readU8(reader)
	if err != nil {
		return AppendRequest{}, fmt.Errorf(
			"read previous-entry flag: %w",
			err,
		)
	}

	var previousEntry *Entry

	switch hasPreviousEntry {
	case 0:
		// nothing

	case 1:
		entry, err := decodeEntry(reader)
		if err != nil {
			return AppendRequest{}, fmt.Errorf(
				"decode previous entry: %w",
				err,
			)
		}

		previousEntry = &entry
	default:
		return AppendRequest{}, fmt.Errorf(
			"invalid previous-entry flag: %d",
			hasPreviousEntry,
		)
	}

	entryCount, err := readU32(reader)
	if err != nil {
		return AppendRequest{}, fmt.Errorf(
			"read entry count: %w",
			err,
		)
	}
	if entryCount > maxEntriesPerRequest {
		return AppendRequest{}, fmt.Errorf(
			"too many entries: got %d max %d",
			entryCount,
			maxEntriesPerRequest,
		)
	}

	entries := make([]Entry, entryCount)

	for i := range entries {
		entry, err := decodeEntry(reader)

		if err != nil {
			return AppendRequest{}, fmt.Errorf(
				"decode entry %d: %w",
				i,
				err,
			)
		}
		entries[i] = entry
	}

	if reader.Len() != 0 {
		return AppendRequest{}, fmt.Errorf(
			"append request contains %d trailing bytes",
			reader.Len(),
		)
	}

	req := AppendRequest{
		LeaderID:  string(leaderIDBytes),
		PrevIndex: prevIndex,
		PrevEntry: previousEntry,
		Entries:   entries,
	}
	if err := validateAppendRequest(req); err != nil {
		return AppendRequest{}, err
	}

	return req, nil

}

// EncodeAppendResponse writes:
//
//	u32 frame length
//	u8  protocol version
//	u8  message type
//	u8  success
//	u64 last index
//	u8  error code
//	u32 error message length
//	error message bytes
func EncodeAppendResponse(w io.Writer, res AppendResponse) error {
	if err := validateAppendResponse(res); err != nil {
		return err
	}

	var payload bytes.Buffer
	var success byte

	if res.Success {
		success = 1
	}
	if err := writeU8(&payload, success); err != nil {
		return err
	}

	if err := writeU64(&payload, res.LastIndex); err != nil {
		return err
	}

	errorCode, err := encodeErrorCode(res.Code)
	if err != nil {
		return err
	}
	if err := writeU8(&payload, errorCode); err != nil {
		return err
	}
	if err := writeU32(&payload, uint32(len(res.Error))); err != nil {
		return err
	}

	if err := writeFull(&payload, []byte(res.Error)); err != nil {
		return err
	}

	return writeFrame(w, messageAppendResponse, payload.Bytes())
}

func DecodeAppendResponse(r io.Reader) (AppendResponse, error) {
	frame, err := readFrame(r)
	if err != nil {
		return AppendResponse{}, err
	}

	if frame.protocolVersion != messageProtocol {
		return AppendResponse{}, fmt.Errorf(
			"unsupported protocol version: got %d want %d",
			frame.protocolVersion,
			messageProtocol,
		)
	}

	if frame.messageType != messageAppendResponse {
		return AppendResponse{}, fmt.Errorf(
			"unexpected message type: got %d want append response",
			frame.messageType,
		)
	}

	reader := bytes.NewReader(frame.payload) // make sure we play with current payload, and not extra from the tcp stream
	successByte, err := readU8(reader)
	if err != nil {
		return AppendResponse{}, fmt.Errorf(
			"read success flag: %w",
			err,
		)
	}

	if successByte != 0 && successByte != 1 {
		return AppendResponse{}, fmt.Errorf(
			"invalid success flag: %d",
			successByte,
		)
	}
	lastIndex, err := readU64(reader)
	if err != nil {
		return AppendResponse{}, fmt.Errorf(
			"read last index: %w",
			err,
		)
	}

	errorCodeByte, err := readU8(reader)
	if err != nil {
		return AppendResponse{}, fmt.Errorf(
			"read error code: %w",
			err,
		)
	}
	errorCode, err := decodeErrorCode(errorCodeByte)
	if err != nil {
		return AppendResponse{}, err
	}

	errorLength, err := readU32(reader)
	if err != nil {
		return AppendResponse{}, fmt.Errorf(
			"read error length: %w",
			err,
		)
	}
	if errorLength > maxErrorMessageLength {
		return AppendResponse{}, errors.New(
			"error message is too long",
		)
	}

	errorBytes, err := readBytes(reader, errorLength)
	if err != nil {
		return AppendResponse{}, fmt.Errorf(
			"read error message: %w",
			err,
		)
	}

	if reader.Len() != 0 {
		return AppendResponse{}, fmt.Errorf(
			"append response contains %d trailing bytes",
			reader.Len(),
		)
	}

	res := AppendResponse{
		Success:   successByte == 1,
		LastIndex: lastIndex,
		Code:      errorCode,
		Error:     string(errorBytes),
	}

	if err := validateAppendResponse(res); err != nil {
		return AppendResponse{}, err
	}

	return res, nil
}

func encodeEntry(w io.Writer, entry Entry) error {
	if err := writeU64(w, entry.Index); err != nil {
		return err
	}

	if err := writeU8(w, entry.Command.Op); err != nil {
		return err
	}

	if err := writeU32(w, uint32(len(entry.Command.Key))); err != nil {
		return err
	}

	if err := writeFull(w, entry.Command.Key); err != nil {
		return err
	}

	if err := writeU32(w, uint32(len(entry.Command.Value))); err != nil {
		return err
	}

	if err := writeFull(w, entry.Command.Value); err != nil {
		return err
	}

	return nil

}

func decodeEntry(r io.Reader) (Entry, error) {
	index, err := readU64(r)
	if err != nil {
		return Entry{}, fmt.Errorf("read entry index: %w", err)
	}

	op, err := readU8(r)
	if err != nil {
		return Entry{}, fmt.Errorf("read command operation: %w", err)
	}

	keyLength, err := readU32(r)
	if err != nil {
		return Entry{}, fmt.Errorf("read key length: %w", err)
	}
	if keyLength > proto.MaxKeyLen {
		return Entry{}, fmt.Errorf(
			"key is too large: got %d max %d",
			keyLength,
			proto.MaxKeyLen,
		)
	}
	key, err := readBytes(r, keyLength)
	if err != nil {
		return Entry{}, fmt.Errorf("read key: %w", err)
	}
	valueLength, err := readU32(r)
	if err != nil {
		return Entry{}, fmt.Errorf("read value length: %w", err)
	}

	if valueLength > proto.MaxValueLen {
		return Entry{}, fmt.Errorf(
			"value is too large: got %d max %d",
			valueLength,
			proto.MaxValueLen,
		)
	}

	value, err := readBytes(r, valueLength)
	if err != nil {
		return Entry{}, fmt.Errorf("read value: %w", err)
	}
	entry := Entry{
		Index: index,
		Command: proto.Command{
			Op:    op,
			Key:   key,
			Value: value,
		},
	}

	if err := validateEntry(entry); err != nil {
		return Entry{}, err
	}

	return entry, nil
}

// check structure, and each entry - if they are good or not ( leader to follower)
func validateAppendRequest(req AppendRequest) error {
	if req.LeaderID == "" {
		return errors.New("leader ID is required")
	}

	if len(req.LeaderID) > maxLeaderIDLength {
		return errors.New("leader ID is too long")
	}

	if req.PrevIndex == 0 {
		if req.PrevEntry != nil {
			return errors.New(
				"previous entry must be nil when previous index is zero",
			)
		}
	} else {
		if req.PrevEntry == nil {
			return errors.New(
				"previous entry is required when previous index is non-zero",
			)
		}

		if req.PrevEntry.Index != req.PrevIndex {
			return fmt.Errorf(
				"previous entry index mismatch: got %d want %d",
				req.PrevEntry.Index,
				req.PrevIndex,
			)
		}

		if err := validateEntry(*req.PrevEntry); err != nil { // prevEntry could be null, pointer helps represent both
			return fmt.Errorf("invalid previous entry: %w", err)
		}

	}

	expectedIndex := req.PrevIndex + 1 //prev is right before entries being appended

	// check uint64 overflow
	if req.PrevIndex == math.MaxUint64 && len(req.Entries) > 0 {
		return errors.New("entry index overflow")
	}

	for i, entry := range req.Entries {
		if entry.Index != expectedIndex {
			return fmt.Errorf(
				"entry %d has index %d, want %d",
				i,
				entry.Index,
				expectedIndex,
			)
		}

		if err := validateEntry(entry); err != nil {
			return fmt.Errorf("invalid entry %d: %w", i, err)
		}

		expectedIndex++

		if expectedIndex == 0 {
			return errors.New("entry index overflow")
		}
	}
	return nil
}

func validateAppendResponse(res AppendResponse) error {
	if !validAppendErrorCode(res.Code) {
		return fmt.Errorf("unknown append error code: %q", res.Code)
	}

	if len(res.Error) > maxErrorMessageLength {
		return errors.New("error message is too long")
	}

	if res.Success {
		if res.Code != AppendErrorNone {
			return errors.New(
				"successful response cannot contain an error code",
			)
		}

		if res.Error != "" {
			return errors.New(
				"successful response cannot contain an error message",
			)
		}

		return nil
	}

	// failed respoinse
	if res.Code == AppendErrorNone {
		return errors.New(
			"failed response must contain an error code",
		)
	}
	if res.Error == "" {
		return errors.New(
			"failed response must contain an error message",
		)
	}

	return nil
}

func validateEntry(entry Entry) error {
	if entry.Index == 0 {
		return errors.New("entry index must be greater than zero")
	}

	return validateCommand(entry.Command)

}

func validAppendErrorCode(code AppendErrorCode) bool {
	switch code {
	case AppendErrorNone,
		AppendErrorInvalid,
		AppendErrorGap,
		AppendErrorConflict,
		AppendErrorInternal:
		return true
	default:
		return false
	}
}

func encodeErrorCode(code AppendErrorCode) (byte, error) {
	switch code {
	case AppendErrorNone:
		return 0, nil
	case AppendErrorInvalid:
		return 1, nil
	case AppendErrorGap:
		return 2, nil
	case AppendErrorConflict:
		return 3, nil
	case AppendErrorInternal:
		return 4, nil
	default:
		return 0, fmt.Errorf(
			"unknown append error code: %q",
			code,
		)
	}
}

func decodeErrorCode(code byte) (AppendErrorCode, error) {
	switch code {
	case 0:
		return AppendErrorNone, nil
	case 1:
		return AppendErrorInvalid, nil
	case 2:
		return AppendErrorGap, nil
	case 3:
		return AppendErrorConflict, nil
	case 4:
		return AppendErrorInternal, nil
	default:
		return AppendErrorNone, fmt.Errorf(
			"unknown append error code: %d",
			code,
		)
	}
}

type replicationFrame struct {
	protocolVersion byte
	messageType     byte
	payload         []byte
}

// writes (len, protocolversion, type, payload) to conn
func writeFrame(
	w io.Writer,
	messageType byte,
	payload []byte,
) error {
	frameLength := 2 + len(payload)
	if frameLength > maxReplicationFrame {
		return fmt.Errorf(
			"frame too large: got %d max %d",
			frameLength,
			maxReplicationFrame,
		)
	}

	if err := writeU32(w, uint32(frameLength)); err != nil {
		return err
	}

	if err := writeU8(w, messageProtocol); err != nil {
		return err
	}

	if err := writeU8(w, messageType); err != nil {
		return err
	}

	return writeFull(w, payload)
}

func readFrame(r io.Reader) (replicationFrame, error) {
	frameLength, err := readU32(r)

	if err != nil {
		return replicationFrame{}, fmt.Errorf(
			"read frame length: %w",
			err,
		)
	}

	if frameLength < 2 {
		return replicationFrame{}, errors.New(
			"frame is too small",
		)
	}

	// just in case conversion, dont need it

	if uint64(frameLength) > uint64(maxReplicationFrame) {
		return replicationFrame{}, fmt.Errorf(
			"frame too large: got %d max %d",
			frameLength,
			maxReplicationFrame,
		)
	}

	framebytes, err := readBytes(r, frameLength)
	if err != nil {
		return replicationFrame{}, fmt.Errorf(
			"read frame: %w",
			err,
		)
	}

	return replicationFrame{
		protocolVersion: framebytes[0],
		messageType:     framebytes[1],
		payload:         framebytes[2:],
	}, nil

}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data) // n tells how many writte
		if n > 0 {              // if left
			data = data[n:]
		}
		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func writeU8(w io.Writer, value uint8) error {
	var buf [1]byte
	buf[0] = value
	return writeFull(w, buf[:])
}

func writeU16(w io.Writer, value uint16) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], value)
	return writeFull(w, buf[:])
}

func writeU32(w io.Writer, value uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)

	return writeFull(w, buf[:])
}

func writeU64(w io.Writer, value uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)

	return writeFull(w, buf[:])
}

func readU8(r io.Reader) (uint8, error) {
	var buf [1]byte

	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}

	return buf[0], nil
}

func readU16(r io.Reader) (uint16, error) {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint16(buf[:]), nil
}

func readU32(r io.Reader) (uint32, error) {
	var buf [4]byte

	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf[:]), nil
}

func readU64(r io.Reader) (uint64, error) {
	var buf [8]byte

	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(buf[:]), nil
}

// read bytes size length
func readBytes(r io.Reader, length uint32) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}

	data := make([]byte, int(length))

	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return data, nil
}
