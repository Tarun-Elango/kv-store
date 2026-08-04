package proto

import (
	"encoding/binary"
	"fmt"
	"io"
)

// wrap decode failure that happened mid-frame
type FrameError struct {
	Stage string // which part of the frame we were reading, e.g. "key", "value length"
	Err   error
}

// to match go's build in interface, and we can add out extra variables
func (e *FrameError) Error() string {
	return fmt.Sprintf("proto: truncated frame while reading %s: %v", e.Stage, e.Err)
	// will be returned when caller does fmt.Println(err)
}

func (e *FrameError) Unwrap() error {
	return e.Err
}

// reads one command frame from r
// stream close between frames
func DecodeCommand(r io.Reader) (Command, error) {
	var opBuf [1]byte
	// reading opcode
	if _, err := io.ReadFull(r, opBuf[:]); err != nil {
		// if nothing read -. clean EOF
		if err == io.EOF {
			return Command{}, io.EOF
		}
		// fail mid frame
		return Command{}, &FrameError{Stage: "opcode", Err: err}
	}

	op := opBuf[0]
	if !ValidOpcode(op) {
		return Command{}, ErrUnknownOpcode
	}

	key, err := readLengthPrefixed(r, MaxKeyLen, "key")
	if err != nil {
		return Command{}, err
	}

	value, err := readLengthPrefixed(r, MaxValueLen, "value")
	if err != nil {
		return Command{}, err
	}
	return Command{Op: op, Key: key, Value: value}, nil
}

// Reads one Response from r, same EOF semantics as above
// read full will send EOF when server sends nothing
func DecodeResponse(r io.Reader) (Response, error) {
	var statusBuf [1]byte
	if _, err := io.ReadFull(r, statusBuf[:]); err != nil {
		if err == io.EOF {
			return Response{}, io.EOF // server closed before a response began
		}
		return Response{}, &FrameError{Stage: "status", Err: err}
	}

	status := statusBuf[0]
	if !validStatus(status) {
		return Response{}, ErrUnknownStatus
	}

	value, err := readLengthPrefixed(r, MaxValueLen, "value")
	if err != nil {
		return Response{}, err
	}

	return Response{Status: status, Value: value}, nil
}

// reads uint32 BE length, checks against max,
// then reads that many bytes
func readLengthPrefixed(r io.Reader, max int, label string) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, &FrameError{Stage: label + " length", Err: err}
	}

	n := binary.BigEndian.Uint32(lenBuf[:]) //size
	if n > uint32(max) {
		return nil, fmt.Errorf("proto: %s length %d exceeds max %d", label, n, max)
	}
	if n == 0 {
		return []byte{}, nil
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, &FrameError{Stage: label, Err: err}
	}

	return buf, nil
}
