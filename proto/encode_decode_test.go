package proto

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestRoundTripCommand(t *testing.T) {
	largeValue := make([]byte, 1<<20) // 1MB ( 1 shift left 20 times in binary)
	if _, err := rand.Read(largeValue); err != nil {
		t.Fatalf("failed to generate random value: %v", err)
	}
	tests := []struct {
		name string
		cmd  Command
	}{
		{"get with normal key", Command{OpGet, []byte("hello"), nil}},
		{"set with normal key+value", Command{OpSet, []byte("hello"), []byte("world")}},
		{"delete", Command{OpDel, []byte("hello"), nil}},
		{"ping, no key no value", Command{OpPing, nil, nil}},
		{"empty key", Command{OpSet, []byte(""), []byte("value")}},
		{"empty value on set", Command{OpSet, []byte("key"), []byte("")}},
		{"empty key and empty value", Command{OpSet, []byte(""), []byte("")}},
		{"max length key", Command{OpSet, []byte(strings.Repeat("k", MaxKeyLen)), []byte("v")}},
		{"single byte value", Command{OpSet, []byte("k"), []byte("x")}},
		{"large value 1MB", Command{OpSet, []byte("k"), largeValue}},
		{"binary non-utf8 key bytes", Command{OpSet, []byte{0x00, 0xFF, 0x01}, []byte("v")}},
		{"binary value with embedded null bytes", Command{OpSet, []byte("k"), []byte{0x41, 0x00, 0x42, 0x00, 0x43}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := &bytes.Buffer{} // empty memory buffer from bytes pkg

			if err := EncodeCommand(buf, tc.cmd); err != nil {
				t.Fatalf("EncodeCommand returned unexpected error: %v", err)
			}

			got, err := DecodeCommand(buf) // decode buffer, that we added
			if err != nil {
				t.Fatalf("DecodeCommand returned unexpected error: %v", err)
			}
			// check variables
			if got.Op != tc.cmd.Op {
				t.Errorf("Op = %d, want %d", got.Op, tc.cmd.Op)
			}
			if !bytes.Equal(got.Key, tc.cmd.Key) {
				t.Errorf("Key = %v, want %v", got.Key, tc.cmd.Key)
			}
			if !bytes.Equal(got.Value, tc.cmd.Value) {
				t.Errorf("Value = %v, want %v", got.Value, tc.cmd.Value)
			}

			// at this point empty buffer
			if buf.Len() != 0 {
				t.Errorf("buffer has %d leftover bytes after decode", buf.Len())
			}

		})
	}
}

func TestEncodeKeyTooLarge(t *testing.T) {
	cmd := Command{
		Op:  OpSet,
		Key: []byte(strings.Repeat("K", MaxKeyLen+1)),
	}
	buf := &bytes.Buffer{}
	err := EncodeCommand(buf, cmd)

	if !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("EncodeCommand error = %v, want ErrKeyTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Errorf("expected no bytes written on rejected encode, got %d bytes", buf.Len())
	}
}

func TestEncodeCommandValueTooLarge(t *testing.T) {
	cmd := Command{
		Op:    OpSet,
		Key:   []byte("K"),
		Value: []byte(strings.Repeat("K", MaxValueLen+1)),
	}
	buf := &bytes.Buffer{}
	err := EncodeCommand(buf, cmd)

	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("EncodeCommand error = %v, want ErrValueTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("buffer has %d bytes, want empty buffer", buf.Len())
	}
}

// tests bad or incomplete binary protocol data
func TestDecodeCommandTruncation(t *testing.T) {

	t.Run("truncated after opcode", func(t *testing.T) {
		buf := bytes.NewBuffer([]byte{OpSet}) // just opcode
		_, err := DecodeCommand(buf)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var ferr *FrameError
		if !errors.As(err, &ferr) {
			t.Errorf("expected *FrameError, got %T: %v", err, err)
		}
	})

	t.Run("truncated in middle of key", func(t *testing.T) {
		buf := &bytes.Buffer{}
		buf.WriteByte(OpSet)
		writeRawUint32(buf, 10)             // key is 10 bytes long
		buf.Write([]byte{0x01, 0x02, 0x03}) // cut midway
		_, err := DecodeCommand(buf)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var frameErr *FrameError
		if !errors.As(err, &frameErr) {
			t.Errorf("expected *FrameError, got %T: %v", err, err)
		}
	})

	t.Run("truncated after key, before value length", func(t *testing.T) {
		buf := &bytes.Buffer{}
		buf.WriteByte(OpSet)
		writeRawUint32(buf, 3)
		buf.Write([]byte("abc"))
		// stop here -- no value length field at all

		_, err := DecodeCommand(buf)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var frameErr *FrameError
		if !errors.As(err, &frameErr) {
			t.Errorf("expected *FrameError, got %T: %v", err, err)
		}
	})

	t.Run("unknown opcode byte", func(t *testing.T) {
		buf := bytes.NewBuffer([]byte{99}) // not a valid opcode

		_, err := DecodeCommand(buf)
		if !errors.Is(err, ErrUnknownOpcode) {
			t.Fatalf("error = %v, want ErrUnknownOpcode", err)
		}
	})

	t.Run("keyLen exceeds MaxKeyLen in encoded bytes", func(t *testing.T) {
		buf := &bytes.Buffer{}
		buf.WriteByte(OpSet)
		writeRawUint32(buf, uint32(MaxKeyLen+1)) // hand-crafted bad length

		_, err := DecodeCommand(buf)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		var frameErr *FrameError
		if errors.As(err, &frameErr) {
			t.Errorf("expected length-validation error, got FrameError: %v", err)
		}
	})
}

// helper: creating frames byte by byte, regardless of encode func
func writeRawUint32(buf *bytes.Buffer, v uint32) {
	var b [4]byte                       // 4 byte slice
	binary.BigEndian.PutUint32(b[:], v) // val into 4 bytes BE
	buf.Write(b[:])                     // add 4 bytes to end of buffer
}
