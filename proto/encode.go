package proto

import (
	"encoding/binary"
	"io"
)

// w - bytes written to ( anything with Write() func)
// cmd - data to be sent
// example:  file, _ := os.Create("data.bin")
//
//	EncodeCommand(file, cmd)
//
// [1 byte op][4 bytes keyLen BE][key][4 bytes valLen BE][value]
// 02 				<- opcode
// 00 00 00 04 		<- key length BE
// 6E 61 6D 65		<- Key
// 00 00 00 05		<- value length BE
// 54 61 72 75 6E	<- Value
func EncodeCommand(w io.Writer, cmd Command) error {
	if len(cmd.Key) > MaxKeyLen {
		return ErrKeyTooLarge
	}

	if len(cmd.Value) > MaxValueLen {
		return ErrValueTooLarge
	}

	if !ValidOpcode(cmd.Op) {
		return ErrUnknownOpcode
	}

	if _, err := w.Write([]byte{cmd.Op}); err != nil { // cmd.Op is single byte, []byte{cmd.Op} -> creates slice of 1byte
		return err
	}
	if err := writeUint32(w, uint32(len(cmd.Key))); err != nil { // int needs to be converted to bytes array
		return err
	}

	if len(cmd.Key) > 0 {
		if _, err := w.Write(cmd.Key); err != nil {
			return err
		}
	}

	if err := writeUint32(w, uint32(len(cmd.Value))); err != nil {
		return err
	}
	if len(cmd.Value) > 0 {
		if _, err := w.Write(cmd.Value); err != nil {
			return err
		}
	}

	return nil
}

// [1 byte status][4 bytes valLen BE][value]
// 02                  <- Status (1 byte)
// 00 00 00 04         <- Value length = 4 (uint32 big endian)
// 6E 61 6D 65         <- "name" bytes
func EncodeResponse(w io.Writer, resp Response) error {
	if len(resp.Value) > MaxValueLen {
		return ErrValueTooLarge
	}
	if !validStatus(resp.Status) {
		return ErrUnknownStatus
	}

	if _, err := w.Write([]byte{resp.Status}); err != nil {
		return err
	}

	if err := writeUint32(w, uint32(len(resp.Value))); err != nil {
		return err
	}
	if len(resp.Value) > 0 {
		if _, err := w.Write(resp.Value); err != nil {
			return err
		}
	}

	return nil
}

// Because uint32 is a number that must be converted into a fixed 4-byte format,
// while string/[]byte is already bytes and can be written directly.
func writeUint32(w io.Writer, v uint32) error {
	var buf [4]byte                       // 4 byte array
	binary.BigEndian.PutUint32(buf[:], v) // v into byte slices  ( big endian order) like  6E 61 6D 65
	_, err := w.Write(buf[:])             //writes to writer
	return err
}
