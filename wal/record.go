package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

type Op byte

const (
	OpSet Op = 1
	OpDel Op = 2
)

type Record struct {
	Op    byte
	Key   []byte
	Value []byte
}

const (
	MaxKeyLen   = 65535            // uint16 max (65,535 bytes)
	MaxValueLen = 16 * 1024 * 1024 // 16 MB - uint32, 16 mil bytes = 16 Mb ( uint32 can hold the number 16 mil)
)

// maxRecordLen guards against a corrupted or bogus length prefix causing
// a huge allocation. Generous upper bound on payload size:
// 1 (op) + 4 (keyLen) + MaxKeyLen + 4 (valLen) + MaxValueLen
const maxRecordLen = 1 + 4 + MaxKeyLen + 4 + MaxValueLen

var (
	ErrUnknownOp        = errors.New("wal: unknown operation")
	ErrKeyTooLarge      = errors.New("wal: key exceeds max length")
	ErrValueTooLarge    = errors.New("wal: value exceeds max length")
	ErrIncompleteRecord = errors.New("wal: incomplete trailing record") // crash mid-write, recoverable
	ErrChecksumMismatch = errors.New("wal: checksum mismatch")          // real corruption, fatal
)

// [uint32 recordLen][payload][uint32 CRC32(payload)]
func EncodeRecord(w io.Writer, rec Record) error {
	if rec.Op != byte(OpSet) && rec.Op != byte(OpDel) {
		return ErrUnknownOp
	}
	if len(rec.Key) > MaxKeyLen {
		return ErrKeyTooLarge
	}
	if len(rec.Value) > MaxValueLen {
		return ErrValueTooLarge
	}

	payload := encodePayload(rec)
	checkSum := crc32.ChecksumIEEE(payload)

	// payload to w
	if err := writeUint32(w, uint32(len(payload))); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	if err := writeUint32(w, checkSum); err != nil {
		return err
	}

	return nil
}

func encodePayload(rec Record) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(rec.Op))
	writeUint32(buf, uint32(len(rec.Key)))
	buf.Write(rec.Key)
	writeUint32(buf, uint32(len(rec.Value)))
	buf.Write(rec.Value)
	return buf.Bytes()

}

// decode one [len][payload][crc] unit from r.
// 1. io.EOF - clean end of log, nothing more to read
// 2. ErrIncompleteRecord - torn write, truncate here
// 3. ErrChecksumMismatch - real corruption, fail startup
func DecodeRecord(r io.Reader) (Record, error) {
	recordLen, err := readUint32(r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// zero bytes read before hitting EOF -> clean end of log
			return Record{}, io.EOF
		}
		// some but not all 4 length-prefix bytes were present
		return Record{}, ErrIncompleteRecord
	}
	if recordLen > maxRecordLen {
		return Record{}, fmt.Errorf("%w: record length %d exceeds max %d",
			ErrChecksumMismatch, recordLen, maxRecordLen)
	}

	payload := make([]byte, recordLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		//some bytes not found
		return Record{}, ErrIncompleteRecord
	}

	wantChecksum, err := readUint32(r)
	if err != nil {
		// payload was fully present but the trailing checksum itself
		// got cut off -> still counts as an incomplete tail
		return Record{}, ErrIncompleteRecord
	}
	gotChecksum := crc32.ChecksumIEEE(payload)
	if gotChecksum != wantChecksum { // everything expected was physically present on disk, but the
		// bytes don't match their own checksum — real corruption
		return Record{}, fmt.Errorf("%w: got %d want %d",
			ErrChecksumMismatch, gotChecksum, wantChecksum)
	}
	rec, err := decodePayload(payload)
	if err != nil {
		return Record{}, err
	}

	return rec, nil

}

// decodePayload parses [op][keyLen][key][valLen][value] from an
// already-fully-read, already-checksum-verified byte slice.
func decodePayload(payload []byte) (Record, error) {
	r := bytes.NewReader(payload)
	opByte, err := r.ReadByte()

	if err != nil {
		// shouldn't happen post-checksum verification, but defensive
		return Record{}, ErrIncompleteRecord
	}
	op := Op(opByte)
	if op != OpSet && op != OpDel {
		return Record{}, ErrUnknownOp
	}
	keyLen, err := readUint32(r)
	if err != nil {
		return Record{}, ErrIncompleteRecord
	}
	if keyLen > MaxKeyLen {
		// validate before allocating
		return Record{}, ErrKeyTooLarge
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return Record{}, ErrIncompleteRecord
	}

	valLen, err := readUint32(r)
	if err != nil {
		return Record{}, ErrIncompleteRecord
	}
	if valLen > MaxValueLen {
		// validate before allocating
		return Record{}, ErrValueTooLarge
	}

	value := make([]byte, valLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return Record{}, ErrIncompleteRecord
	}
	return Record{Op: byte(op), Key: key, Value: value}, nil

}

// Because uint32 is a number that must be converted into a fixed 4-byte format,
// while string/[]byte is already bytes and can be written directly.
func writeUint32(w io.Writer, v uint32) error {
	var buf [4]byte                       // 4 byte array
	binary.BigEndian.PutUint32(buf[:], v) // v into byte slices  ( big endian order) like  6E 61 6D 65
	_, err := w.Write(buf[:])             //writes to writer
	return err
}

// readUint32 reads a big-endian uint32. Returns io.EOF only when zero
// bytes were read before hitting end-of-stream; any partial read is
// surfaced as io.ErrUnexpectedEOF by io.ReadFull.
func readUint32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

//
