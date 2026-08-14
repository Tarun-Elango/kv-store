package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// - TestEncodeRecordRoundTrip
// - TestEncodeRecordRejectsInvalidOp
// - TestEncodeRecordRejectsTooLargeKey
// - TestEncodeRecordRejectsTooLargeValue
// - TestDecodeRecordEOF
// - TestDecodeRecordIncompleteTail
// - TestDecodeRecordChecksumMismatch
// - TestOpenCreatesFileAndDir
// - TestAppendAndReplay
// - TestReplayTruncatesIncompleteRecord
// - TestReplayAppliesRecordsInOrder
// - TestCloseFlushesAndCloses
// - TestConcurrentAppend (mutex safety)

func TestEncodeRecordRoundTrip(t *testing.T) {
	rec := Record{Index: 1, Op: byte(OpSet), Key: []byte("k"), Value: []byte("v")}
	var buf bytes.Buffer
	err := EncodeRecord(&buf, rec)
	if err != nil {
		t.Fatalf("failed encode")
	}
	got, err := DecodeRecord(&buf)
	if err != nil {
		t.Fatalf(" error in decoding record")
	}
	if got.Index != rec.Index || got.Op != rec.Op || !bytes.Equal(got.Key, rec.Key) || !bytes.Equal(got.Value, rec.Value) {
		t.Fatalf("decoded record does not match encoded record")
	}

}

func TestEncodeRecordRejectsInvalidOp(t *testing.T) {
	rec := Record{
		Index: 1,
		Op:    99,
		Key:   []byte("k"),
		Value: []byte("v"),
	}

	var buf bytes.Buffer
	err := EncodeRecord(&buf, rec)
	if !errors.Is(err, ErrUnknownOp) {
		t.Fatalf("expected ErrUnknownOp, got %v", err)
	}
}

func TestEncodeRecordRejectsTooLargeKey(t *testing.T) {
	key := make([]byte, MaxKeyLen+1)
	rec := Record{
		Index: 1,
		Op:    byte(OpSet),
		Key:   key,
		Value: []byte("v"),
	}

	var buf bytes.Buffer
	err := EncodeRecord(&buf, rec)
	if !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("expected ErrKeyTooLarge, got %v", err)
	}
}

func TestEncodeRecordRejectsTooLargeValue(t *testing.T) {
	value := make([]byte, MaxValueLen+1)
	rec := Record{
		Index: 1,
		Op:    byte(OpSet),
		Key:   []byte("k"),
		Value: value,
	}

	var buf bytes.Buffer
	err := EncodeRecord(&buf, rec)
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got %v", err)
	}
}

func TestDecodeRecordEOF(t *testing.T) {
	_, err := DecodeRecord(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestDecodeRecordIncompleteTail(t *testing.T) {
	var buf bytes.Buffer
	if err := writeUint32(&buf, 10); err != nil {
		t.Fatalf("write length: %v", err)
	}
	buf.Write([]byte{byte(OpSet), 0x00}) // partial record body

	_, err := DecodeRecord(&buf)
	if !errors.Is(err, ErrIncompleteRecord) {
		t.Fatalf("expected ErrIncompleteRecord, got %v", err)
	}
}

func TestDecodeRecordChecksumMismatch(t *testing.T) {
	var buf bytes.Buffer
	rec := Record{Index: 1, Op: byte(OpSet), Key: []byte("k"), Value: []byte("v")}
	if err := EncodeRecord(&buf, rec); err != nil {
		t.Fatalf("encode record: %v", err)
	}

	b := buf.Bytes()
	b[len(b)-1] ^= 0xFF // corrupt checksum

	_, err := DecodeRecord(bytes.NewReader(b))
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}

//

func TestOpenCreatesFileAndDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "wal", "wal.log")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer log.Close()

	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatalf("expected dir to exist: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file to exist: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read WAL header: %v", err)
	}
	if !bytes.Equal(data, walHeader) {
		t.Fatalf("WAL header = %v, want %v", data, walHeader)
	}
}

func TestAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	// 3 records
	recs := []Record{
		{Index: 1, Op: byte(OpSet), Key: []byte("a"), Value: []byte("1")},
		{Index: 2, Op: byte(OpSet), Key: []byte("b"), Value: []byte("2")},
		{Index: 3, Op: byte(OpDel), Key: []byte("a")},
	}

	// append
	for i, rec := range recs {
		if err := log.Append(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	log, err = Open(path)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer log.Close()

	var got []Record
	// replay
	if err := log.Replay(func(rec Record) error {
		got = append(got, rec)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(got) != len(recs) {
		t.Fatalf("replayed %d records, want %d", len(got), len(recs))
	}
	for i := range recs {
		if got[i].Index != recs[i].Index || got[i].Op != recs[i].Op || !bytes.Equal(got[i].Key, recs[i].Key) || !bytes.Equal(got[i].Value, recs[i].Value) {
			t.Fatalf("record %d = %#v, want %#v", i, got[i], recs[i])
		}
	}
	if gotIndex := log.LastIndex(); gotIndex != 3 {
		t.Fatalf("LastIndex() = %d, want 3", gotIndex)
	}
}

// test crash recovery
func TestReplayTruncatesIncompleteRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	// append first record
	first := Record{Index: 1, Op: byte(OpSet), Key: []byte("a"), Value: []byte("1")}
	if err := log.Append(first); err != nil {
		t.Fatalf("append valid record: %v", err)
	}
	// close
	if err := log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("open file for torn write: %v", err)
	}
	// append a torn record
	if _, err := f.Write([]byte{0, 0, 0, 10, byte(OpSet), 0x00}); err != nil {
		f.Close()
		t.Fatalf("write torn record: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close torn writer: %v", err)
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat before replay: %v", err)
	}

	log, err = Open(path)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer log.Close()

	// open, replay and check
	var got []Record
	if err := log.Replay(func(rec Record) error {
		got = append(got, rec)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after replay: %v", err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("expected replay to truncate torn tail, size before=%d after=%d", before.Size(), after.Size())
	}
	if len(got) != 1 || got[0].Op != first.Op || !bytes.Equal(got[0].Key, first.Key) || !bytes.Equal(got[0].Value, first.Value) {
		t.Fatalf("replayed records = %#v, want only %#v", got, first)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read truncated wal: %v", err)
	}
	r := bytes.NewReader(data[len(walHeader):])
	if _, err := DecodeRecord(r); err != nil {
		t.Fatalf("decode first record after truncate: %v", err)
	}
	if _, err := DecodeRecord(r); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after truncated record, got %v", err)
	}
}

// check order
// append in order, replay
func TestReplayAppliesRecordsInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer log.Close()

	recs := []Record{
		{Index: 1, Op: byte(OpSet), Key: []byte("k"), Value: []byte("1")},
		{Index: 2, Op: byte(OpSet), Key: []byte("k"), Value: []byte("2")},
		{Index: 3, Op: byte(OpDel), Key: []byte("k")},
		{Index: 4, Op: byte(OpSet), Key: []byte("k"), Value: []byte("3")},
	}
	for i, rec := range recs {
		if err := log.Append(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	var order []string
	if err := log.Replay(func(rec Record) error {
		if rec.Op == byte(OpDel) {
			order = append(order, "del")
			return nil
		}
		order = append(order, string(rec.Value))
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	want := []string{"1", "2", "del", "3"}
	if len(order) != len(want) {
		t.Fatalf("order len=%d, want %d", len(order), len(want))
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order[%d]=%q, want %q", i, order[i], want[i])
		}
	}
}

func TestCloseFlushesAndCloses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	// append record
	want := Record{Index: 1, Op: byte(OpSet), Key: []byte("k"), Value: []byte("v")}
	if err := log.Append(want); err != nil {
		t.Fatalf("append: %v", err)
	}

	// close
	if err := log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	// append after close, should error
	if err := log.Append(Record{Index: 2, Op: byte(OpSet), Key: []byte("x"), Value: []byte("y")}); err == nil {
		t.Fatal("expected append on closed log to fail")
	}

	// read file after close - to check its present
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	r := bytes.NewReader(data[len(walHeader):])
	got, err := DecodeRecord(r)
	if err != nil {
		t.Fatalf("decode record: %v", err)
	}
	if got.Index != want.Index || got.Op != want.Op || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) {
		t.Fatalf("decoded record = %#v, want %#v", got, want)
	}
	if _, err := DecodeRecord(r); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after one record, got %v", err)
	}
}

// check multiple goroutine can call append at same time
func TestConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	// open file
	log, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer log.Close()

	const n = 64
	start := make(chan struct{}) // signal channel, keeps them paused till all 64 ready
	var wg sync.WaitGroup        // lets test wait till all goroutine completed
	errCh := make(chan error, n) // channel to take errors

	// start 64 go routine
	// each appends a different record
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // wait for start channel

			key := fmt.Sprintf("key-%03d", i)
			value := bytes.Repeat([]byte{byte('a' + i%26)}, 2048)
			rec := Record{Index: uint64(i + 1), Op: byte(OpSet), Key: []byte(key), Value: value}
			if err := log.Append(rec); err != nil {
				errCh <- fmt.Errorf("append %s: %w", key, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}

	got := make(map[string]Record, n)
	r := bytes.NewReader(data[len(walHeader):])
	for {
		rec, err := DecodeRecord(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode concurrent WAL record: %v", err)
		}
		got[string(rec.Key)] = rec
	}

	if len(got) != n {
		t.Fatalf("replayed %d records, want %d", len(got), n)
	}

	// check each of 64
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%03d", i)
		rec, ok := got[key]
		if !ok {
			t.Fatalf("missing record for %s", key)
		}
		wantValue := bytes.Repeat([]byte{byte('a' + i%26)}, 2048)
		if rec.Index == 0 || rec.Op != byte(OpSet) || !bytes.Equal(rec.Key, []byte(key)) || !bytes.Equal(rec.Value, wantValue) {
			t.Fatalf("record %s = %#v, want op=%d value len=%d", key, rec, OpSet, len(wantValue))
		}
	}
}

// TestOpenRejectsLegacyWAL verifies that an unversioned WAL is rejected.
func TestOpenRejectsLegacyWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.wal")
	if err := os.WriteFile(path, []byte("legacy WAL"), 0600); err != nil {
		t.Fatalf("write legacy WAL: %v", err)
	}

	_, err := Open(path)
	if !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("Open() error = %v, want ErrUnsupportedFormat", err)
	}
}

// TestEncodeRecordRejectsZeroIndex verifies that records must have a valid log index.
func TestEncodeRecordRejectsZeroIndex(t *testing.T) {
	var buf bytes.Buffer
	err := EncodeRecord(&buf, Record{Op: byte(OpSet), Key: []byte("k"), Value: []byte("v")})
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("EncodeRecord() error = %v, want ErrInvalidIndex", err)
	}
}

// TestReplayRestoresLastIndex verifies that replay recovers the highest persisted index.
func TestReplayRestoresLastIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := log.Append(Record{Index: 7, Op: byte(OpSet), Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatalf("append record: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	log, err = Open(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer log.Close()

	if err := log.Replay(func(Record) error { return nil }); err != nil {
		t.Fatalf("replay WAL: %v", err)
	}
	if got := log.LastIndex(); got != 7 {
		t.Fatalf("LastIndex() = %d, want 7", got)
	}
}
