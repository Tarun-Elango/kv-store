package wal

// owns the wal file and api to interact
import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Identifies the WAL format and version so incompatible or legacy files can be rejected safely.
// bytes added to top of file as a indicator
var walHeader = []byte{'K', 'V', 'W', 'L', 1}

var ErrUnsupportedFormat = errors.New(
	"wal: unsupported format; migrate or delete the existing WAL",
)

type Log struct {
	file      *os.File // represent open file descriptor
	muWal     sync.Mutex
	lastIndex uint64 // highest log index in WAL
}

// ensureHeader writes the WAL format header for a new file or validates it for an existing file.
func ensureHeader(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		// newly created WAL file
		n, err := file.Write(walHeader)
		if err != nil {
			return err
		}

		if n != len(walHeader) {
			return io.ErrShortWrite
		}
		return file.Sync()
	}

	got := make([]byte, len(walHeader))
	n, err := file.ReadAt(got, 0) // read at offset 0 -> got list
	if err != nil || n != len(walHeader) || !bytes.Equal(got, walHeader) {
		return fmt.Errorf("%w: expected WAL v1 header", ErrUnsupportedFormat)
	}
	return nil
}

// open
// O_APPEND -> write atomic to end of file, no overwrite
func Open(path string) (*Log, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("Failed to make dir: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file: %w", err)
	}

	if err := ensureHeader(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Log{file: f}, nil
}

// append
// caller to not treat command as durable until return err
func (l *Log) Append(rec Record) error {
	l.muWal.Lock()
	defer l.muWal.Unlock()

	if err := EncodeRecord(l.file, rec); err != nil {
		return fmt.Errorf("Failed to append: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	l.lastIndex = rec.Index
	return nil
}

// Replay + callback func, apply all commands from file to memory
func (l *Log) Replay(apply func(Record) error) error {
	l.muWal.Lock()
	defer l.muWal.Unlock()

	if _, err := l.file.Seek(int64(len(walHeader)), io.SeekStart); err != nil {
		return fmt.Errorf("failed to move cursor: %w", err)
	}
	l.lastIndex = 0 // set to 0
	for {
		// get the current position, before decoding
		pos, err := l.file.Seek(0, io.SeekCurrent) // just tells
		if err != nil {
			return fmt.Errorf("failed to get WAL position: %w", err)
		}
		rec, err := DecodeRecord(l.file)

		if errors.Is(err, io.EOF) {
			break
		}

		if errors.Is(err, ErrIncompleteRecord) {
			// remove incomplete record and all after
			if err := l.file.Truncate(pos); err != nil {
				return fmt.Errorf("failed to truncate incomplete WAL tail: %w", err)
			}
			break
		}

		if err != nil {
			return err
		}

		if rec.Index <= l.lastIndex {
			return fmt.Errorf(
				"%w: got index %d after %d",
				ErrIndexOutOfOrder,
				rec.Index,
				l.lastIndex,
			)
		}

		if err := apply(rec); err != nil {
			return err
		}

		l.lastIndex = rec.Index // last successful played index
	}

	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek WAL end: %w", err)
	}

	return nil
}

// close
func (l *Log) Close() error {
	l.muWal.Lock()
	defer l.muWal.Unlock()

	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("Error during closing Wal file: %w", err)
	}
	return l.file.Close()
}

// LastIndex returns the highest record index observed during append or replay.
func (l *Log) LastIndex() uint64 {
	l.muWal.Lock()
	defer l.muWal.Unlock()

	return l.lastIndex
}
