package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// owns the wal file and api to interact

type Log struct {
	file  *os.File // represent open file descriptor
	muWal sync.Mutex
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
	return l.file.Sync() // memory to stable storage
}

// Replay + callback func, apply all commands from file to memory
func (l *Log) Replay(apply func(Record) error) error {
	l.muWal.Lock()
	defer l.muWal.Unlock()

	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to move cursor: %w", err)
	}

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
			l.file.Truncate(pos)
			break
		}

		if err != nil {
			return err
		}

		if err := apply(rec); err != nil {
			return err
		}
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
