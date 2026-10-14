package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func OpenWAL(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %q: %w", filepath.Dir(path), err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", path, err)
	}
	return &WAL{file: f}, nil
}

func (w *WAL) Append(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("wal: marshal: %w", err)
	}
	data = append(data, '\n')

	if _, err = w.file.Write(data); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	if err = w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

func (w *WAL) ReadAll() ([]LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("wal: seek: %w", err)
	}

	var entries []LogEntry
	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var entry LogEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, fmt.Errorf("wal: corrupt record %q: %w", line, err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("wal: scan: %w", err)
	}
	return entries, nil
}

func (w *WAL) Rewrite(entries []LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("wal: seek: %w", err)
	}

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("wal: marshal: %w", err)
		}
		data = append(data, '\n')
		if _, err = w.file.Write(data); err != nil {
			return fmt.Errorf("wal: write: %w", err)
		}
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}

type Metadata struct {
	CurrentTerm int `json:"current_term"`
	VotedFor    int `json:"voted_for"`
	CommitIndex int `json:"commit_index"`
}

func SaveMetadata(path string, meta Metadata) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	f, err := os.Open(tmpPath)
	if err == nil {
		_ = f.Sync()
		f.Close()
	}
	return os.Rename(tmpPath, path)
}

func ReadMetadata(path string) (Metadata, error) {
	meta := Metadata{CurrentTerm: 0, VotedFor: -1, CommitIndex: 0}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return meta, nil
		}
		return meta, err
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, err
	}
	return meta, nil
}

