package dbraft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	entriesFile   = "entries.json"
	hardStateFile = "hard_state.json"
)

type LogEntry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type HardState struct {
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
}

type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry
	dir     string
}

func NewRaftLog(dir string) (*RaftLog, error) {
	l := &RaftLog{
		entries: make([]LogEntry, 0),
		dir:     dir,
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create raft log directory: %w", err)
	}
	if err := l.restore(); err != nil {
		return nil, fmt.Errorf("restore raft log: %w", err)
	}
	return l, nil
}

func (l *RaftLog) Append(entries ...LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
	return l.persistLocked()
}

func (l *RaftLog) GetEntry(index uint64) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index == 0 || index > uint64(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[index-1], true
}

func (l *RaftLog) GetRange(from, to uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if from == 0 || from > uint64(len(l.entries)) || from > to {
		return nil
	}
	if to > uint64(len(l.entries)) {
		to = uint64(len(l.entries))
	}
	result := make([]LogEntry, to-from+1)
	copy(result, l.entries[from-1:to])
	return result
}

func (l *RaftLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return uint64(len(l.entries))
}

func (l *RaftLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// indexより1つ前のエントリまでを残して切り詰め
func (l *RaftLog) Truncate(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index == 0 || index > uint64(len(l.entries)) {
		return nil
	}
	l.entries = l.entries[:index-1]
	return l.persistLocked()
}

func (l *RaftLog) Persist() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.persistLocked()
}

func (l *RaftLog) persistLocked() error {
	data, err := json.Marshal(l.entries)
	if err != nil {
		return fmt.Errorf("marshal log entries: %w", err)
	}
	return os.WriteFile(filepath.Join(l.dir, entriesFile), data, 0644)
}

func (l *RaftLog) restore() error {
	path := filepath.Join(l.dir, entriesFile)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read log entries: %w", err)
	}
	return json.Unmarshal(data, &l.entries)
}

func (l *RaftLog) PersistHardState(state HardState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal hard state: %w", err)
	}
	return os.WriteFile(filepath.Join(l.dir, hardStateFile), data, 0644)
}

func (l *RaftLog) RestoreHardState() (HardState, error) {
	path := filepath.Join(l.dir, hardStateFile)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return HardState{}, nil
	}
	if err != nil {
		return HardState{}, fmt.Errorf("read hard state: %w", err)
	}
	var state HardState
	if err := json.Unmarshal(data, &state); err != nil {
		return HardState{}, fmt.Errorf("unmarshal hard state: %w", err)
	}
	return state, nil
}
