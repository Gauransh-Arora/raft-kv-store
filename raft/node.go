package raft

import (
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Gauransh-Arora/raft-kv-store/storage"
)

type Node struct {
	mu              sync.RWMutex
	ID              int
	State           NodeState
	CurrentTerm     int
	VotedFor        int
	KV              *storage.KVStore
	Peers           []string
	LastHeartbeat   time.Time
	ElectionTimeout time.Duration
	Log             []LogEntry
	CommitIndex     int
	LastApplied     int
	WAL             *WAL
}

func NewNode(id int, peers []string, dataDir string) (*Node, error) {
	timeout := time.Duration(8+rand.Intn(5)) * time.Second
	walPath := filepath.Join(dataDir, fmt.Sprintf("node-%d", id), "wal.log")

	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("node %d: open WAL: %w", id, err)
	}

	n := &Node{
		ID:              id,
		State:           Follower,
		CurrentTerm:     0,
		VotedFor:        -1,
		KV:              storage.NewKVStore(),
		Peers:           peers,
		LastHeartbeat:   time.Now(),
		ElectionTimeout: timeout,
		Log:             make([]LogEntry, 0),
		CommitIndex:     0,
		LastApplied:     0,
		WAL:             wal,
	}

	if err := n.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("node %d: WAL recovery: %w", id, err)
	}
	return n, nil
}

func (n *Node) recoverFromWAL() error {
	entries, err := n.WAL.ReadAll()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		n.Log = append(n.Log, entry)
		parts := strings.Split(entry.Command, " ")
		if len(parts) == 3 && parts[0] == "SET" {
			n.KV.Set(parts[1], parts[2])
		}
	}
	n.CommitIndex = len(n.Log)
	n.LastApplied = len(n.Log)
	if len(entries) > 0 {
		log.Printf("Node %d recovered %d entries from WAL (commitIndex=%d)",
			n.ID, len(entries), n.CommitIndex)
	}
	return nil
}

func (n *Node) Set(key, value string) {
	n.KV.Set(key, value)
}

func (n *Node) Get(key string) (string, bool) {
	return n.KV.Get(key)
}

func (n *Node) Delete(key string) {
	n.KV.Delete(key)
}

func (n *Node) AppendLog(entry LogEntry) error {
	if err := n.WAL.Append(entry); err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Log = append(n.Log, entry)
	return nil
}

func (n *Node) PrintLog() {
	n.mu.RLock()
	defer n.mu.RUnlock()
	log.Printf("Node %d Log: %+v", n.ID, n.Log)
}

func (n *Node) ReplicateLogEntry(entry LogEntry) error {
	if err := n.AppendLog(entry); err != nil {
		return err
	}
	log.Printf("Leader %d appended entry: %+v", n.ID, entry)
	return nil
}

func (n *Node) CreateSetCommand(key, value string) LogEntry {
	return LogEntry{
		Term:    n.CurrentTerm,
		Command: "SET " + key + " " + value,
	}
}

func (n *Node) GetState() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.State
}

func (n *Node) GetLogLen() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.Log)
}

func (n *Node) HandleVoteRequest(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	voteGranted := false
	if req.Term >= n.CurrentTerm && (n.VotedFor == -1 || n.VotedFor == req.CandidateID) {
		n.VotedFor = req.CandidateID
		n.CurrentTerm = req.Term
		voteGranted = true
		n.LastHeartbeat = time.Now()
	}
	return RequestVoteResponse{
		Term:        n.CurrentTerm,
		VoteGranted: voteGranted,
	}
}

func (n *Node) BecomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.State = Leader
	log.Printf("Node %d became LEADER (term %d)", n.ID, n.CurrentTerm)
}

func (n *Node) BecomeFollower(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Printf("Node %d stepping down to FOLLOWER (term %d → %d)", n.ID, n.CurrentTerm, term)
	n.State = Follower
	n.CurrentTerm = term
	n.VotedFor = -1
}

func (n *Node) IsCandidate() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.State == Candidate
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.State == Leader
}

func (n *Node) GetCurrentTerm() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.CurrentTerm
}

func (n *Node) HandleHeartbeat(req HeartbeatRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LastHeartbeat = time.Now()
	if req.Term >= n.CurrentTerm {
		n.CurrentTerm = req.Term
		n.State = Follower
		n.VotedFor = -1
	}
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.LastHeartbeat = time.Now()

	if req.Term >= n.CurrentTerm {
		n.CurrentTerm = req.Term
		n.State = Follower
		n.VotedFor = -1
	}

	if len(req.Entries) > 0 {
		for _, entry := range req.Entries {
			if err := n.WAL.Append(entry); err != nil {
				log.Printf("Node %d WAL append failed: %v", n.ID, err)
				return AppendEntriesResponse{Term: n.CurrentTerm, Success: false}
			}
			n.Log = append(n.Log, entry)
			parts := strings.Split(entry.Command, " ")
			if len(parts) == 3 && parts[0] == "SET" {
				n.KV.Set(parts[1], parts[2])
				log.Printf("Node %d applied: %s=%s", n.ID, parts[1], parts[2])
			}
		}
		n.LastApplied = len(n.Log)
		log.Printf("Node %d appended %d entries (lastApplied=%d)",
			n.ID, len(req.Entries), n.LastApplied)
	}

	return AppendEntriesResponse{
		Term:    n.CurrentTerm,
		Success: true,
	}
}

func (n *Node) AdvanceCommitIndex(index int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index > n.CommitIndex {
		n.CommitIndex = index
		n.LastApplied = index
		log.Printf("Node %d advanced commitIndex → %d", n.ID, n.CommitIndex)
	}
}

func (n *Node) ApplyLogEntry(entry LogEntry) {
	parts := strings.Split(entry.Command, " ")
	if len(parts) != 3 {
		return
	}
	if parts[0] != "SET" {
		return
	}
	key, value := parts[1], parts[2]
	n.KV.Set(key, value)
	log.Printf("Node %d Applied: %s=%s", n.ID, key, value)
}