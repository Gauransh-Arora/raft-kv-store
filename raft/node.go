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
	metaPath        string

	NextIndex  map[string]int
	MatchIndex map[string]int
}

func NewNode(id int, peers []string, dataDir string) (*Node, error) {
	timeout := time.Duration(8+rand.Intn(5)) * time.Second
	nodeDir := filepath.Join(dataDir, fmt.Sprintf("node-%d", id))
	walPath := filepath.Join(nodeDir, "wal.log")
	metaPath := filepath.Join(nodeDir, "meta.json")

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
		metaPath:        metaPath,
		NextIndex:       make(map[string]int),
		MatchIndex:      make(map[string]int),
	}

	if err := n.recover(); err != nil {
		return nil, fmt.Errorf("node %d: recovery: %w", id, err)
	}
	return n, nil
}

func (n *Node) persistStateLocked() {
	meta := Metadata{
		CurrentTerm: n.CurrentTerm,
		VotedFor:    n.VotedFor,
		CommitIndex: n.CommitIndex,
	}
	if err := SaveMetadata(n.metaPath, meta); err != nil {
		log.Printf("Node %d: save metadata failed: %v", n.ID, err)
	}
}

func (n *Node) recover() error {
	meta, err := ReadMetadata(n.metaPath)
	if err != nil {
		return fmt.Errorf("read meta: %w", err)
	}
	n.CurrentTerm = meta.CurrentTerm
	n.VotedFor = meta.VotedFor
	n.CommitIndex = meta.CommitIndex

	entries, err := n.WAL.ReadAll()
	if err != nil {
		return fmt.Errorf("wal read: %w", err)
	}
	n.Log = entries

	if n.CommitIndex > len(n.Log) {
		n.CommitIndex = len(n.Log)
	}

	// Apply ONLY committed entries up to CommitIndex
	for i := 0; i < n.CommitIndex; i++ {
		parts := strings.SplitN(n.Log[i].Command, " ", 3)
		if len(parts) == 3 && parts[0] == "SET" {
			n.KV.Set(parts[1], parts[2])
		}
	}
	n.LastApplied = n.CommitIndex

	log.Printf("Node %d recovered: term=%d, votedFor=%d, logLen=%d, commitIndex=%d, lastApplied=%d",
		n.ID, n.CurrentTerm, n.VotedFor, len(n.Log), n.CommitIndex, n.LastApplied)
	return nil
}

func (n *Node) applyEntriesLocked() {
	for n.CommitIndex > n.LastApplied {
		idx := n.LastApplied
		if idx >= len(n.Log) {
			break
		}
		entry := n.Log[idx]
		parts := strings.SplitN(entry.Command, " ", 3)
		if len(parts) == 3 && parts[0] == "SET" {
			n.KV.Set(parts[1], parts[2])
			log.Printf("Node %d applied entry %d: %s=%s", n.ID, idx+1, parts[1], parts[2])
		}
		n.LastApplied++
	}
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

func (n *Node) AppendLog(entry LogEntry) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Log = append(n.Log, entry)
	if err := n.WAL.Append(entry); err != nil {
		n.Log = n.Log[:len(n.Log)-1]
		return 0, fmt.Errorf("WAL append: %w", err)
	}
	return len(n.Log), nil
}

func (n *Node) PrintLog() {
	n.mu.RLock()
	defer n.mu.RUnlock()
	log.Printf("Node %d Log: %+v", n.ID, n.Log)
}

func (n *Node) CreateSetCommand(key, value string) LogEntry {
	n.mu.RLock()
	defer n.mu.RUnlock()
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

func (n *Node) GetCurrentTerm() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.CurrentTerm
}

func (n *Node) GetCommitIndex() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.CommitIndex
}

func (n *Node) GetLastLogIndexAndTerm() (int, int) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	lastIndex := len(n.Log)
	lastTerm := 0
	if lastIndex > 0 {
		lastTerm = n.Log[lastIndex-1].Term
	}
	return lastIndex, lastTerm
}

func (n *Node) HandleVoteRequest(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1. Reply false if req.Term < CurrentTerm (§5.1)
	if req.Term < n.CurrentTerm {
		return RequestVoteResponse{
			Term:        n.CurrentTerm,
			VoteGranted: false,
		}
	}

	// 2. If term is greater, convert to follower and reset votedFor (§5.1)
	if req.Term > n.CurrentTerm {
		n.CurrentTerm = req.Term
		n.State = Follower
		n.VotedFor = -1
		n.persistStateLocked()
	}

	// 3. Check vote eligibility (§5.2)
	canVote := (n.VotedFor == -1 || n.VotedFor == req.CandidateID)

	// 4. Log-freshness check (§5.4.1):
	// Candidate's log is up-to-date if it has a higher last term,
	// or the same last term and index >= voter's last index.
	lastLogIndex := len(n.Log)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = n.Log[lastLogIndex-1].Term
	}

	isUpToDate := false
	if req.LastLogTerm > lastLogTerm {
		isUpToDate = true
	} else if req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex {
		isUpToDate = true
	}

	if canVote && isUpToDate {
		n.VotedFor = req.CandidateID
		n.LastHeartbeat = time.Now()
		n.persistStateLocked()
		log.Printf("Node %d voted for Candidate %d (term %d)", n.ID, req.CandidateID, n.CurrentTerm)
		return RequestVoteResponse{
			Term:        n.CurrentTerm,
			VoteGranted: true,
		}
	}

	return RequestVoteResponse{
		Term:        n.CurrentTerm,
		VoteGranted: false,
	}
}

func (n *Node) BecomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.State = Leader
	for _, peer := range n.Peers {
		n.NextIndex[peer] = len(n.Log) + 1
		n.MatchIndex[peer] = 0
	}
	log.Printf("Node %d became LEADER (term %d, logLen %d)", n.ID, n.CurrentTerm, len(n.Log))
}

func (n *Node) BecomeFollower(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Printf("Node %d stepping down to FOLLOWER (term %d → %d)", n.ID, n.CurrentTerm, term)
	n.State = Follower
	n.CurrentTerm = term
	n.VotedFor = -1
	n.persistStateLocked()
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

func (n *Node) HandleHeartbeat(req HeartbeatRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.CurrentTerm {
		return
	}

	if req.Term > n.CurrentTerm {
		n.CurrentTerm = req.Term
		n.State = Follower
		n.VotedFor = -1
		n.persistStateLocked()
	} else if n.State != Follower {
		n.State = Follower
	}

	n.LastHeartbeat = time.Now()

	// Update commit index from leader's commit index
	if req.LeaderCommit > n.CommitIndex {
		if req.LeaderCommit < len(n.Log) {
			n.CommitIndex = req.LeaderCommit
		} else {
			n.CommitIndex = len(n.Log)
		}
		n.persistStateLocked()
		n.applyEntriesLocked()
	}
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1. Reply false if req.Term < CurrentTerm (§5.1)
	if req.Term < n.CurrentTerm {
		return AppendEntriesResponse{
			Term:       n.CurrentTerm,
			Success:    false,
			MatchIndex: len(n.Log),
		}
	}

	// 2. If term > CurrentTerm, update term and step down
	if req.Term > n.CurrentTerm {
		n.CurrentTerm = req.Term
		n.State = Follower
		n.VotedFor = -1
		n.persistStateLocked()
	} else if n.State != Follower {
		n.State = Follower
	}

	n.LastHeartbeat = time.Now()

	// 3. Reply false if log doesn't contain an entry at PrevLogIndex matching PrevLogTerm (§5.3)
	if req.PrevLogIndex > len(n.Log) {
		return AppendEntriesResponse{
			Term:       n.CurrentTerm,
			Success:    false,
			MatchIndex: len(n.Log),
		}
	}

	if req.PrevLogIndex > 0 && n.Log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
		return AppendEntriesResponse{
			Term:       n.CurrentTerm,
			Success:    false,
			MatchIndex: req.PrevLogIndex - 1,
		}
	}

	// 4. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	logChanged := false
	insertIdx := req.PrevLogIndex
	for i, entry := range req.Entries {
		idx := insertIdx + 1 + i
		if idx <= len(n.Log) {
			if n.Log[idx-1].Term != entry.Term {
				log.Printf("Node %d: conflict at index %d (local term %d != entry term %d), truncating",
					n.ID, idx, n.Log[idx-1].Term, entry.Term)
				n.Log = n.Log[:idx-1]
				n.Log = append(n.Log, entry)
				logChanged = true
			}
		} else {
			n.Log = append(n.Log, entry)
			logChanged = true
		}
	}

	if logChanged {
		if err := n.WAL.Rewrite(n.Log); err != nil {
			log.Printf("Node %d: WAL rewrite failed: %v", n.ID, err)
			return AppendEntriesResponse{
				Term:       n.CurrentTerm,
				Success:    false,
				MatchIndex: len(n.Log),
			}
		}
	}

	// 5. If LeaderCommit > CommitIndex, set CommitIndex = min(LeaderCommit, len(Log))
	if req.LeaderCommit > n.CommitIndex {
		if req.LeaderCommit < len(n.Log) {
			n.CommitIndex = req.LeaderCommit
		} else {
			n.CommitIndex = len(n.Log)
		}
		n.persistStateLocked()
		n.applyEntriesLocked()
	}

	return AppendEntriesResponse{
		Term:       n.CurrentTerm,
		Success:    true,
		MatchIndex: len(n.Log),
	}
}

func (n *Node) CheckAndAdvanceLeaderCommitLocked() bool {
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
	// and log[N-1].Term == currentTerm: set commitIndex = N (§5.3, §5.4.2)
	advanced := false
	for N := len(n.Log); N > n.CommitIndex; N-- {
		if n.Log[N-1].Term != n.CurrentTerm {
			continue
		}
		matches := 1 // leader itself
		for _, peer := range n.Peers {
			if n.MatchIndex[peer] >= N {
				matches++
			}
		}
		if matches > (len(n.Peers)+1)/2 {
			n.CommitIndex = N
			n.persistStateLocked()
			n.applyEntriesLocked()
			log.Printf("Leader %d committed up to index %d (term %d, matches=%d)", n.ID, N, n.CurrentTerm, matches)
			advanced = true
			break
		}
	}
	return advanced
}

func (n *Node) GetPeerReplicationInfo(peer string) (prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	nextIdx, ok := n.NextIndex[peer]
	if !ok || nextIdx < 1 {
		nextIdx = 1
	}

	prevLogIndex = nextIdx - 1
	prevLogTerm = 0
	if prevLogIndex > 0 && prevLogIndex <= len(n.Log) {
		prevLogTerm = n.Log[prevLogIndex-1].Term
	}

	if prevLogIndex < len(n.Log) {
		entries = make([]LogEntry, len(n.Log[prevLogIndex:]))
		copy(entries, n.Log[prevLogIndex:])
	}

	leaderCommit = n.CommitIndex
	return
}

func (n *Node) RecordPeerAppendSuccess(peer string, matchIndex int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if matchIndex > n.MatchIndex[peer] {
		n.MatchIndex[peer] = matchIndex
	}
	n.NextIndex[peer] = n.MatchIndex[peer] + 1
	return n.CheckAndAdvanceLeaderCommitLocked()
}

func (n *Node) RecordPeerAppendFailure(peer string, hintMatchIndex int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if hintMatchIndex >= 0 && hintMatchIndex < n.NextIndex[peer] {
		n.NextIndex[peer] = hintMatchIndex + 1
	} else if n.NextIndex[peer] > 1 {
		n.NextIndex[peer]--
	}
}