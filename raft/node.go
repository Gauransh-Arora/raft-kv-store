package raft

import (
	"log"
	"math/rand"
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
}

func NewNode(id int, peers []string) *Node {
	timeout := time.Duration(8+rand.Intn(5)) * time.Second
	return &Node{
		ID:              id,
		State:           Follower,
		CurrentTerm:     0,
		VotedFor:        -1,
		KV:              storage.NewKVStore(),
		Peers:           peers,
		LastHeartbeat:   time.Now(),
		ElectionTimeout: timeout,
		Log:             make([]LogEntry, 0),
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

func (n *Node) AppendLog(entry LogEntry) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Log = append(n.Log, entry)
}

func (n *Node) PrintLog() {
	n.mu.RLock()
	defer n.mu.RUnlock()
	log.Printf("Node %d Log: %+v", n.ID, n.Log)
}

func (n *Node) HandleVoteRequest(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	voteGranted := false
	if n.VotedFor == -1 || n.VotedFor == req.CandidateID {
		n.VotedFor = req.CandidateID
		voteGranted = true
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
