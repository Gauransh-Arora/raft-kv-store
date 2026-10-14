package raft

import (
	"log"
	"math/rand"
	"time"
)

func (n *Node) StartElectionTimer() {
	go func() {
		for {
			n.mu.RLock()
			state := n.State
			elapsed := time.Since(n.LastHeartbeat)
			timeout := n.ElectionTimeout
			n.mu.RUnlock()

			if state != Leader && elapsed > timeout {
				n.StartElection()
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()
}

func (n *Node) StartElection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.State = Candidate
	n.CurrentTerm++
	n.VotedFor = n.ID
	n.ElectionTimeout = time.Duration(8+rand.Intn(5)) * time.Second
	n.LastHeartbeat = time.Now()
	n.persistStateLocked()

	log.Printf(
		"Node %d became Candidate (term %d, timeout %v)",
		n.ID,
		n.CurrentTerm,
		n.ElectionTimeout,
	)
}
