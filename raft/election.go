package raft

import (
	"log"
	"time"
)

func (n *Node) StartElectionTimer() {
	go func() {
		for {
			n.mu.RLock()
			state := n.State
			n.mu.RUnlock()
			if state != Follower {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			elapsed := time.Since(n.LastHeartbeat)
			if elapsed > n.ElectionTimeout {
				n.StartElection()
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
}

func (n *Node) StartElection() {

	n.mu.Lock()

	n.State = Candidate
	n.CurrentTerm++
	n.VotedFor = n.ID
	n.LastHeartbeat = time.Now()

	log.Printf(
		"Node %d became Candidate (term %d)",
		n.ID,
		n.CurrentTerm,
	)

	n.mu.Unlock()
}
