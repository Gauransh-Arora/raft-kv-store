package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/Gauransh-Arora/raft-kv-store/network"
	"github.com/Gauransh-Arora/raft-kv-store/raft"
)

func main() {
	id := flag.Int("id", 1, "node id")
	port := flag.String("port", "8001", "port")
	flag.Parse()

	var peers []string
	switch *id {
	case 1:
		peers = []string{
			"localhost:8002",
			"localhost:8003",
		}
	case 2:
		peers = []string{
			"localhost:8001",
			"localhost:8003",
		}
	case 3:
		peers = []string{
			"localhost:8001",
			"localhost:8002",
		}
	}

	node, err := raft.NewNode(*id, peers, "./data")
	if err != nil {
		log.Fatalf("Failed to initialise node: %v", err)
	}

	node.StartElectionTimer()

	// Leader heartbeat loop: sends heartbeats with LeaderCommit to all peers
	go func() {
		for {
			if node.IsLeader() {
				for _, peer := range node.Peers {
					go func(p string) {
						// Attempt log replication if peer is behind, otherwise heartbeat
						if !network.ReplicateToPeer(node, p) {
							_ = network.SendHeartbeat(
								p,
								raft.HeartbeatRequest{
									Term:         node.GetCurrentTerm(),
									LeaderID:     node.ID,
									LeaderCommit: node.GetCommitIndex(),
								},
							)
						}
					}(peer)
				}
			}
			time.Sleep(2 * time.Second)
		}
	}()

	// Candidate election loop: solicits votes with log freshness (LastLogIndex & LastLogTerm)
	go func() {
		for {
			time.Sleep(1 * time.Second)
			if !node.IsCandidate() {
				continue
			}

			lastLogIndex, lastLogTerm := node.GetLastLogIndexAndTerm()
			currentTerm := node.GetCurrentTerm()
			votes := 1

			for _, peer := range node.Peers {
				resp, err := network.SendVoteRequest(
					peer,
					raft.RequestVoteRequest{
						Term:         currentTerm,
						CandidateID:  node.ID,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					},
				)
				if err != nil {
					continue
				}
				if resp.Term > node.GetCurrentTerm() {
					node.BecomeFollower(resp.Term)
					break
				}
				if resp.VoteGranted {
					votes++
				}
			}

			log.Printf("Node %d received %d votes (term %d)", node.ID, votes, currentTerm)
			majority := (len(node.Peers)+1)/2 + 1
			if node.IsCandidate() && node.GetCurrentTerm() == currentTerm && votes >= majority {
				node.BecomeLeader()
			}
		}
	}()

	http.HandleFunc("/set", network.SetHandler(node))
	http.HandleFunc("/get", network.GetHandler(node))
	http.HandleFunc("/ping", network.PingHandler())
	http.HandleFunc("/raft/heartbeat", network.HeartbeatHandler(node))
	http.HandleFunc("/raft/requestVote", network.RequestVoteHandler(node))
	http.HandleFunc("/raft/appendEntries", network.AppendEntriesHandler(node))

	log.Printf("Node %d running on :%s\n", *id, *port)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
