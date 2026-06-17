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
	// node := raft.NewNode(
	// 	*id,
	// 	[]string{},
	// )
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

	node := raft.NewNode(*id, peers)
	// node.AppendLog(
	// 	raft.LogEntry{
	// 		Term:    1,
	// 		Command: "SET name gauransh",
	// 	},
	// )

	node.PrintLog()
	node.StartElectionTimer()

	go func() {
		for {

			if node.IsLeader() {

				for _, peer := range node.Peers {

					err := network.SendHeartbeat(
						peer,
						raft.HeartbeatRequest{
							Term:     node.GetCurrentTerm(),
							LeaderID: node.ID,
						},
					)

					if err != nil {
						log.Printf(
							"heartbeat to %s failed: %v",
							peer,
							err,
						)
					}
				}
			}

			time.Sleep(2 * time.Second)
		}
	}()

	go func() {
		for {
			time.Sleep(1 * time.Second)
			if !node.IsCandidate() {
				continue
			}
			votes := 1
			for _, peer := range node.Peers {
				resp, err := network.SendVoteRequest(
					peer,
					raft.RequestVoteRequest{
						Term:        node.GetCurrentTerm(),
						CandidateID: node.ID,
					},
				)
				if err != nil {
					continue
				}
				if resp.VoteGranted {
					votes++
				}
			}
			log.Printf("Node %d received %d votes", node.ID, votes)
			if votes >= 2 {
				node.BecomeLeader()

			}
		}
	}()

	http.HandleFunc("/set", network.SetHandler(node))
	http.HandleFunc("/get", network.GetHandler(node))
	http.HandleFunc("/ping", network.PingHandler())
	http.HandleFunc("/raft/heartbeat", network.HeartbeatHandler(node))
	http.HandleFunc("/raft/requestVote", network.RequestVoteHandler(node))

	log.Printf("Node %d running on :%s\n", *id, *port)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
