package network

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/Gauransh-Arora/raft-kv-store/raft"
)

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func ReplicateToPeer(node *raft.Node, peer string) bool {
	for attempt := 0; attempt < 5; attempt++ {
		if !node.IsLeader() {
			return false
		}
		prevIndex, prevTerm, entries, leaderCommit := node.GetPeerReplicationInfo(peer)
		req := raft.AppendEntriesRequest{
			Term:         node.GetCurrentTerm(),
			LeaderID:     node.ID,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}

		resp, err := SendAppendEntries(peer, req)
		if err != nil {
			return false
		}
		if resp.Term > node.GetCurrentTerm() {
			node.BecomeFollower(resp.Term)
			return false
		}
		if resp.Success {
			node.RecordPeerAppendSuccess(peer, resp.MatchIndex)
			return true
		}

		node.RecordPeerAppendFailure(peer, resp.MatchIndex)
	}
	return false
}

func SetHandler(node *raft.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SetRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !node.IsLeader() {
			http.Error(w, "not a leader", http.StatusBadRequest)
			return
		}

		entry := node.CreateSetCommand(req.Key, req.Value)
		entryIndex, err := node.AppendLog(entry)
		if err != nil {
			http.Error(w, "WAL write failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		var wg sync.WaitGroup
		for _, peer := range node.Peers {
			wg.Add(1)
			go func(p string) {
				defer wg.Done()
				ReplicateToPeer(node, p)
			}(peer)
		}
		wg.Wait()

		if !node.IsLeader() {
			http.Error(w, "stepped down: higher term observed", http.StatusInternalServerError)
			return
		}

		if node.GetCommitIndex() < entryIndex {
			http.Error(w, "failed to reach majority commitment", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "committed",
			"index":  entryIndex,
		})
	}
}

func GetHandler(node *raft.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value, found := node.Get(key)
		if !found {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"value": value,
		})
	}
}
