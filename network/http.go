package network

import (
	"encoding/json"
	"net/http"

	"github.com/Gauransh-Arora/raft-kv-store/raft"
)

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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

		entry := raft.LogEntry{
			Term:    node.GetCurrentTerm(),
			Command: "SET " + req.Key + " " + req.Value,
		}

		if err := node.AppendLog(entry); err != nil {
			http.Error(w, "WAL write failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		appendReq := raft.AppendEntriesRequest{
			Term:     node.GetCurrentTerm(),
			LeaderID: node.ID,
			Entries:  []raft.LogEntry{entry},
		}

		acks := 1
		for _, peer := range node.Peers {
			resp, err := SendAppendEntries(peer, appendReq)
			if err != nil {
				continue
			}
			if resp.Term > node.GetCurrentTerm() {
				node.BecomeFollower(resp.Term)
				http.Error(w, "stepped down: higher term observed", http.StatusInternalServerError)
				return
			}
			if resp.Success {
				acks++
			}
		}

		if acks < 2 {
			http.Error(w, "failed to reach majority", http.StatusInternalServerError)
			return
		}

		node.AdvanceCommitIndex(node.GetLogLen())
		node.ApplyLogEntry(entry)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "committed",
			"acks":   acks,
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
		json.NewEncoder(w).Encode(map[string]string{
			"value": value,
		})
	}
}
