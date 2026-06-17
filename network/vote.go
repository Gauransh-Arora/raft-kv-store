package network

import (
	"encoding/json"
	"net/http"

	"github.com/Gauransh-Arora/raft-kv-store/raft"
)

func RequestVoteHandler(node *raft.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req raft.RequestVoteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := node.HandleVoteRequest(req)
		json.NewEncoder(w).Encode(resp)
	}
}