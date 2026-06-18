package network

import (
	"encoding/json"
	"net/http"

	"github.com/Gauransh-Arora/raft-kv-store/raft"
)

func AppendEntriesHandler(node *raft.Node) http.HandlerFunc{
	return func(w http.ResponseWriter, r *http.Request){
		var req raft.AppendEntriesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil{
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := node.HandleAppendEntries(req)
		json.NewEncoder(w).Encode(resp)
	}
}