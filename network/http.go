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
		node.Set(req.Key, req.Value)
		w.WriteHeader(http.StatusOK)
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
