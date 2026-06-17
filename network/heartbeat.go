package network

import (
	"encoding/json"
	"log"
	"net/http"
	// "time"

	"github.com/Gauransh-Arora/raft-kv-store/raft"
)

func HeartbeatHandler(node *raft.Node) http.HandlerFunc{
	return func(w http.ResponseWriter, r *http.Request){
		var req raft.HeartbeatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil{
			http.Error(w,err.Error(),http.StatusBadRequest)
			return
		}
		log.Printf("Heartbeat received from leader %d (term %d)\n",req.LeaderID, req.Term)
		node.HandleHeartbeat(req)
		w.WriteHeader(http.StatusOK)
	}
}