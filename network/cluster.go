package network

import (
	"encoding/json"
	"log"
	"net/http"
)

type PingRequest struct{
	Message string `json:"message"`
}

func PingHandler() http.HandlerFunc{
	return func(w http.ResponseWriter, r* http.Request){
		var req PingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil{
			http.Error(w,err.Error(),http.StatusBadRequest)
			return
		}
		log.Printf("Received: %s\n", req.Message)
		w.WriteHeader(http.StatusOK)
	}
}