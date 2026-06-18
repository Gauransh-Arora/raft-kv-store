package network

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/Gauransh-Arora/raft-kv-store/raft"
)

func SendHeartbeat(address string, req raft.HeartbeatRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = http.Post(
		"http://"+address+"/raft/heartbeat",
		"application/json",
		bytes.NewBuffer(body),
	)
	return err
}

func SendVoteRequest(address string, req raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(
		"http://"+address+"/raft/requestVote",
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var voteResp raft.RequestVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return nil, err
	}

	return &voteResp, nil
}

func SendAppendEntries(address string, req raft.AppendEntriesRequest)(*raft.AppendEntriesResponse, error){
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(
		"http://"+address+"/raft/appendEntries",
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var entriesResp raft.AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&entriesResp); err != nil {
		return nil, err
	}

	return &entriesResp, nil
}