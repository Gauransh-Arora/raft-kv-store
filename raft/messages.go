package raft

type HeartbeatRequest struct {
	Term     int `json:"term"`
	LeaderID int `json:"leader_id"`
}

type AppendEntriesRequest struct {
	Term     int        `json:"term"`
	LeaderID int        `json:"leader_id"`
	Entries  []LogEntry `json:"entries"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

type RequestVoteRequest struct {
	Term        int `json:"term"`
	CandidateID int `json:"candidate_id"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}
