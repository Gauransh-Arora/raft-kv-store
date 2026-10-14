package raft

type HeartbeatRequest struct {
	Term         int `json:"term"`
	LeaderID     int `json:"leader_id"`
	LeaderCommit int `json:"leader_commit"`
}

type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     int        `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term       int  `json:"term"`
	Success    bool `json:"success"`
	MatchIndex int  `json:"match_index"`
}

type RequestVoteRequest struct {
	Term         int `json:"term"`
	CandidateID  int `json:"candidate_id"`
	LastLogIndex int `json:"last_log_index"`
	LastLogTerm  int `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

