package raft

import (
	"os"
	"testing"
)

func createTestNode(t *testing.T, id int) (*Node, string) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	node, err := NewNode(id, []string{"peer2", "peer3"}, tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create node: %v", err)
	}
	return node, tmpDir
}

// 1. Correct Leader Election & Log Freshness Tests
func TestVoteRequestLogFreshness(t *testing.T) {
	node, tmpDir := createTestNode(t, 1)
	defer os.RemoveAll(tmpDir)

	// Local node is at term 2 with two log entries: (term 1), (term 2)
	node.CurrentTerm = 2
	node.Log = []LogEntry{
		{Term: 1, Command: "SET k1 v1"},
		{Term: 2, Command: "SET k2 v2"},
	}

	// Case 1: Candidate has lower term -> Rejected
	resp := node.HandleVoteRequest(RequestVoteRequest{
		Term:         1,
		CandidateID:  2,
		LastLogIndex: 2,
		LastLogTerm:  2,
	})
	if resp.VoteGranted {
		t.Fatalf("expected vote rejected due to lower term")
	}

	// Case 2: Candidate has higher term (3) but STALE log (lastLogTerm 1 < local 2) -> Rejected
	resp = node.HandleVoteRequest(RequestVoteRequest{
		Term:         3,
		CandidateID:  2,
		LastLogIndex: 5,
		LastLogTerm:  1,
	})
	if resp.VoteGranted {
		t.Fatalf("expected vote rejected due to stale candidate log term")
	}

	// Case 3: Candidate has same last term (2) but shorter log (1 < 2) -> Rejected
	resp = node.HandleVoteRequest(RequestVoteRequest{
		Term:         3,
		CandidateID:  2,
		LastLogIndex: 1,
		LastLogTerm:  2,
	})
	if resp.VoteGranted {
		t.Fatalf("expected vote rejected due to shorter candidate log length")
	}

	// Case 4: Candidate has up-to-date log (term 3, lastLogTerm 2, lastLogIndex 2) -> Granted
	resp = node.HandleVoteRequest(RequestVoteRequest{
		Term:         3,
		CandidateID:  2,
		LastLogIndex: 2,
		LastLogTerm:  2,
	})
	if !resp.VoteGranted {
		t.Fatalf("expected vote granted for up-to-date candidate")
	}
	if node.VotedFor != 2 {
		t.Fatalf("expected votedFor to be 2, got %d", node.VotedFor)
	}

	// Case 5: Second candidate in same term -> Rejected (single vote per term)
	resp = node.HandleVoteRequest(RequestVoteRequest{
		Term:         3,
		CandidateID:  3,
		LastLogIndex: 3,
		LastLogTerm:  3,
	})
	if resp.VoteGranted {
		t.Fatalf("expected second vote in same term to be rejected")
	}
}

// 2. Log Consistency & Conflict Resolution Tests
func TestAppendEntriesConsistencyAndConflictResolution(t *testing.T) {
	node, tmpDir := createTestNode(t, 1)
	defer os.RemoveAll(tmpDir)

	// Node has 3 entries: index 1 (term 1), index 2 (term 1), index 3 (term 1)
	node.CurrentTerm = 2
	node.Log = []LogEntry{
		{Term: 1, Command: "SET a 1"},
		{Term: 1, Command: "SET b 2"},
		{Term: 1, Command: "SET c 3"},
	}

	// Test A: PrevLogIndex beyond log length -> Rejected
	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         2,
		LeaderID:     2,
		PrevLogIndex: 5,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 2, Command: "SET d 4"}},
	})
	if resp.Success {
		t.Fatalf("expected rejection when PrevLogIndex > len(Log)")
	}

	// Test B: PrevLogTerm mismatch -> Rejected
	resp = node.HandleAppendEntries(AppendEntriesRequest{
		Term:         2,
		LeaderID:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  99, // Mismatched term
		Entries:      []LogEntry{{Term: 2, Command: "SET d 4"}},
	})
	if resp.Success {
		t.Fatalf("expected rejection when PrevLogTerm does not match")
	}

	// Test C: PrevLog matches at index 2, but new entries conflict with index 3 (term 2 vs local term 1)
	// Follower must truncate index 3 and append new entries
	resp = node.HandleAppendEntries(AppendEntriesRequest{
		Term:         2,
		LeaderID:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 2, Command: "SET c_new 33"},
			{Term: 2, Command: "SET d 44"},
		},
		LeaderCommit: 2,
	})
	if !resp.Success {
		t.Fatalf("expected AppendEntries to succeed with matching PrevLog")
	}

	if len(node.Log) != 4 {
		t.Fatalf("expected log length 4 after conflict resolution, got %d", len(node.Log))
	}
	if node.Log[2].Command != "SET c_new 33" || node.Log[2].Term != 2 {
		t.Fatalf("expected index 3 to be replaced by conflicting entry, got %+v", node.Log[2])
	}
	if node.Log[3].Command != "SET d 44" {
		t.Fatalf("expected index 4 to be appended, got %+v", node.Log[3])
	}
}

// 3. Commit & Application Separation Tests
func TestCommitAndApplySeparation(t *testing.T) {
	node, tmpDir := createTestNode(t, 1)
	defer os.RemoveAll(tmpDir)

	node.CurrentTerm = 1

	// Follower receives an uncommitted entry (LeaderCommit = 0)
	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 1, Command: "SET user alice"},
		},
		LeaderCommit: 0,
	})
	if !resp.Success {
		t.Fatalf("expected append success")
	}

	// Verify entry is in log, but NOT yet applied to KVStore!
	if _, found := node.Get("user"); found {
		t.Fatalf("uncommitted entry was applied to KV store before commitIndex advanced!")
	}
	if node.CommitIndex != 0 || node.LastApplied != 0 {
		t.Fatalf("commitIndex or lastApplied advanced prematurely: commit=%d, applied=%d",
			node.CommitIndex, node.LastApplied)
	}

	// Now leader sends heartbeat or next RPC advancing LeaderCommit to 1
	node.HandleHeartbeat(HeartbeatRequest{
		Term:         1,
		LeaderID:     2,
		LeaderCommit: 1,
	})

	// Verify entry is now committed and applied to KVStore!
	val, found := node.Get("user")
	if !found || val != "alice" {
		t.Fatalf("expected user=alice in KV store after commit, got val=%q, found=%v", val, found)
	}
	if node.CommitIndex != 1 || node.LastApplied != 1 {
		t.Fatalf("expected commitIndex=1 and lastApplied=1, got commit=%d, applied=%d",
			node.CommitIndex, node.LastApplied)
	}
}

// 4. Correct Crash Recovery: Committed vs Uncommitted Entries & Metadata
func TestCrashRecoveryDistinguishesCommitted(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-recovery-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Step 1: Initialize node, append 3 entries, but ONLY commit 2 of them
	node1, err := NewNode(1, []string{"peer2", "peer3"}, tmpDir)
	if err != nil {
		t.Fatalf("failed to create node1: %v", err)
	}

	node1.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 1, Command: "SET k1 v1"},
			{Term: 1, Command: "SET k2 v2"},
			{Term: 1, Command: "SET k3 uncommitted"},
		},
		LeaderCommit: 2, // only entries 1 and 2 are committed!
	})

	if val, _ := node1.Get("k1"); val != "v1" {
		t.Fatalf("expected k1=v1")
	}
	if val, _ := node1.Get("k2"); val != "v2" {
		t.Fatalf("expected k2=v2")
	}
	if _, found := node1.Get("k3"); found {
		t.Fatalf("uncommitted k3 must not be applied")
	}

	// Simulate crash: close node1
	node1.WAL.Close()

	// Step 2: Restart node (simulate recovery from disk)
	nodeRecovered, err := NewNode(1, []string{"peer2", "peer3"}, tmpDir)
	if err != nil {
		t.Fatalf("failed to recover node: %v", err)
	}
	defer nodeRecovered.WAL.Close()

	// Verify all 3 entries exist in log
	if len(nodeRecovered.Log) != 3 {
		t.Fatalf("expected 3 entries in recovered log, got %d", len(nodeRecovered.Log))
	}

	// Verify CommitIndex and LastApplied were restored to 2, NOT 3!
	if nodeRecovered.CommitIndex != 2 {
		t.Fatalf("expected recovered commitIndex=2, got %d", nodeRecovered.CommitIndex)
	}
	if nodeRecovered.LastApplied != 2 {
		t.Fatalf("expected recovered lastApplied=2, got %d", nodeRecovered.LastApplied)
	}

	// Verify state machine has k1 and k2, but NEVER k3!
	if val, found := nodeRecovered.Get("k1"); !found || val != "v1" {
		t.Fatalf("expected k1=v1 after recovery")
	}
	if val, found := nodeRecovered.Get("k2"); !found || val != "v2" {
		t.Fatalf("expected k2=v2 after recovery")
	}
	if _, found := nodeRecovered.Get("k3"); found {
		t.Fatalf("uncommitted entry k3 was erroneously applied during recovery!")
	}
}

// 5. Leader Commit Rule Test: Leader only commits current term entries by majority
func TestLeaderCommitCurrentTermRule(t *testing.T) {
	node, tmpDir := createTestNode(t, 1)
	defer os.RemoveAll(tmpDir)

	node.BecomeLeader()
	node.CurrentTerm = 3
	node.Peers = []string{"peer2", "peer3"} // 3 nodes total: leader, peer2, peer3

	// Add entry from previous term 2
	node.Log = append(node.Log, LogEntry{Term: 2, Command: "SET old term2"})
	node.MatchIndex["peer2"] = 1
	node.MatchIndex["peer3"] = 1

	// Raft §5.4.2: Leader CANNOT commit entry 1 from term 2 directly by counting replicas!
	node.mu.Lock()
	advanced := node.CheckAndAdvanceLeaderCommitLocked()
	node.mu.Unlock()

	if advanced || node.CommitIndex != 0 {
		t.Fatalf("leader must not directly commit entries from previous terms")
	}

	// Now add an entry from CURRENT term 3
	node.Log = append(node.Log, LogEntry{Term: 3, Command: "SET current term3"})
	node.MatchIndex["peer2"] = 2 // peer2 replicated up to entry 2 (majority: leader + peer2)

	node.mu.Lock()
	advanced = node.CheckAndAdvanceLeaderCommitLocked()
	node.mu.Unlock()

	if !advanced || node.CommitIndex != 2 {
		t.Fatalf("expected commitIndex to advance to 2 once current term entry reached majority")
	}
	// Both old entry 1 and current entry 2 are now applied!
	if val, found := node.Get("old"); !found || val != "term2" {
		t.Fatalf("expected old entry to be applied indirectly")
	}
	if val, found := node.Get("current"); !found || val != "term3" {
		t.Fatalf("expected current entry to be applied")
	}
}
