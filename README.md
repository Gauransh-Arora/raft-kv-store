# Raft KV Store

A distributed, fault-tolerant key-value store built from scratch in Go, implementing the core [Raft Consensus Algorithm](https://raft.github.io/). Every write is replicated to a majority of nodes before being acknowledged, and every node persists its log to a Write-Ahead Log (WAL) so data survives process crashes.

---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [How It Works](#how-it-works)
  - [Leader Election Flow](#leader-election-flow)
  - [Log Replication Flow](#log-replication-flow)
- [Running a 3-Node Cluster](#running-a-3-node-cluster)
- [HTTP API](#http-api)
- [Failure Recovery Demo](#failure-recovery-demo)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLIENT                                   │
│              curl / HTTP client (port 8001–8003)                 │
└───────────────────────────┬─────────────────────────────────────┘
                            │  POST /set   GET /get
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      LEADER NODE                                 │
│                                                                  │
│  ┌─────────────┐   ┌──────────────┐   ┌────────────────────┐   │
│  │ HTTP Server │──▶│  Raft State  │──▶│  WAL (wal.log)     │   │
│  │  /set /get  │   │  Machine     │   │  append + fsync    │   │
│  └─────────────┘   └──────┬───────┘   └────────────────────┘   │
│                            │ AppendEntries RPC                   │
└────────────────────────────┼────────────────────────────────────┘
              ┌──────────────┴──────────────┐
              ▼                             ▼
┌─────────────────────┐         ┌─────────────────────┐
│   FOLLOWER NODE 2   │         │   FOLLOWER NODE 3   │
│                     │         │                     │
│  ┌───────────────┐  │         │  ┌───────────────┐  │
│  │ Raft State    │  │         │  │ Raft State    │  │
│  │ /raft/*       │  │         │  │ /raft/*       │  │
│  └───────┬───────┘  │         │  └───────┬───────┘  │
│          │ WAL      │         │          │ WAL      │
│  ┌───────▼───────┐  │         │  ┌───────▼───────┐  │
│  │  wal.log      │  │         │  │  wal.log      │  │
│  └───────────────┘  │         │  └───────────────┘  │
└─────────────────────┘         └─────────────────────┘
```

**Intra-cluster RPCs (all over HTTP):**

| Endpoint | Direction | Purpose |
|---|---|---|
| `POST /raft/heartbeat` | Leader → Followers | Suppress elections, signal aliveness |
| `POST /raft/requestVote` | Candidate → Peers | Solicit votes during election |
| `POST /raft/appendEntries` | Leader → Followers | Replicate log entries |

---

## Features

- **Leader Election** — randomised election timeouts (8–12 s) ensure only one node campaigns at a time. Votes are only granted to candidates whose term is ≥ the voter's current term.
- **Heartbeats** — leader sends heartbeats every 2 s to reset follower timers and prevent unnecessary elections.
- **Log Replication** — a write is only acknowledged after a majority (≥ 2 of 3) of nodes have durably written it.
- **Write-Ahead Log (WAL)** — each log entry is fsynced to `./data/node-<id>/wal.log` before being applied, guaranteeing durability across crashes.
- **Crash Recovery** — on startup, a node replays its WAL to restore its log and KV state before joining the cluster.
- **Term-based Step-Down** — a leader or candidate that sees a higher term in any RPC response immediately reverts to Follower.
- **Election Timer Reset on Vote** — a follower that grants a vote resets its election timer so it doesn't compete against the candidate it just supported.
- **CommitIndex / LastApplied tracking** — commitIndex advances once a majority ack is received; LastApplied tracks the highest entry applied to the state machine.

---

## Project Structure

```
raft-kv-store/
├── cmd/
│   └── node/
│       └── main.go          # Entry point: cluster wiring, heartbeat loop,
│                            #   election loop, HTTP server
├── network/
│   ├── append_entries.go    # AppendEntries RPC handler (follower-side)
│   ├── client.go            # Outgoing HTTP calls to peers
│   ├── cluster.go           # /ping handler
│   ├── heartbeat.go         # Heartbeat RPC handler (follower-side)
│   ├── http.go              # /set and /get client-facing handlers
│   └── vote.go              # RequestVote RPC handler (follower-side)
├── raft/
│   ├── election.go          # Election timer goroutine, StartElection()
│   ├── log.go               # LogEntry struct
│   ├── messages.go          # All Raft RPC request/response structs
│   ├── node.go              # Core Raft state machine (Node struct + methods)
│   ├── state.go             # NodeState enum: Follower / Candidate / Leader
│   └── wal.go               # Write-Ahead Log: Append, ReadAll, fsync
├── storage/
│   └── kv.go                # Thread-safe in-memory key-value map
└── go.mod
```

---

## How It Works

### Leader Election Flow

```
All nodes start as Followers with a random election timeout (8–12 s).

  ┌──────────┐   timeout fires    ┌───────────┐
  │ Follower │──────────────────▶ │ Candidate │
  └──────────┘                    └─────┬─────┘
                                        │ sends RequestVote to all peers
                                        ▼
                              ┌──────────────────┐
                              │  Peer evaluates  │
                              │  req.Term >=      │
                              │  currentTerm AND  │
                              │  votedFor == -1  │
                              └────────┬─────────┘
                                       │ VoteGranted = true
                                       ▼
                              ┌──────────────────┐
                              │  Candidate counts │
                              │  votes. If ≥ 2   │──▶ BecomeLeader()
                              │  (majority of 3) │
                              └──────────────────┘

  Key safety rules implemented:
  • Voter resets its election timer when granting a vote.
  • Candidate steps down immediately if it sees resp.Term > currentTerm.
  • A node only votes once per term (VotedFor is cleared on new-term heartbeat).
```

### Log Replication Flow

```
Client ──POST /set──▶ Leader

  1. Leader writes entry to its own WAL (fsync) and in-memory log.
  2. Leader sends AppendEntries RPC to all peers in parallel.

         Leader ──AppendEntries──▶ Follower 2  (WAL write + apply)
         Leader ──AppendEntries──▶ Follower 3  (WAL write + apply)

  3. Leader counts ACKs. Itself counts as 1.
     • If acks >= 2 (majority):
         - AdvanceCommitIndex(len(log))
         - Apply entry to local KV store
         - Return 200 {"status":"committed","acks":N} to client
     • If acks < 2:
         - Return 500 "failed to reach majority"

  4. If any follower responds with Term > leader's term:
         - Leader calls BecomeFollower(resp.Term) and returns 500.

  Durability guarantee: an entry that is committed has been fsynced on
  at least 2 out of 3 nodes before the client receives a 200.
```

---

## Running a 3-Node Cluster

### Prerequisites

- Go 1.21+

### Start the nodes

Open **three separate terminals** from the project root:

```bash
# Terminal 1 — Node 1 (port 8001)
go run ./cmd/node/main.go -id 1 -port 8001

# Terminal 2 — Node 2 (port 8002)
go run ./cmd/node/main.go -id 2 -port 8002

# Terminal 3 — Node 3 (port 8003)
go run ./cmd/node/main.go -id 3 -port 8003
```

> WAL files are written to `./data/node-<id>/wal.log` in the directory you run from.

### Watch the election

Within 8–12 seconds you'll see one node log:

```
Node 2 became LEADER (term 1)
```

The others will log received heartbeats every 2 seconds.

### Write a key (send to the leader)

```bash
curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{"key":"name","value":"raft"}' \
  http://localhost:8002/set
```

Expected response:
```json
{"acks":3,"status":"committed"}
```

### Read a key (any node)

```bash
curl -s http://localhost:8002/get?key=name
# {"value":"raft"}
```

### Check if a node is alive

```bash
curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{"message":"hello"}' \
  http://localhost:8001/ping
```

---

## Failure Recovery Demo

This demo shows that committed data survives a node crash and restart.

### Step 1 — Write some data

```bash
# Find the leader port from logs, e.g. 8002
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"key":"city","value":"delhi"}' \
  http://localhost:8002/set

curl -s -X POST -H "Content-Type: application/json" \
  -d '{"key":"lang","value":"go"}' \
  http://localhost:8002/set
```

### Step 2 — Kill a follower

Press `Ctrl+C` in Terminal 1 (Node 1 on port 8001).

The cluster remains operational because the leader still has a majority (2 of 3).

### Step 3 — Write more data while Node 1 is down

```bash
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"key":"status","value":"degraded-but-alive"}' \
  http://localhost:8002/set
```

### Step 4 — Restart Node 1

```bash
go run ./cmd/node/main.go -id 1 -port 8001
```

Node 1 will log something like:

```
Node 1 recovered 3 entries from WAL (commitIndex=3)
```

It replays its WAL and re-applies all entries it had persisted before crashing. Entries written while it was down are received on the next AppendEntries from the leader.

### Step 5 — Verify data on the recovered node

```bash
curl -s http://localhost:8001/get?key=city
# {"value":"delhi"}

curl -s http://localhost:8001/get?key=lang
# {"value":"go"}
```

> **Note:** `status` was written while Node 1 was down. It will only be present on Node 1 after the leader sends it another AppendEntries (triggered by the next `/set` call or heartbeat cycle).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.21+ |
| Consensus | Raft (custom implementation) |
| Transport | HTTP/JSON |
| Persistence | Append-only WAL with `fsync` |
| Storage | Thread-safe in-memory map |
| Concurrency | Goroutines + `sync.RWMutex` |
