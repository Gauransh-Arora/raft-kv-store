# Raft Key-Value Store (Work in Progress)

A distributed, in-memory key-value store built in Go, implementing the foundational concepts of the [Raft Consensus Algorithm](https://raft.github.io/).

> **Note:** This project is currently under active development.

## 📌 Overview

This project aims to build a fault-tolerant distributed key-value database from scratch. It explores distributed systems concepts by implementing the Raft protocol for consensus among multiple nodes. Currently, the project features basic node communication, leader election, and an in-memory storage engine.

## ✨ Current Features

* **Leader Election:** Nodes can transition between Follower, Candidate, and Leader states based on randomized election timeouts.
* **Heartbeat Mechanism:** The elected Leader periodically sends heartbeats to maintain authority and prevent unnecessary elections.
* **In-Memory Storage:** A thread-safe, mutex-guarded in-memory key-value store (`storage` package).
* **HTTP API:** Nodes expose RESTful endpoints for client interaction (`/get`, `/set`) and intra-cluster communication (RPCs over HTTP).
* **Multi-Node Cluster Simulation:** The `cmd/node` binary can be run as different nodes to form a local cluster.

## 🛠️ Tech Stack

* **Language:** Go (Golang)
* **Communication:** HTTP/REST for both Client-Node and Node-Node communication.
* **Concurrency:** Go routines and Channels/Mutexes for handling asynchronous state transitions and thread-safe data access.

## 📂 Project Structure

```text
├── cmd/
│   └── node/
│       └── main.go       # Application entry point, cluster setup, and HTTP server
├── network/
│   ├── client.go         # Outgoing HTTP requests to peers
│   ├── cluster.go        # Cluster membership management
│   ├── heartbeat.go      # Raft AppendEntries (Heartbeat) RPC definitions
│   ├── http.go           # HTTP Handlers for KV operations and Raft RPCs
│   └── vote.go           # Raft RequestVote RPC definitions
├── raft/
│   ├── election.go       # Election timer and voting logic
│   ├── messages.go       # Raft RPC message structs
│   ├── node.go           # Core Raft Node state machine and methods
│   └── state.go          # Node state definitions (Follower, Candidate, Leader)
├── storage/
│   └── kv.go             # Thread-safe in-memory key-value map
└── go.mod                # Go module dependencies
```

## 🚀 Getting Started

### Prerequisites

* Go 1.18 or higher installed.

### Running a Local Cluster

You can start a 3-node cluster locally. Open three separate terminal windows and run:

**Node 1:**
```bash
go run cmd/node/main.go -id 1 -port 8001
```

**Node 2:**
```bash
go run cmd/node/main.go -id 2 -port 8002
```

**Node 3:**
```bash
go run cmd/node/main.go -id 3 -port 8003
```

Watch the terminal output to see the nodes start as Followers, trigger election timeouts, and elect a Leader!

### Interacting with the KV Store

Once a leader is elected (check the terminal logs), you can interact with the store via HTTP. 
*(Note: Currently, requests must be sent directly to the leader, and log replication is a work in progress).*

**Set a Value:**
```bash
curl -X POST -H "Content-Type: application/json" -d '{"key":"name", "value":"raft-kv"}' http://localhost:<LEADER_PORT>/set
```

**Get a Value:**
```bash
curl http://localhost:<LEADER_PORT>/get?key=name
```
