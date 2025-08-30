# Raft Distributed Consensus Algorithm Implementation

This project is an implementation of the Raft distributed consensus algorithm in Go. Raft is a consensus algorithm designed for managing replicated logs, ensuring data consistency across multiple nodes in a distributed system through leader election, log replication, and other mechanisms.

## Project Overview

This implementation is structured as a series of lab assignments, progressively building the complete Raft algorithm functionality:

### Part A: Leader Election
- Implement Raft's leader election and heartbeat mechanisms
- Followers transition to Candidates when election timeout occurs
- Candidates become Leaders after receiving majority votes
- Leaders send periodic heartbeats to maintain leadership

### Part B: Log Replication
- Implement mechanism for Leaders to replicate log entries to Followers
- Leaders receive client requests and append them as log entries
- Use `AppendEntries` RPC to replicate log entries to other nodes
- Implement log consistency checks to ensure Follower logs match Leader

### Part C: Persistence
- Implement persistence of Raft node state for crash recovery
- Persistent state includes `currentTerm`, `votedFor`, and `log`
- Write state changes to stable storage promptly

### Part D: Log Compaction (Snapshots)
- Implement log snapshotting to prevent unbounded log growth
- Create snapshots of current state when log grows too large
- Leaders can send snapshots to lagging Followers for fast catch-up

## Running Tests

Use Go's testing tools to run tests for each part:

```bash
# Navigate to the Raft source directory
cd src/raft

# Run Part A tests
go test -run PartA -race

# Run Part B tests
go test -run PartB -race

# Run Part C tests
go test -run PartC -race

# Run Part D tests
go test -run PartD -race

# Run all tests
go test -race
```

The `-race` flag enables race detection, which is crucial for concurrent programming.

## Code Structure

- **`raft.go`**: Contains core Raft algorithm implementation including the `Raft` struct, RPC handlers (`RequestVote`, `AppendEntries`), and state transition logic
- **`util.go`**: Provides utility functions, primarily for debug logging
- **`persister.go`**: Provides persistent storage interface
- **`config.go`**: (Test code) Contains testing framework for simulating network environments and node behavior

## Key Features

- **Leader Election**: Robust leader election with randomized timeouts
- **Log Replication**: Reliable log replication with consistency guarantees
- **Fault Tolerance**: Handles network partitions and node failures
- **Persistence**: State persistence for crash recovery
- **Log Compaction**: Efficient log management through snapshots

## Algorithm Details

The implementation follows the Raft paper specifications:
- **Safety**: Never returns incorrect results under non-Byzantine conditions
- **Availability**: Functional as long as majority of servers are operational
- **Performance**: Does not depend on timing for consistency
- **Simplicity**: Designed for understandability and practical implementation
