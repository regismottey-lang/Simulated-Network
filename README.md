# Raft Consensus in a Simulated Network
An implementation of the Raft consensus algorithm (leader election, log replication, commit) running inside a deterministic simulated network with partitions, message drops, delays, and node crashes.

Demonstrates: distributed consensus, state machine replication, fault injection, deterministic simulation, invariant checking.
# How it works
Nodes implement Follower, Candidate, and Leader roles with randomized election timeouts, term handling, vote granting with the log up-to-date check, AppendEntries with log-consistency checks and conflict resolution, and a no-op entry on election so earlier-term entries can commit. Leaders only commit current-term entries by counting replicas.
Cluster simulates the network on a tick clock: delayed delivery, random drops, network partitions, and crash/restart (persistent state like term, vote, and log survives a restart; volatile state is rebuilt).
Safety invariants are checked after every tick:
Election safety: at most one leader per term.
State machine safety: once an entry is committed, every node's log holds the same entry at that index.
#What the program runs
Scenario: elect a leader, commit entries, partition the old leader into a minority, let the majority elect a new leader and commit more, then heal. An uncommitted entry on the isolated leader is discarded and all 5 nodes converge on the same applied commands.
Chaos test: 50 random seeds with 10% message drops plus random partitions, heals, crashes, and restarts, checking invariants throughout.

Exit status is nonzero if anything fails.
# Build and run
g++ -std=c++20 -O2 -Wall -Wextra project12_raft.cpp -o raft

./raft
