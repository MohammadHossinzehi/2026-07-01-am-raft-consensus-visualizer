# Raft Consensus Visualizer

An interactive, from scratch implementation of the Raft distributed consensus algorithm: leader election and log replication, simulated on a virtual clock and rendered live in the browser.

Why this exists: Raft is the algorithm behind etcd, Consul, CockroachDB and many other distributed systems. Understanding it means being able to answer "how do these nodes agree on anything without a single point of failure or split brain?" This project implements the algorithm itself, not just a diagram of it: RequestVote and AppendEntries RPCs, term numbers, election timeouts, log matching, and the commit index majority rule. It then wraps that in a small simulator with a browser UI so you can partition the network, kill nodes, and watch the cluster elect a new leader and converge.

## What it does

5 simulated Raft nodes run independently, each a full state machine (follower, candidate, or leader) with terms, votes, and a replicated log. A discrete event `Cluster` class stands in for the network: messages are delivered after a configurable latency, and can be dropped by simulated partitions. Because the "clock" is virtual (advanced by calling `tick(dt)`), the simulation runs instantly and deterministically, with no setTimeout races and no flakiness in tests.

A canvas UI shows node state (leader, candidate, follower, or down) in real time, in flight RPCs as moving dots, and controls to:

send a client write to whichever node is currently leader, partition any node away from the rest of the cluster (and heal it), kill and revive nodes to simulate a crash, and scrub simulation speed or pause and resume.

## How to run it

No build step and no dependencies, it is plain ES modules.

Browser UI:

1. Clone the repo.
2. Serve the folder with any static file server, e.g. `npx serve .` or `python3 -m http.server`.
3. Open `index.html` (or the localhost URL it prints) in a browser.
4. Watch the 5 nodes elect a leader within a second or two, then try "Partition N<x>" on the leader and watch the rest of the cluster elect a new one.

Tests (pure Node, no browser needed):

```
npm test
```

which runs `node --test test/*.test.mjs`. All 6 tests pass locally on Node 22.

## Design notes

Discrete event simulation instead of real timers. Every `RaftNode.advance(dt)` and `Cluster.tick(dt)` takes an explicit elapsed time argument rather than reading `Date.now()` or using `setTimeout`. This is what makes the test suite fast and deterministic: a test can simulate two seconds of cluster time in a few milliseconds of wall clock time, and a seeded PRNG makes election timeouts reproducible across runs.

The core algorithm in `src/raft.js` has zero DOM dependencies. It only knows about `cluster.send(...)`, it does not know or care whether the cluster is a browser simulation or a real network. That is what let me unit test leader election and log replication headlessly in `test/raft.test.mjs`, and reuse the exact same class for the interactive visualization in `src/ui.js`.

Safety property tested directly: the suite asserts there is never more than one leader per term, which is Raft's central safety guarantee, as opposed to just checking that a leader eventually exists.

Partition handling models an asymmetric network: `setLinkUp(a, b, false)` removes a link in both directions, so a node cut off from the majority keeps believing it is leader (it never sees a higher term) until the partition heals and a heartbeat from the new leader forces it to step down. This is deliberate, since that is exactly the "stale leader" scenario Raft is designed to handle safely: it cannot commit new entries without a majority, so it cannot cause harm even while confused.

What is simplified versus real Raft: no persistent storage or restart recovery, no log compaction or snapshots, and no dynamic cluster membership changes. Those are the natural next extensions but are orthogonal to demonstrating the core election and replication safety properties.

## Project structure

```
index.html          UI shell and styling
src/raft.js          RaftNode state machine (election + log replication)
src/cluster.js       Discrete event network simulator (latency, partitions, crashes)
src/ui.js            Canvas rendering + controls, drives the simulation loop
test/raft.test.mjs   Node test suite (election safety, replication, partition recovery)
package.json
```
