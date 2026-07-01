// A from-scratch implementation of the core Raft consensus algorithm state
// machine (leader election + log replication), driven by a discrete-event
// virtual clock so it can be simulated instantly and tested deterministically
// (no real timers, no flakiness).

export const ELECTION_TIMEOUT_MIN = 150;
export const ELECTION_TIMEOUT_MAX = 300;
export const HEARTBEAT_INTERVAL = 50;

let messageIdCounter = 0;

export class RaftNode {
  constructor(id, peerIds, opts = {}) {
    this.id = id;
    this.peerIds = peerIds.filter((p) => p !== id);
    this.rng = opts.rng || Math.random;

    // Persistent state (would survive a restart on real disk).
    this.currentTerm = 0;
    this.votedFor = null;
    this.log = []; // { term, command }

    // Volatile state.
    this.commitIndex = -1;
    this.lastApplied = -1;
    this.state = "follower"; // follower | candidate | leader
    this.leaderId = null;

    // Election bookkeeping.
    this.votesReceived = new Set();
    this.electionElapsed = 0;
    this.electionTimeout = this._randomElectionTimeout();

    // Leader-only bookkeeping.
    this.nextIndex = {};
    this.matchIndex = {};
    this.heartbeatElapsed = 0;

    this.alive = true; // simulates a crashed node when false
  }

  _randomElectionTimeout() {
    const span = ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN;
    return ELECTION_TIMEOUT_MIN + this.rng() * span;
  }

  lastLogIndex() {
    return this.log.length - 1;
  }

  lastLogTerm() {
    const idx = this.lastLogIndex();
    return idx >= 0 ? this.log[idx].term : 0;
  }

  // Called every simulation tick with the elapsed virtual ms.
  advance(dt, cluster) {
    if (!this.alive) return;

    if (this.state === "leader") {
      this.heartbeatElapsed += dt;
      if (this.heartbeatElapsed >= HEARTBEAT_INTERVAL) {
        this.heartbeatElapsed = 0;
        this._sendAppendEntriesToAll(cluster);
      }
    } else {
      this.electionElapsed += dt;
      if (this.electionElapsed >= this.electionTimeout) {
        this._startElection(cluster);
      }
    }

    // Apply newly committed entries (no-op state machine here; a real
    // application would execute `command` against its own data model).
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied += 1;
    }
  }

  _startElection(cluster) {
    this.state = "candidate";
    this.currentTerm += 1;
    this.votedFor = this.id;
    this.votesReceived = new Set([this.id]);
    this.electionElapsed = 0;
    this.electionTimeout = this._randomElectionTimeout();
    this.leaderId = null;

    for (const peer of this.peerIds) {
      cluster.send(this.id, peer, {
        id: ++messageIdCounter,
        type: "RequestVote",
        term: this.currentTerm,
        candidateId: this.id,
        lastLogIndex: this.lastLogIndex(),
        lastLogTerm: this.lastLogTerm(),
      });
    }

    // A lone node (no peers) wins immediately.
    if (this.peerIds.length === 0) {
      this._becomeLeader();
    }
  }

  _stepDownIfStale(term) {
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
      this.state = "follower";
      this.leaderId = null;
      this.electionElapsed = 0;
      this.electionTimeout = this._randomElectionTimeout();
      return true;
    }
    return false;
  }

  _becomeLeader() {
    this.state = "leader";
    this.leaderId = this.id;
    this.nextIndex = {};
    this.matchIndex = {};
    for (const peer of this.peerIds) {
      this.nextIndex[peer] = this.log.length;
      this.matchIndex[peer] = -1;
    }
    this.heartbeatElapsed = HEARTBEAT_INTERVAL; // send heartbeat on next tick
  }

  // Client interface: append a command to the leader's log.
  appendCommand(command) {
    if (this.state !== "leader") {
      throw new Error(`node ${this.id} is not the leader`);
    }
    this.log.push({ term: this.currentTerm, command });
    return this.lastLogIndex();
  }

  handleMessage(msg, cluster) {
    if (!this.alive) return;
    this._stepDownIfStale(msg.term);

    switch (msg.type) {
      case "RequestVote":
        this._handleRequestVote(msg, cluster);
        break;
      case "RequestVoteResponse":
        this._handleRequestVoteResponse(msg);
        break;
      case "AppendEntries":
        this._handleAppendEntries(msg, cluster);
        break;
      case "AppendEntriesResponse":
        this._handleAppendEntriesResponse(msg);
        break;
      default:
        break;
    }
  }

  _handleRequestVote(msg, cluster) {
    let granted = false;
    const logOk =
      msg.lastLogTerm > this.lastLogTerm() ||
      (msg.lastLogTerm === this.lastLogTerm() &&
        msg.lastLogIndex >= this.lastLogIndex());

    if (
      msg.term === this.currentTerm &&
      (this.votedFor === null || this.votedFor === msg.candidateId) &&
      logOk
    ) {
      granted = true;
      this.votedFor = msg.candidateId;
      this.electionElapsed = 0;
    }

    cluster.send(this.id, msg.candidateId, {
      id: ++messageIdCounter,
      type: "RequestVoteResponse",
      term: this.currentTerm,
      voterId: this.id,
      granted,
    });
  }

  _handleRequestVoteResponse(msg) {
    if (this.state !== "candidate" || msg.term !== this.currentTerm) return;
    if (msg.granted) {
      this.votesReceived.add(msg.voterId);
      const clusterSize = this.peerIds.length + 1;
      if (this.votesReceived.size > clusterSize / 2) {
        this._becomeLeader();
      }
    }
  }

  _sendAppendEntriesToAll(cluster) {
    for (const peer of this.peerIds) {
      const nextIdx = this.nextIndex[peer] ?? this.log.length;
      const prevLogIndex = nextIdx - 1;
      const prevLogTerm = prevLogIndex >= 0 ? this.log[prevLogIndex].term : 0;
      cluster.send(this.id, peer, {
        id: ++messageIdCounter,
        type: "AppendEntries",
        term: this.currentTerm,
        leaderId: this.id,
        prevLogIndex,
        prevLogTerm,
        entries: this.log.slice(nextIdx),
        leaderCommit: this.commitIndex,
      });
    }
  }

  _handleAppendEntries(msg, cluster) {
    if (msg.term < this.currentTerm) {
      cluster.send(this.id, msg.leaderId, {
        id: ++messageIdCounter,
        type: "AppendEntriesResponse",
        term: this.currentTerm,
        success: false,
        followerId: this.id,
        matchIndex: -1,
      });
      return;
    }

    // A valid leader for our term: stay/become follower and reset timer.
    this.state = "follower";
    this.leaderId = msg.leaderId;
    this.electionElapsed = 0;

    const hasPrev =
      msg.prevLogIndex === -1 ||
      (this.log[msg.prevLogIndex] &&
        this.log[msg.prevLogIndex].term === msg.prevLogTerm);

    if (!hasPrev) {
      cluster.send(this.id, msg.leaderId, {
        id: ++messageIdCounter,
        type: "AppendEntriesResponse",
        term: this.currentTerm,
        success: false,
        followerId: this.id,
        matchIndex: -1,
      });
      return;
    }

    // Splice in new entries, truncating any conflicting suffix.
    let idx = msg.prevLogIndex + 1;
    for (const entry of msg.entries) {
      if (this.log[idx] && this.log[idx].term !== entry.term) {
        this.log = this.log.slice(0, idx);
      }
      if (!this.log[idx]) {
        this.log.push(entry);
      }
      idx += 1;
    }

    if (msg.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(msg.leaderCommit, this.log.length - 1);
    }

    cluster.send(this.id, msg.leaderId, {
      id: ++messageIdCounter,
      type: "AppendEntriesResponse",
      term: this.currentTerm,
      success: true,
      followerId: this.id,
      matchIndex: msg.prevLogIndex + msg.entries.length,
    });
  }

  _handleAppendEntriesResponse(msg) {
    if (this.state !== "leader" || msg.term !== this.currentTerm) return;

    if (msg.success) {
      this.matchIndex[msg.followerId] = msg.matchIndex;
      this.nextIndex[msg.followerId] = msg.matchIndex + 1;

      // Advance commitIndex if a majority has replicated an entry from
      // the current term (the core Raft safety rule).
      for (let n = this.log.length - 1; n > this.commitIndex; n--) {
        if (this.log[n].term !== this.currentTerm) continue;
        let count = 1; // self
        for (const peer of this.peerIds) {
          if (this.matchIndex[peer] >= n) count += 1;
        }
        const clusterSize = this.peerIds.length + 1;
        if (count > clusterSize / 2) {
          this.commitIndex = n;
          break;
        }
      }
    } else {
      this.nextIndex[msg.followerId] = Math.max(
        0,
        (this.nextIndex[msg.followerId] ?? this.log.length) - 1
      );
    }
  }
}
