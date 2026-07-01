// Simulates a network of RaftNodes on a virtual clock: message delivery is
// delayed by a configurable latency and can be dropped entirely to model a
// network partition. Driving `tick(dt)` repeatedly is equivalent to letting
// real time pass, but deterministically and without waiting.

import { RaftNode } from "./raft.js";

export class Cluster {
  constructor(nodeCount, opts = {}) {
    this.now = 0;
    this.latency = opts.latency ?? 10; // ms, one-way
    this.latencyJitter = opts.latencyJitter ?? 5;
    this.rng = opts.rng || Math.random;

    const ids = Array.from({ length: nodeCount }, (_, i) => i);
    this.nodes = new Map(
      ids.map((id) => [id, new RaftNode(id, ids, { rng: this.rng })])
    );

    // linkUp[a][b] === false means messages between a and b are dropped.
    this.linkUp = {};
    for (const a of ids) {
      this.linkUp[a] = {};
      for (const b of ids) this.linkUp[a][b] = true;
    }

    this.inbox = []; // { deliverAt, from, to, msg }
    this.history = []; // log of delivered messages, for debugging/tests
  }

  nodeList() {
    return Array.from(this.nodes.values());
  }

  leader() {
    return (
      this.nodeList().find((n) => n.state === "leader" && n.alive) || null
    );
  }

  setLinkUp(a, b, up) {
    this.linkUp[a][b] = up;
    this.linkUp[b][a] = up;
  }

  partition(groupA, groupB) {
    for (const a of groupA) {
      for (const b of groupB) this.setLinkUp(a, b, false);
    }
  }

  healAll() {
    for (const a of this.nodes.keys()) {
      for (const b of this.nodes.keys()) this.linkUp[a][b] = true;
    }
  }

  killNode(id) {
    this.nodes.get(id).alive = false;
  }

  reviveNode(id) {
    this.nodes.get(id).alive = true;
  }

  send(from, to, msg) {
    if (!this.linkUp[from][to]) return; // dropped by partition
    const jitter = (this.rng() - 0.5) * 2 * this.latencyJitter;
    const deliverAt = this.now + Math.max(1, this.latency + jitter);
    this.inbox.push({ deliverAt, from, to, msg });
  }

  tick(dt) {
    this.now += dt;

    // Deliver due messages first, then advance timers, matching how a real
    // event loop would interleave I/O and timeout callbacks within a tick.
    const due = this.inbox.filter((m) => m.deliverAt <= this.now);
    this.inbox = this.inbox.filter((m) => m.deliverAt > this.now);
    for (const { to, msg, from } of due) {
      const node = this.nodes.get(to);
      if (node && node.alive && this.linkUp[from][to]) {
        this.history.push({ at: this.now, from, to, type: msg.type });
        node.handleMessage(msg, this);
      }
    }

    for (const node of this.nodes.values()) {
      node.advance(dt, this);
    }
  }

  run(totalMs, stepMs = 5) {
    let remaining = totalMs;
    while (remaining > 0) {
      const step = Math.min(stepMs, remaining);
      this.tick(step);
      remaining -= step;
    }
  }
}
