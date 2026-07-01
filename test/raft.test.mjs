import test from "node:test";
import assert from "node:assert/strict";
import { Cluster } from "../src/cluster.js";

// Small seeded PRNG (mulberry32) so election timeouts are reproducible.
function mulberry32(seed) {
  return function () {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

test("a lone node immediately elects itself leader", () => {
  const cluster = new Cluster(1, { rng: mulberry32(1) });
  cluster.run(400);
  const leader = cluster.leader();
  assert.ok(leader, "expected a leader");
  assert.equal(leader.id, 0);
});

test("a 5 node cluster elects exactly one leader", () => {
  const cluster = new Cluster(5, { rng: mulberry32(42) });
  cluster.run(2000);
  const leaders = cluster.nodeList().filter((n) => n.state === "leader");
  assert.equal(leaders.length, 1, "exactly one leader should be elected");
});

test("no two nodes are leader in the same term (safety)", () => {
  const cluster = new Cluster(5, { rng: mulberry32(7) });
  const leadersByTerm = new Map();
  for (let i = 0; i < 400; i++) {
    cluster.tick(10);
    for (const n of cluster.nodeList()) {
      if (n.state === "leader") {
        const existing = leadersByTerm.get(n.currentTerm);
        if (existing !== undefined && existing !== n.id) {
          assert.fail(
            `two different leaders (${existing} and ${n.id}) in term ${n.currentTerm}`
          );
        }
        leadersByTerm.set(n.currentTerm, n.id);
      }
    }
  }
});

test("a committed log entry replicates to a majority of followers", () => {
  const cluster = new Cluster(5, { rng: mulberry32(99) });
  cluster.run(1000);
  const leader = cluster.leader();
  assert.ok(leader);

  leader.appendCommand("SET x=1");
  cluster.run(500);

  assert.ok(leader.commitIndex >= 0, "leader should have committed the entry");
  const replicatedCount = cluster
    .nodeList()
    .filter((n) => n.log.some((e) => e.command === "SET x=1")).length;
  assert.ok(
    replicatedCount > cluster.nodeList().length / 2,
    "entry should reach a majority of nodes"
  );
});

test("cluster recovers a new leader after partitioning the old leader away", () => {
  const cluster = new Cluster(5, { rng: mulberry32(5) });
  cluster.run(1000);
  const oldLeader = cluster.leader();
  assert.ok(oldLeader);

  const minority = [oldLeader.id];
  const majority = cluster
    .nodeList()
    .map((n) => n.id)
    .filter((id) => id !== oldLeader.id);
  cluster.partition(minority, majority);

  cluster.run(2000);

  const newLeader = cluster
    .nodeList()
    .find((n) => n.state === "leader" && majority.includes(n.id));
  assert.ok(newLeader, "the majority partition should elect a new leader");
  assert.notEqual(newLeader.id, oldLeader.id);
  assert.ok(newLeader.currentTerm > oldLeader.currentTerm);
});

test("after healing a partition, the stale leader steps down", () => {
  const cluster = new Cluster(5, { rng: mulberry32(5) });
  cluster.run(1000);
  const oldLeader = cluster.leader();
  const minority = [oldLeader.id];
  const majority = cluster
    .nodeList()
    .map((n) => n.id)
    .filter((id) => id !== oldLeader.id);
  cluster.partition(minority, majority);
  cluster.run(2000);
  cluster.healAll();
  cluster.run(1000);

  const leaders = cluster.nodeList().filter((n) => n.state === "leader");
  assert.equal(leaders.length, 1, "cluster should converge on one leader");
  assert.notEqual(leaders[0].id, oldLeader.id);
});
