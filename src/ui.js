import { Cluster } from "./cluster.js";

const NODE_COUNT = 5;
const COLORS = {
  leader: "#f5c451",
  candidate: "#ef8354",
  follower: "#4f8cff",
  dead: "#3a3d47",
};

let cluster = new Cluster(NODE_COUNT, { latency: 20, latencyJitter: 10 });
let playing = true;
let speed = 1;
let writeCounter = 0;
const partitioned = new Set(); // node ids manually cut off from everyone

const canvas = document.getElementById("canvas");
const ctx = canvas.getContext("2d");
const logEl = document.getElementById("log");

function resizeCanvas() {
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * devicePixelRatio;
  canvas.height = rect.height * devicePixelRatio;
  canvas.style.width = rect.width + "px";
  canvas.style.height = rect.height + "px";
  ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
}
window.addEventListener("resize", resizeCanvas);
resizeCanvas();

function nodePosition(id, cx, cy, r) {
  const angle = (2 * Math.PI * id) / NODE_COUNT - Math.PI / 2;
  return { x: cx + r * Math.cos(angle), y: cy + r * Math.sin(angle) };
}

function log(msg) {
  const line = document.createElement("div");
  const t = (cluster.now / 1000).toFixed(2);
  line.textContent = `[t=${t}s] ${msg}`;
  logEl.prepend(line);
  while (logEl.children.length > 200) logEl.removeChild(logEl.lastChild);
}

let lastStates = new Map();
function watchStateChanges() {
  for (const n of cluster.nodeList()) {
    const prev = lastStates.get(n.id);
    if (prev && prev.state !== n.state) {
      log(`node ${n.id} became ${n.state} (term ${n.currentTerm})`);
    }
    lastStates.set(n.id, { state: n.state });
  }
}

function draw() {
  const rect = canvas.parentElement.getBoundingClientRect();
  const cx = rect.width / 2;
  const cy = rect.height / 2;
  const r = Math.min(rect.width, rect.height) / 2 - 70;

  ctx.clearRect(0, 0, rect.width, rect.height);

  // connections
  for (const a of cluster.nodeList()) {
    for (const b of cluster.nodeList()) {
      if (a.id >= b.id) continue;
      const pa = nodePosition(a.id, cx, cy, r);
      const pb = nodePosition(b.id, cx, cy, r);
      const up = cluster.linkUp[a.id][b.id];
      ctx.beginPath();
      ctx.moveTo(pa.x, pa.y);
      ctx.lineTo(pb.x, pb.y);
      ctx.strokeStyle = up ? "rgba(255,255,255,0.08)" : "rgba(239,83,80,0.35)";
      ctx.lineWidth = up ? 1 : 2;
      ctx.setLineDash(up ? [] : [4, 4]);
      ctx.stroke();
      ctx.setLineDash([]);
    }
  }

  // in-flight messages
  for (const m of cluster.inbox) {
    const a = nodePosition(m.from, cx, cy, r);
    const b = nodePosition(m.to, cx, cy, r);
    const progress = Math.min(
      1,
      Math.max(0, 1 - (m.deliverAt - cluster.now) / (cluster.latency * 2 + 1))
    );
    const x = a.x + (b.x - a.x) * progress;
    const y = a.y + (b.y - a.y) * progress;
    ctx.beginPath();
    ctx.arc(x, y, 3, 0, Math.PI * 2);
    ctx.fillStyle = m.msg.type.startsWith("RequestVote") ? "#f5c451" : "#4fd1ff";
    ctx.fill();
  }

  // nodes
  for (const n of cluster.nodeList()) {
    const { x, y } = nodePosition(n.id, cx, cy, r);
    const dead = !n.alive || partitioned.has(n.id);
    const color = dead ? COLORS.dead : COLORS[n.state];

    ctx.beginPath();
    ctx.arc(x, y, 26, 0, Math.PI * 2);
    ctx.fillStyle = "#171a23";
    ctx.fill();
    ctx.lineWidth = 3;
    ctx.strokeStyle = color;
    ctx.stroke();

    ctx.fillStyle = "#e6e8ee";
    ctx.font = "12px ui-monospace, monospace";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText(`N${n.id}`, x, y - 4);
    ctx.font = "10px ui-monospace, monospace";
    ctx.fillStyle = "#8b90a0";
    ctx.fillText(`t${n.currentTerm} log:${n.log.length}`, x, y + 10);

    ctx.font = "10px sans-serif";
    ctx.fillStyle = color;
    ctx.fillText(dead ? "down" : n.state, x, y + 40);
  }
}

let lastFrame = performance.now();
function frame(now) {
  const dt = now - lastFrame;
  lastFrame = now;
  if (playing) {
    cluster.tick(Math.min(dt, 100) * speed);
    watchStateChanges();
  }
  draw();
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);

// --- controls -------------------------------------------------------------

document.getElementById("playPause").addEventListener("click", (e) => {
  playing = !playing;
  e.target.textContent = playing ? "Pause" : "Play";
});

document.getElementById("reset").addEventListener("click", () => {
  cluster = new Cluster(NODE_COUNT, { latency: 20, latencyJitter: 10 });
  partitioned.clear();
  lastStates = new Map();
  logEl.innerHTML = "";
  renderFaultButtons();
  log("cluster reset");
});

const speedInput = document.getElementById("speed");
speedInput.addEventListener("input", () => {
  speed = parseFloat(speedInput.value);
  document.getElementById("speedVal").textContent = `${speed}x`;
});

document.getElementById("writeCmd").addEventListener("click", () => {
  const leader = cluster.leader();
  if (!leader) {
    log("no leader available right now, try again shortly");
    return;
  }
  writeCounter += 1;
  const cmd = `SET x=${writeCounter}`;
  leader.appendCommand(cmd);
  log(`client sent "${cmd}" to leader ${leader.id}`);
});

document.getElementById("healAll").addEventListener("click", () => {
  cluster.healAll();
  partitioned.clear();
  renderFaultButtons();
  log("all partitions healed");
});

function renderFaultButtons() {
  const partitionWrap = document.getElementById("partitionBtns");
  const killWrap = document.getElementById("killBtns");
  partitionWrap.innerHTML = "";
  killWrap.innerHTML = "";

  for (const n of cluster.nodeList()) {
    const pBtn = document.createElement("button");
    pBtn.textContent = `Partition N${n.id}`;
    pBtn.className = partitioned.has(n.id) ? "active" : "";
    pBtn.addEventListener("click", () => {
      const others = cluster
        .nodeList()
        .map((x) => x.id)
        .filter((id) => id !== n.id);
      if (partitioned.has(n.id)) {
        for (const o of others) cluster.setLinkUp(n.id, o, true);
        partitioned.delete(n.id);
        log(`node ${n.id} reconnected to the cluster`);
      } else {
        for (const o of others) cluster.setLinkUp(n.id, o, false);
        partitioned.add(n.id);
        log(`node ${n.id} partitioned away from the cluster`);
      }
      renderFaultButtons();
    });
    partitionWrap.appendChild(pBtn);

    const kBtn = document.createElement("button");
    kBtn.textContent = n.alive ? `Kill N${n.id}` : `Revive N${n.id}`;
    kBtn.addEventListener("click", () => {
      if (n.alive) {
        cluster.killNode(n.id);
        log(`node ${n.id} crashed`);
      } else {
        cluster.reviveNode(n.id);
        log(`node ${n.id} revived`);
      }
      renderFaultButtons();
    });
    killWrap.appendChild(kBtn);
  }
}
renderFaultButtons();
log("cluster started with 5 nodes");
