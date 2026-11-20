// Panoul de control PAD — logica interfeței (fără biblioteci externe).
"use strict";

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const uuid = () => (crypto.randomUUID ? crypto.randomUUID() :
  "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => { const r = Math.random() * 16 | 0; return (c === "x" ? r : (r & 3 | 8)).toString(16); }));

const S = {
  info: null,
  lab: null,
  procs: [],
  lines: new Map(), // pid -> [text]
  since: 0,
  logSel: "all",
  evSince: 0,
  broker: null,
  accounts: null,
  held: new Map(),
};

const DEMOS = {
  lab1: [["all", "toate scenariile lab. 1"], ["critic", "scenariul critic"], ["producer", "ACK pierdut către producer"], ["retry", "retry + DLQ"], ["ordering", "ordonare"]],
  lab2: [["lab2", "toate scenariile lab. 2"], ["rpc-critic", "scenariul critic (răspuns pierdut)"], ["ambiguitate", "rezultate explicite"],
    ["versiuni", "versionare și compatibilitate"], ["deadline", "deadline și cancellation"], ["lant", "lanțul complet"],
    ["lab1", "scenariile lab. 1 (din folderul lab. 2)"], ["toate", "lab. 1 + lab. 2"]],
};

// ───────────────────────── utilitare HTTP ─────────────────────────

function toast(msg, err) {
  const t = $("toast");
  t.textContent = msg;
  t.className = "toast show" + (err ? " err" : "");
  clearTimeout(toast._t);
  toast._t = setTimeout(() => (t.className = "toast"), err ? 5000 : 2500);
}

async function call(method, path, body) {
  const r = await fetch(path, { method, headers: { "Content-Type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body) });
  let data = {};
  try { data = await r.json(); } catch (_) {}
  return { ok: r.ok, status: r.status, data };
}

/** POST către API-ul panoului, cu parametri în query string. */
async function api(path, params = {}, okMsg) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null && v !== "") qs.set(k, v);
  const r = await call("POST", path + (qs.toString() ? "?" + qs : ""));
  if (!r.ok) { toast(r.data.error || ("eroare " + r.status), true); return null; }
  if (okMsg) toast(okMsg);
  refreshState();
  return r.data;
}

const start = (kind, params = {}) => api("/api/start", { kind, ...params });

// ───────────────────────── inițializare / laboratoare ─────────────────────────

async function init() {
  const r = await call("GET", "/api/info");
  S.info = r.data;
  S.lab = r.data.active;
  $("chipGo").textContent = r.data.go ? r.data.go.replace("go version ", "") : "Go: lipsește!";
  renderTabs();
  applyLab();
  loop();
}

function renderTabs() {
  const all = [["lab1", "Laboratorul 1"], ["lab2", "Laboratorul 2"]];
  $("labTabs").innerHTML = all.map(([id, t]) => {
    const has = S.info.labs.some((l) => l.id === id);
    return `<button data-id="${id}" class="${S.lab === id ? "active" : ""}" ${has ? "" : "disabled title='nu există în acest branch'"}>${t}</button>`;
  }).join("");
  $("labTabs").querySelectorAll("button").forEach((b) => (b.onclick = () => selectLab(b.dataset.id)));
}

async function selectLab(id) {
  if (id === S.lab) return;
  const running = S.procs.some((p) => p.status === "running");
  if (running && !confirm("Schimbarea laboratorului oprește procesele pornite (folosesc aceleași porturi). Continui?")) return;
  if (!(await api("/api/lab", { id }))) return;
  S.lab = id;
  S.evSince = 0;
  $("evBody").innerHTML = "";
  S.held.clear();
  renderManual();
  renderTabs();
  applyLab();
}

function applyLab() {
  document.body.className = "lab-" + S.lab;
  $("demoScenario").innerHTML = DEMOS[S.lab].map(([v, t]) => `<option value="${v}">${t}</option>`).join("");
  $("cEffect").value = "remote";
}

function updateBanner(built) {
  const b = $("banner");
  if (!S.info.go) {
    b.className = "banner bad";
    b.innerHTML = "<b>Go nu a fost găsit.</b> Instalează Go 1.22+ de pe <code>go.dev/dl</code>, închide și redeschide terminalul, apoi repornește panoul (<code>cargo run --release</code>).";
  } else if (!built && !isRunning("build")) {
    b.className = "banner";
    b.innerHTML = "Executabilele laboratorului nu sunt compilate încă. <button onclick='build()'>⚒ Compilează acum</button> <span class='muted'>(prima dată durează până la un minut)</span>";
  } else if (isRunning("build")) {
    b.className = "banner";
    b.innerHTML = "⚒ Se compilează laboratorul… urmărește jurnalul <b>compilare</b>.";
  } else {
    b.className = "banner hidden";
  }
}

// ───────────────────────── procese ─────────────────────────

const isRunning = (kind, name) => S.procs.some((p) => p.status === "running" && p.kind === kind && (!name || p.name === name));
const procById = (id) => S.procs.find((p) => p.id === id);

function procStatus(p) {
  if (p.status === "running") return { cls: "running", text: "rulează" };
  if (p.status === "stopped") return { cls: p.kind === "consumer" ? "crashed" : "", text: "oprit (kill)" };
  if (p.exit_code === 0) return { cls: "", text: "terminat ✓" };
  if (p.exit_code === 3) return { cls: "crashed", text: "CĂZUT (simulat, cod 3)" };
  return { cls: "crashed", text: "eroare, cod " + p.exit_code };
}

const KIND_LABEL = { build: "⚒", broker: "📨", accounts: "🏦", consumer: "⚙", producer: "✉", demo: "▶" };

function renderProcs() {
  const list = [...S.procs].reverse();
  if (!list.length) { $("procList").innerHTML = '<p class="muted">Niciun proces.</p>'; return; }
  $("procList").innerHTML = list.map((p) => {
    const st = procStatus(p);
    const btn = p.status === "running"
      ? `<button class="small ${p.kind === "consumer" ? "bad" : "ghost"}" onclick="event.stopPropagation();stopProc(${p.id})" title="oprește brutal (kill)">${p.kind === "consumer" ? "💥 kill" : "■"}</button>`
      : "";
    return `<div class="proc ${st.cls}" onclick="selectLog(${p.id})" title="${esc(p.cmdline)}">
      <span class="dot"></span>
      <div><div class="pn">${KIND_LABEL[p.kind] || ""} ${esc(p.name)}</div><div class="ps">${st.text}</div></div>${btn}</div>`;
  }).join("");
}

async function stopProc(id) { await api("/api/stop", { id }); }

async function stopKind(kind) {
  for (const p of S.procs.filter((p) => p.kind === kind && p.status === "running")) await api("/api/stop", { id: p.id });
}

async function waitExit(id, timeout = 60000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeout) {
    await refreshState();
    const p = procById(id);
    if (p && p.status !== "running") return p;
    await sleep(250);
  }
  return procById(id);
}

async function waitUntil(fn, timeout = 10000, step = 250) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeout) {
    try { if (await fn()) return true; } catch (_) {}
    await sleep(step);
  }
  return false;
}

async function build() {
  const r = await start("build");
  if (r) { selectLog(r.id); toast("Compilare pornită…"); }
  return r;
}

async function resetData() {
  if (S.procs.some((p) => p.status === "running")) { toast("Oprește întâi toate procesele.", true); return; }
  if (await api("/api/reset-data", {}, "Datele au fost șterse (folderul data/)")) {
    S.evSince = 0; $("evBody").innerHTML = "";
  }
}

// ───────────────────────── formulare ─────────────────────────

function startProducer() {
  return startProducerWith({
    count: $("pCount").value, accounts: $("pAcc").value, amount: $("pAmt").value,
    poison_every: $("pPoison").value, invalid: $("pInvalid").checked ? 1 : "",
  });
}

async function startProducerWith(o) {
  if (!isRunning("broker")) { toast("Pornește întâi brokerul.", true); return null; }
  const r = await start("producer", o);
  if (r) selectLog(r.id);
  return r;
}

function startConsumer() {
  const o = { name: $("cName").value, lease: $("cLease").value, crash: $("cCrash").value, fail_rate: $("cFail").value };
  if (S.lab === "lab2") { o.effect = $("cEffect").value; o.api = $("cApi").value; }
  if ($("cNaive").checked) { o.naive = 1; if (S.lab === "lab2") o.effect = "local"; }
  return startConsumerWith(o);
}

async function startConsumerWith(o) {
  if (!isRunning("broker")) { toast("Pornește întâi brokerul.", true); return null; }
  if (S.lab === "lab2" && (o.effect || "remote") === "remote" && !isRunning("accounts")) toast("Atenție: serviciul de conturi nu rulează — consumer-ul va aștepta (fallback).");
  const r = await start("consumer", o);
  if (r) selectLog(r.id);
  return r;
}

async function ensureBroker(lease = 5) {
  if (!isRunning("broker")) {
    const r = await start("broker", { lease });
    if (!r) return false;
  }
  const up = await waitUntil(async () => (await call("GET", "/api/broker/healthz")).ok, 8000);
  if (!up) toast("Brokerul nu răspunde.", true);
  return up;
}

async function startAccounts(o = {}) {
  if (S.lab !== "lab2") return false;
  if (!isRunning("accounts")) {
    const r = await start("accounts", o);
    if (!r) return false;
  }
  const up = await waitUntil(async () => (await call("GET", "/api/accounts/admin/state")).ok, 8000);
  if (!up) toast("Serviciul de conturi nu răspunde.", true);
  return up;
}

async function restartAccounts(o) {
  await stopKind("accounts");
  await sleep(400);
  if (await startAccounts(o)) toast(o.disable_v2 ? "Server VECHI pornit: cunoaște doar accounts.v1" : "Server nou pornit: accounts.v1 + accounts.v2");
}

async function ensureLab2Services() {
  return (await ensureBroker(4)) && (await startAccounts());
}

async function setFaults(f) {
  const body = {
    drop_response_next: +(f.drop_response_next || 0), drop_response_rate: +(f.drop_response_rate || 0),
    slow_response_ms: +(f.slow_response_ms || 0), unavailable_next: +(f.unavailable_next || 0),
  };
  const r = await call("PUT", "/api/accounts/admin/faults", body);
  if (!r.ok) { toast(r.data.error || "serviciul nu rulează", true); return false; }
  toast("Defecte aplicate: " + Object.entries(body).filter(([, v]) => v).map(([k, v]) => k + "=" + v).join(", ") || "fără defecte");
  return true;
}

function applyFaultsForm() {
  return setFaults({
    drop_response_next: $("fDropNext").value, drop_response_rate: $("fDropRate").value,
    slow_response_ms: $("fSlow").value, unavailable_next: $("fUnav").value,
  });
}

async function setDropAck() {
  const p = +$("dropAck").value / 100;
  const r = await call("PUT", "/api/broker/v1/admin/faults", { drop_publish_ack: p });
  toast(r.ok ? `Brokerul pierde ${Math.round(p * 100)}% din ACK-urile către producer` : "Brokerul nu rulează", !r.ok);
}

async function runDemo() {
  const r = await start("demo", { scenario: $("demoScenario").value });
  if (r) { selectLog(r.id); toast("Demo pornit — rezultatul apare în jurnal"); }
}

// ───────────────────────── scenarii automate ─────────────────────────

function setResult(id, html) { $(id).innerHTML = html; }
const step = (id, text) => setResult(id, `<div class="verdict info">⏳ ${text}</div>`);

async function freshStart(resultId) {
  step(resultId, "Opresc procesele și șterg datele vechi…");
  await call("POST", "/api/stopall");
  await sleep(300);
  await call("POST", "/api/reset-data");
  S.evSince = 0; $("evBody").innerHTML = "";
  S.lines.clear(); S.allLines = []; S.logSel = "all";
  await refreshState();
  if (!S.info.labs.find((l) => l.id === S.lab).built && !(await refreshBuilt())) {
    step(resultId, "Compilez laboratorul (prima dată)…");
    const b = await build();
    if (!b) return false;
    const p = await waitExit(b.id, 180000);
    if (p.exit_code !== 0) { setResult(resultId, '<div class="verdict bad">Compilarea a eșuat — vezi jurnalul.</div>'); return false; }
  }
  return true;
}

async function refreshBuilt() {
  const r = await call("GET", "/api/state?since=" + S.since);
  return r.data.built;
}

async function lab1Critical() {
  const naive = document.querySelector("input[name=l1mode]:checked").value === "naiv";
  const name = naive ? "c-naiv" : "c-idem";
  const R = "l1Result";
  if (!(await freshStart(R))) return;
  step(R, "Pornesc brokerul (lease 3 secunde)…");
  if (!(await ensureBroker(3))) return;
  step(R, "Producer-ul trimite 5 plăți de 100 pe ACC-1…");
  const p = await startProducerWith({ count: 5, accounts: 1, amount: 100 });
  if (!p) return;
  await waitExit(p.id);
  step(R, `Pornesc consumer-ul ${naive ? "NAIV" : "IDEMPOTENT"}, care cade după efectul mesajului #3…`);
  const c = await startConsumerWith({ name, crash: 3, naive: naive ? 1 : "", lease: 3 });
  if (!c) return;
  const ended = await waitExit(c.id, 15000);
  step(R, `Consumer-ul a căzut (cod ${ended.exit_code}) fără ACK. Aștept 3,5 s să expire lease-ul, apoi îl repornesc…`);
  await sleep(3500);
  const c2 = await startConsumerWith({ name, naive: naive ? 1 : "", lease: 3 });
  if (!c2) return;
  await waitUntil(async () => S.broker && S.broker.counters.acked >= 5, 10000);
  await sleep(500);
  await api("/api/stop", { id: c2.id });
  const l = await call("GET", "/api/ledger?name=" + name);
  const s = ledgerSummary(l.data);
  const good = s.total === 500;
  setResult(R, `<div class="verdict ${good ? "good" : "bad"}">${naive ? "NAIV" : "IDEMPOTENT"}: sold ACC-1 = <b>${s.total}</b> (corect 500) · efecte duplicate: <b>${s.dup}</b> · relivrări: ${S.broker ? S.broker.counters.redelivered : "?"}
    — ${good ? "✓ corect: mesajul relivrat a fost recunoscut în inbox" : "✗ GREȘIT: mesajul relivrat a pus banii a doua oară"}</div>`);
}

async function lab1Ordering() {
  if (!(await ensureBroker(5))) return;
  const p = await startProducerWith({ count: 24, accounts: 4, amount: 1 });
  if (!p) return;
  await waitExit(p.id);
  for (const n of ["o1", "o2", "o3"]) {
    if (!isRunning("consumer", n)) await startConsumerWith({ name: n, fail_rate: 0.2, batch: 2, lease: 5 });
  }
  toast("3 consumeri pornesc în paralel — urmărește registrele (încălcări de ordine = 0)");
}

async function lab2Critical() {
  const R = "l2Result";
  if (!(await freshStart(R))) return;
  step(R, "Pornesc brokerul și serviciul de conturi…");
  if (!(await ensureLab2Services())) return;
  step(R, "Configurez serviciul să PIARDĂ următorul răspuns (după ce execută operația)…");
  if (!(await setFaults({ drop_response_next: 1 }))) return;
  step(R, "Producer-ul trimite o plată de 100 pe ACC-1…");
  const p = await startProducerWith({ count: 1, accounts: 1, amount: 100 });
  if (!p) return;
  await waitExit(p.id);
  step(R, "Pornesc consumer-ul, care apelează serviciul prin gRPC…");
  const c = await startConsumerWith({ name: "c1", effect: "remote" });
  if (!c) return;
  await waitUntil(async () => S.broker && S.broker.counters.acked >= 1, 15000);
  await sleep(600);
  await api("/api/stop", { id: c.id });
  const st = (await call("GET", "/api/accounts/admin/state")).data;
  const bal = (st.balances || {})["ACC-1"] || 0;
  const s = st.stats || {};
  const good = bal === 100 && s.executed === 1;
  setResult(R, `<div class="verdict ${good ? "good" : "bad"}">sold ACC-1 = <b>${bal}</b> (corect 100) · executată: <b>${s.executed}</b> dată ·
    răspunsuri pierdute: <b>${s.responses_dropped}</b> · reluată din registru: <b>${s.replayed}</b><br>
    ${good ? "✓ Răspunsul s-a pierdut, clientul a reîncercat cu aceeași cheie, iar serviciul a răspuns „deja făcut” fără să mai pună banii." : "✗ rezultat neașteptat — vezi jurnalul consumer-ului"}</div>`);
}

// ───────────────────────── consumer manual (lab 1) ─────────────────────────

async function manualReceive() {
  if (!isRunning("broker")) { toast("Pornește întâi brokerul.", true); return; }
  const r = await call("POST", "/api/broker/v1/queues/plati/receive", { consumer: "manual", max: 1, lease_ms: +$("mLease").value, wait_ms: 0 });
  const ds = (r.data && r.data.deliveries) || [];
  if (!ds.length) { toast("Nimic disponibil (coadă goală, backoff sau cheie blocată de ordonare)"); return; }
  ds.forEach((d) => S.held.set(d.delivery_id, d));
  renderManual();
}

async function settle(did, action) {
  const d = S.held.get(did);
  if (!d) return;
  let r;
  if (action === "ack") r = await call("POST", "/api/broker/v1/queues/plati/ack", { delivery_id: did });
  else r = await call("POST", "/api/broker/v1/queues/plati/nack", { delivery_id: did, retry: action === "retry", reason: action === "retry" ? "manual: eroare tranzitorie" : "manual: mesaj otrăvit" });
  toast(r.status === 409 ? "409: livrarea a expirat — brokerul respinge confirmarea (mesajul e relivrat)" : `${action.toUpperCase()}: ${r.data.status || r.data.outcome || r.data.error}`, r.status >= 400);
  S.held.delete(did);
  renderManual();
}

function renderManual() {
  const now = Date.now();
  $("manual").innerHTML = [...S.held.values()].map((d) => {
    const left = Math.max(0, new Date(d.lease_until) - now);
    return `<div class="manual-d"><div class="meta">${esc(d.message.id.slice(0, 8))} · cheie <b>${esc(d.message.key)}</b> · seq ${d.message.seq} · <b>attempt ${d.attempt}</b> · ${esc(JSON.stringify(d.message.payload))}</div>
      <div class="meta">lease: ${left ? (left / 1000).toFixed(1) + " s rămase" : "<b style='color:var(--bad)'>EXPIRAT</b> — confirmarea va fi respinsă"}</div>
      <div class="row gap"><button class="small ok" onclick="settle('${d.delivery_id}','ack')">ACK</button>
      <button class="small warn" onclick="settle('${d.delivery_id}','retry')">NACK + retry</button>
      <button class="small bad" onclick="settle('${d.delivery_id}','dlq')">NACK → DLQ</button>
      <button class="small ghost" onclick="S.held.delete('${d.delivery_id}');renderManual()">Abandonează (cădere)</button></div></div>`;
  }).join("");
}

// ───────────────────────── jurnale ─────────────────────────

function selectLog(id) { S.logSel = id; renderLogTabs(); renderLog(true); }

function renderLogTabs() {
  const tabs = [["all", "toate"]].concat([...S.procs].reverse().slice(0, 14).map((p) => [p.id, p.name]));
  $("logTabs").innerHTML = tabs.map(([id, n]) => `<button class="${String(S.logSel) === String(id) ? "active" : ""}" onclick="selectLog(${id === "all" ? "'all'" : id})">${esc(n)}</button>`).join("");
}

function lineClass(t) {
  if (/^──/.test(t)) return /CAZUT|eroare|codul [1-9]/.test(t) ? "l-bad" : "l-sys";
  if (/💥|☠|✗|GRESIT|DUBLAT|FAIL|panic|error:/i.test(t)) return "l-bad";
  if (/≡|DUPLICAT|reluat|replay|duplicate/i.test(t)) return "l-dup";
  if (/↻|⚠|ambiguu|⏸|retry|UNKNOWN|NOT_APPLIED/i.test(t)) return "l-warn";
  if (/✓|✚|▶|corect|APPLIED|ok\b/.test(t)) return "l-ok";
  return "";
}

function renderLog(force) {
  const v = $("logView");
  let rows = [];
  if (S.logSel === "all") {
    rows = S.allLines.slice(-1500).map(([pid, t]) => {
      const p = procById(pid);
      return `<span class="l-src">[${esc(p ? p.name : pid)}]</span> <span class="${lineClass(t)}">${esc(t)}</span>`;
    });
  } else {
    rows = (S.lines.get(S.logSel) || []).slice(-2000).map((t) => `<span class="${lineClass(t)}">${esc(t)}</span>`);
  }
  const atBottom = v.scrollTop + v.clientHeight >= v.scrollHeight - 30;
  v.innerHTML = rows.join("\n") || '<span class="l-src">(fără ieșire încă)</span>';
  if ($("autoScroll").checked && (atBottom || force)) v.scrollTop = v.scrollHeight;
}
S.allLines = [];

// ───────────────────────── stare live ─────────────────────────

async function refreshState() {
  const r = await call("GET", "/api/state?since=" + S.since);
  if (!r.ok) return;
  const d = r.data;
  const before = S.procs.length;
  S.procs = d.procs;
  let changed = d.lines.length > 0;
  for (const l of d.lines) {
    if (!S.lines.has(l.pid)) S.lines.set(l.pid, []);
    S.lines.get(l.pid).push(l.text);
    S.allLines.push([l.pid, l.text]);
  }
  if (S.allLines.length > 4000) S.allLines.splice(0, S.allLines.length - 4000);
  S.since = d.next;
  renderProcs();
  if (before !== S.procs.length) renderLogTabs();
  if (changed) renderLog(false);
  updateBanner(d.built);
  $("chipBroker").className = "chip" + (isRunning("broker") ? " on" : "");
  $("chipAccounts").className = "chip lab2-only" + (isRunning("accounts") ? " on" : "");
}

async function refreshBroker() {
  if (!isRunning("broker")) { S.broker = null; $("brokerBox").innerHTML = '<p class="muted">Brokerul nu rulează. <button class="small" onclick="ensureBroker(5)">Pornește</button></p>'; $("dlqBox").innerHTML = '<p class="muted">Gol.</p>'; return; }
  const r = await call("GET", "/api/broker/v1/queues");
  if (!r.ok) return;
  const q = (r.data.queues || []).find((x) => x.name === "plati");
  const c = r.data.config || {};
  if (!q) { S.broker = null; $("brokerBox").innerHTML = `<p class="muted">Brokerul rulează (lease ${c.default_lease_ms} ms, max ${c.max_attempts} tentative). Coada apare la primul mesaj.</p>`; }
  else {
    S.broker = q;
    const k = q.counters;
    const tot = Math.max(1, q.ready + q.delayed + q.in_flight + q.dead);
    const seg = (n, col) => (n ? `<span style="width:${(100 * n) / tot}%;background:${col}"></span>` : "");
    $("brokerBox").innerHTML = `
      <div class="bar">${seg(q.ready, "#2c5d8a")}${seg(q.delayed, "#e0b25a")}${seg(q.in_flight, "#24804f")}${seg(q.dead, "#c0392b")}</div>
      <div class="legend"><span><i style="background:#2c5d8a"></i>gata ${q.ready}</span><span><i style="background:#e0b25a"></i>în backoff ${q.delayed}</span>
        <span><i style="background:#24804f"></i>în zbor ${q.in_flight}</span><span><i style="background:#c0392b"></i>DLQ ${q.dead}</span></div>
      <div class="kv">
        <div><b>${k.published}</b><span>publicate</span></div><div class="hl"><b>${k.duplicates}</b><span>duplicate filtrate</span></div><div><b>${k.acked}</b><span>ACK</span></div>
        <div><b>${k.delivered}</b><span>livrări</span></div><div class="hl"><b>${k.redelivered}</b><span>relivrări</span></div><div class="hl"><b>${k.lease_expired}</b><span>lease expirat</span></div>
        <div><b>${k.nacked}</b><span>NACK</span></div><div><b>${k.dead_lettered}</b><span>în DLQ</span></div><div><b>${k.rejected}</b><span>respinse</span></div>
      </div>
      <div class="muted" style="font-size:12px">lease ${c.default_lease_ms} ms · max ${c.max_attempts} tentative · backoff ${c.backoff_base_ms}–${c.backoff_max_ms} ms · persistent: ${c.persistent}</div>`;
  }
  // DLQ
  if (q && q.dead) {
    const d = await call("GET", "/api/broker/v1/queues/plati/dlq");
    const items = d.data.dead_letters || [];
    $("dlqBox").innerHTML = `<table class="tbl"><thead><tr><th>mesaj</th><th>cheie/seq</th><th>tent.</th><th>motiv</th><th></th></tr></thead><tbody>${items.map((x) => `<tr><td>${esc(x.message.id.slice(0, 8))}</td><td>${esc(x.message.key)}/${x.message.seq}</td><td>${x.attempts}</td><td>${esc(x.reason)}</td><td><button class="small" onclick="replay('${esc(x.message.id)}')">replay</button></td></tr>`).join("")}</tbody></table>`;
  } else $("dlqBox").innerHTML = '<p class="muted">Gol.</p>';
  // evenimente
  const ev = await call("GET", "/api/broker/v1/events?since=" + S.evSince);
  const evs = (ev.data && ev.data.events) || [];
  if (evs.length) {
    const rows = evs.map((e) => {
      S.evSince = Math.max(S.evSince, e.n);
      const cls = /dead|rejected|stale/.test(e.kind) ? "b-bad" : /redeliver|lease|retry/.test(e.kind) ? "b-warn" : /duplicate|replay|recover/.test(e.kind) ? "b-v" : /acked/.test(e.kind) ? "b-ok" : "b-blue";
      const t = new Date(e.at);
      return `<tr><td>${t.toLocaleTimeString("ro-RO", { hour12: false })}.${String(t.getMilliseconds()).padStart(3, "0")}</td><td><span class="badge ${cls}">${e.kind}</span></td><td>${esc((e.msg_id || "").slice(0, 8))} ${esc(e.key || "")}</td><td>${esc(e.detail)}</td></tr>`;
    });
    $("evBody").insertAdjacentHTML("afterbegin", rows.reverse().join(""));
    while ($("evBody").children.length > 250) $("evBody").lastChild.remove();
  }
}

async function replay(id) {
  const r = await call("POST", "/api/broker/v1/queues/plati/dlq/" + encodeURIComponent(id) + "/replay");
  toast(r.ok ? "Mesajul a fost readus din DLQ în coadă" : "Replay eșuat", !r.ok);
}

async function refreshAccounts() {
  if (S.lab !== "lab2") return;
  if (!isRunning("accounts")) { S.accounts = null; $("accBox").innerHTML = '<p class="muted">Serviciul nu rulează. <button class="small" onclick="startAccounts()">Pornește</button></p>'; return; }
  const r = await call("GET", "/api/accounts/admin/state");
  if (!r.ok) return;
  const s = r.data;
  S.accounts = s;
  const st = s.stats;
  const f = s.faults;
  const bal = Object.entries(s.balances || {}).sort();
  const faults = [f.drop_response_next && `pierde următoarele ${f.drop_response_next} răspunsuri`, f.drop_response_rate && `pierde ${Math.round(f.drop_response_rate * 100)}% din răspunsuri`,
    f.slow_response_ms && `întârziere ${f.slow_response_ms} ms`, f.unavailable_next && `refuză următoarele ${f.unavailable_next}`].filter(Boolean);
  $("accBox").innerHTML = `
    <div class="muted" style="font-size:12px;margin-bottom:6px">${s.v2_enabled ? "servește accounts.v1 + accounts.v2" : "<b style='color:var(--warn)'>server VECHI: doar accounts.v1</b>"} · defecte: ${faults.length ? `<b style="color:var(--bad)">${faults.join(", ")}</b>` : "niciunul"}</div>
    <div class="kv">
      <div><b>${st.executed}</b><span>executate</span></div><div class="hl"><b>${st.replayed}</b><span>reluate din registru</span></div><div class="hl"><b>${st.responses_dropped}</b><span>răspunsuri pierdute</span></div>
      <div><b>${st.rejected}</b><span>respinse</span></div><div><b>${st.cancelled_before_commit}</b><span>anulate înainte de commit</span></div><div><b>${st.unavailable_injected}</b><span>refuzuri injectate</span></div>
    </div>
    <table class="tbl"><thead><tr><th>cont</th><th style="text-align:right">sold</th></tr></thead><tbody>${bal.map(([k, v]) => `<tr><td>${esc(k)}</td><td class="n">${v}</td></tr>`).join("") || "<tr><td colspan=2 class='muted'>niciun cont</td></tr>"}</tbody></table>
    <table class="tbl" style="margin-top:8px"><thead><tr><th>operație</th><th>cont</th><th>sumă</th><th>stare</th><th>api</th></tr></thead><tbody>
      ${(s.operations || []).slice(0, 10).map((o) => `<tr><td>${esc(o.id)}</td><td>${esc(o.account)}</td><td class="n">${o.amount}</td><td><span class="badge ${o.status === "SUCCEEDED" ? "b-ok" : "b-bad"}">${o.status}</span></td><td><span class="badge ${o.api_version === "v1" ? "b-warn" : "b-v"}">${esc(o.api_version)}</span></td></tr>`).join("")}
    </tbody></table>`;
}

function ledgerSummary(l) {
  const s = { total: 0, msgs: 0, exec: 0, dup: 0, inbox: 0, order: 0 };
  if (!l || !l.balances) return s;
  for (const v of Object.values(l.balances)) s.total += v;
  for (const n of Object.values(l.executions || {})) { s.msgs++; s.exec += n; s.dup += n - 1; }
  s.inbox = Object.keys(l.inbox || {}).length;
  s.order = l.order_violations || 0;
  s.balances = l.balances;
  return s;
}

async function refreshLedgers() {
  const names = [...new Set(S.procs.filter((p) => p.kind === "consumer" && !/-effect remote/.test(p.cmdline)).map((p) => p.name))];
  if (!names.length) { $("ledgerBox").innerHTML = '<p class="muted">Niciun registru încă (apar pentru consumerii cu efect local).</p>'; return; }
  const parts = [];
  for (const n of names) {
    const r = await call("GET", "/api/ledger?name=" + encodeURIComponent(n));
    if (!r.ok) { parts.push(`<div class="ledger"><div class="lh"><span>${esc(n)}</span><span class="muted">gol</span></div></div>`); continue; }
    const s = ledgerSummary(r.data);
    const naive = S.procs.some((p) => p.name === n && /-naive/.test(p.cmdline));
    parts.push(`<div class="ledger"><div class="lh"><span>${esc(n)} <span class="badge ${naive ? "b-warn" : "b-v"}">${naive ? "naiv" : "idempotent"}</span></span><span>total ${s.total}</span></div>
      <div class="kv"><div><b>${s.msgs}</b><span>mesaje</span></div><div class="${s.dup ? "hl" : ""}"><b style="${s.dup ? "color:var(--bad)" : ""}">${s.dup}</b><span>efecte DUPLICATE</span></div><div><b>${s.order}</b><span>încălcări ordine</span></div></div>
      <div class="muted" style="font-size:12px">${Object.entries(s.balances).sort().map(([k, v]) => `${esc(k)}=${v}`).join(" · ")} · inbox ${s.inbox}</div></div>`);
  }
  $("ledgerBox").innerHTML = parts.join("");
}

async function loop() {
  let tick = 0;
  for (;;) {
    try {
      await refreshState();
      if (tick % 2 === 0) { await refreshBroker(); await refreshAccounts(); }
      if (tick % 3 === 0) await refreshLedgers();
      if (S.held.size) renderManual();
    } catch (e) { console.error(e); }
    tick++;
    await sleep(500);
  }
}

window.addEventListener("load", init);
