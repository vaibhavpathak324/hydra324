const $ = (id) => document.getElementById(id);

const state = {
  phase: "logged_out",
  chats: [],
  selectedChat: null,
  people: [],
  selected: new Set(),
  armed: 0,
};

function show(el, on) {
  el.classList.toggle("hidden", !on);
}

function loginError(text) {
  const n = $("login-err");
  n.hidden = !text;
  n.textContent = text || "";
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    let d = data.detail;
    if (Array.isArray(d)) d = d.map((x) => x.msg || JSON.stringify(x)).join("; ");
    throw new Error(d || res.statusText || "Request failed");
  }
  return data;
}

function applyStatus(s) {
  if (!s) return;
  state.phase = s.phase;
  state.armed = s.armed || 0;
  $("armed-n").textContent = String(state.armed);
  $("btn-fire").disabled = state.armed < 1;
  if (s.me) {
    $("me-name").textContent = s.me.name || "session";
    $("me-user").textContent = s.me.username ? "@" + s.me.username : s.me.phone || "";
  }
  const ready = s.phase === "ready";
  show($("login"), !ready);
  show($("app"), ready);
  if (s.job) renderJob(s.job);
}

function setBusy(on) {
  ["btn-arm", "btn-fire", "btn-send", "btn-disarm"].forEach((id) => {
    const n = $(id);
    if (id === "btn-fire") n.disabled = on || state.armed < 1;
    else n.disabled = on;
  });
}

function renderJob(job) {
  const box = $("progress");
  if (!job) {
    show(box, false);
    setBusy(false);
    return;
  }
  show(box, true);
  setBusy(job.status === "running");
  const pct = job.total ? Math.round((job.done / job.total) * 100) : 0;
  $("bar-fill").style.width = pct + "%";
  $("progress-text").textContent =
    `${job.kind}  ${job.done}/${job.total}  ok ${job.ok}  fail ${job.fail}  ${job.detail || job.status}`;
}

function addLog(entry) {
  const ol = $("log-list");
  const li = document.createElement("li");
  li.className = entry.level || "";
  const t = (entry.ts || "").slice(11, 19);
  li.textContent = `${t}  ${entry.text}`;
  ol.prepend(li);
}

function connectWs() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/ws`);
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.event === "status") applyStatus(msg);
    if (msg.event === "job") renderJob(msg.job);
    if (msg.event === "log") addLog(msg.log);
    if (msg.event === "chats" && msg.chats) {
      state.chats = msg.chats;
      renderChats();
    }
  };
  ws.onclose = () => setTimeout(connectWs, 1500);
}

function kindLabel(c) {
  return c.kind === "channel" ? "channel" : c.kind === "supergroup" ? "supergroup" : "group";
}

function renderChats() {
  const q = $("chat-filter").value.trim().toLowerCase();
  const ul = $("chat-list");
  ul.innerHTML = "";
  const rows = state.chats.filter((c) => !q || (c.title || "").toLowerCase().includes(q));
  if (!rows.length) {
    ul.innerHTML = `<li class="dim">No groups or channels.</li>`;
    return;
  }
  for (const c of rows) {
    const li = document.createElement("li");
    if (state.selectedChat && state.selectedChat.id === c.id) li.classList.add("on");
    const pending =
      c.pending == null ? "" : `<span class="badge">${c.pending} pending</span>`;
    li.innerHTML = `
      <span class="title"></span>
      <span class="meta"><span>${kindLabel(c)}</span>${c.admin ? "<span>admin</span>" : ""}<span>${pending}</span></span>
    `;
    li.querySelector(".title").textContent = c.title;
    li.addEventListener("click", () => selectChat(c));
    ul.appendChild(li);
  }
}

async function loadChats() {
  const data = await api("/api/chats");
  state.chats = data.chats || [];
  renderChats();
}

async function scanChats() {
  const data = await api("/api/chats/scan", { method: "POST", body: { chats: state.chats } });
  state.chats = data.chats || [];
  renderChats();
}

async function selectChat(chat) {
  state.selectedChat = chat;
  state.selected = new Set();
  renderChats();
  $("people-title").textContent = chat.title;
  $("people-sub").textContent =
    "Pending join requests. HYDRA does not accept or decline — it only DMs the people on this list.";
  $("people-body").innerHTML = `<tr class="empty"><td colspan="5">Loading…</td></tr>`;
  try {
    const data = await api("/api/requests?chat_id=" + encodeURIComponent(chat.id));
    state.people = data.people || [];
    state.selected = new Set(state.people.map((p) => p.id));
    renderPeople();
  } catch (err) {
    $("people-body").innerHTML = `<tr class="empty"><td colspan="5">${err.message}</td></tr>`;
  }
}

function renderPeople() {
  const q = $("people-filter").value.trim().toLowerCase();
  const body = $("people-body");
  const rows = state.people.filter((p) => {
    const blob = `${p.name} ${p.username || ""} ${p.about || ""}`.toLowerCase();
    return !q || blob.includes(q);
  });
  $("sel-count").textContent = `${state.selected.size} selected / ${state.people.length}`;
  $("sel-all").checked = state.people.length > 0 && state.selected.size === state.people.length;
  if (!rows.length) {
    body.innerHTML = `<tr class="empty"><td colspan="5">No pending join requests.</td></tr>`;
    return;
  }
  body.innerHTML = "";
  for (const p of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="checkbox" /></td>
      <td></td>
      <td class="uname"></td>
      <td class="uname"></td>
      <td class="dim"></td>
    `;
    const cb = tr.querySelector("input");
    cb.checked = state.selected.has(p.id);
    cb.addEventListener("change", () => {
      if (cb.checked) state.selected.add(p.id);
      else state.selected.delete(p.id);
      $("sel-count").textContent = `${state.selected.size} selected / ${state.people.length}`;
    });
    tr.children[1].textContent = p.name;
    tr.children[2].textContent = p.username ? "@" + p.username : "—";
    tr.children[3].textContent = p.date || "";
    tr.children[4].textContent = p.about || "";
    body.appendChild(tr);
  }
}

function selectedPeople() {
  return state.people.filter((p) => state.selected.has(p.id));
}

function ask(text) {
  return new Promise((resolve) => {
    const d = $("confirm");
    $("confirm-text").textContent = text;
    const yes = $("confirm-yes");
    const no = $("confirm-no");
    const done = (v) => {
      d.close();
      yes.onclick = no.onclick = null;
      resolve(v);
    };
    yes.onclick = () => done(true);
    no.onclick = () => done(false);
    d.showModal();
  });
}

function payload() {
  if (!state.selectedChat) throw new Error("Select a group or channel first.");
  const message = $("message").value;
  const people = selectedPeople();
  if (!people.length) throw new Error("Select at least one requester.");
  if (!message.trim()) throw new Error("Write the DM first.");
  return { chat_id: state.selectedChat.id, message, people };
}

$("form-start").addEventListener("submit", async (e) => {
  e.preventDefault();
  loginError("");
  try {
    const data = await api("/api/auth/start", {
      method: "POST",
      body: {
        api_id: Number($("api_id").value),
        api_hash: $("api_hash").value.trim(),
        phone: $("phone").value.trim(),
      },
    });
    show($("form-start"), false);
    show($("form-code"), data.phase === "awaiting_code");
    show($("form-pass"), data.phase === "awaiting_password");
  } catch (err) {
    loginError(err.message);
  }
});

$("form-code").addEventListener("submit", async (e) => {
  e.preventDefault();
  loginError("");
  try {
    const data = await api("/api/auth/code", { method: "POST", body: { code: $("code").value.trim() } });
    if (data.phase === "awaiting_password") {
      show($("form-code"), false);
      show($("form-pass"), true);
      return;
    }
    applyStatus(data);
    await bootApp();
  } catch (err) {
    loginError(err.message);
  }
});

$("form-pass").addEventListener("submit", async (e) => {
  e.preventDefault();
  loginError("");
  try {
    const data = await api("/api/auth/password", {
      method: "POST",
      body: { password: $("password").value },
    });
    applyStatus(data);
    await bootApp();
  } catch (err) {
    loginError(err.message);
  }
});

$("form-string").addEventListener("submit", async (e) => {
  e.preventDefault();
  loginError("");
  try {
    const data = await api("/api/auth/string", {
      method: "POST",
      body: {
        api_id: Number($("s_api_id").value),
        api_hash: $("s_api_hash").value.trim(),
        session_string: $("s_string").value.trim(),
      },
    });
    applyStatus(data);
    await bootApp();
  } catch (err) {
    loginError(err.message);
  }
});

$("btn-refresh").addEventListener("click", () => loadChats().catch((e) => alert(e.message)));
$("btn-scan").addEventListener("click", () => scanChats().catch((e) => alert(e.message)));
$("chat-filter").addEventListener("input", renderChats);
$("people-filter").addEventListener("input", renderPeople);
$("sel-all").addEventListener("change", () => {
  if ($("sel-all").checked) state.selected = new Set(state.people.map((p) => p.id));
  else state.selected = new Set();
  renderPeople();
});

$("btn-arm").addEventListener("click", async () => {
  try {
    const body = payload();
    const ok = await ask(
      `Write this message as an unsent draft in ${body.people.length} requester DMs? Nothing will be sent until you hit Send all drafts.`
    );
    if (!ok) return;
    const res = await api("/api/arm", { method: "POST", body });
    state.armed = res.armed || 0;
    $("armed-n").textContent = String(state.armed);
    $("btn-fire").disabled = state.armed < 1;
  } catch (err) {
    alert(err.message);
  }
});

$("btn-fire").addEventListener("click", async () => {
  try {
    const ok = await ask(
      `Send every armed draft now (${state.armed})? This is the one-click release.`
    );
    if (!ok) return;
    const res = await api("/api/fire", { method: "POST" });
    state.armed = res.armed || 0;
    $("armed-n").textContent = String(state.armed);
    $("btn-fire").disabled = state.armed < 1;
  } catch (err) {
    alert(err.message);
  }
});

$("btn-send").addEventListener("click", async () => {
  try {
    const body = payload();
    const ok = await ask(`Send the DM immediately to ${body.people.length} requesters?`);
    if (!ok) return;
    await api("/api/send", { method: "POST", body });
  } catch (err) {
    alert(err.message);
  }
});

$("btn-disarm").addEventListener("click", async () => {
  try {
    const res = await api("/api/disarm", { method: "POST" });
    state.armed = res.armed || 0;
    $("armed-n").textContent = String(state.armed);
    $("btn-fire").disabled = state.armed < 1;
  } catch (err) {
    alert(err.message);
  }
});

$("btn-logout").addEventListener("click", async () => {
  await api("/api/auth/logout", { method: "POST" });
  location.reload();
});

$("btn-export").addEventListener("click", async () => {
  try {
    const data = await api("/api/session-string");
    await navigator.clipboard.writeText(data.session_string);
    alert("Session string copied to clipboard. Treat it like a password.");
  } catch (err) {
    alert(err.message);
  }
});

async function bootApp() {
  show($("login"), false);
  show($("app"), true);
  const logs = await api("/api/logs");
  (logs.logs || []).forEach(addLog);
  await loadChats();
}

function applyBot(b) {
  if (!b) return;
  const bar = $("bot-bar");
  const status = $("bot-status");
  if (b.running && b.username) {
    bar.classList.add("live");
    status.innerHTML = `Live as <a href="https://t.me/${b.username}" target="_blank" rel="noreferrer">@${b.username}</a> — open it and tap the buttons. First private chat becomes the owner.`;
  } else {
    bar.classList.remove("live");
    status.textContent = "Token starts the Telegram button panel. Session login still needed for join requests.";
  }
}

$("form-bot").addEventListener("submit", async (e) => {
  e.preventDefault();
  try {
    const data = await api("/api/bot/start", {
      method: "POST",
      body: { token: $("bot_token").value.trim() },
    });
    applyBot(data);
  } catch (err) {
    $("bot-status").textContent = err.message;
  }
});

async function boot() {
  connectWs();
  const s = await api("/api/status");
  applyStatus(s);
  applyBot(s.bot || (await api("/api/bot")));
  if (s.phase === "ready") await bootApp();
}

boot().catch((err) => loginError(err.message));
