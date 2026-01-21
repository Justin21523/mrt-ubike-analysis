async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  return await res.json();
}

function fmtAge(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s)) return "—";
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  if (s < 86400) return `${(s / 3600).toFixed(1)}h`;
  return `${(s / 86400).toFixed(1)}d`;
}

function shortId(x, n = 8) {
  if (!x) return "—";
  const s = String(x);
  return s.length <= n ? s : s.slice(0, n);
}

function setStatusText(text) {
  const el = document.getElementById("statusText");
  if (el) el.textContent = text || "";
}

function downloadJson(filename, payload) {
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function copyText(text) {
  await navigator.clipboard.writeText(text);
}

function setModePill(demoMode) {
  const el = document.getElementById("modePill");
  if (!el) return;
  el.textContent = demoMode ? "Demo mode" : "Real data mode";
  el.classList.remove("ok", "warn", "bad");
  el.classList.add(demoMode ? "warn" : "ok");
}

function setWeatherPill(metaPayload) {
  const el = document.getElementById("weatherPill");
  if (!el) return;
  el.classList.remove("ok", "warn", "bad");
  const w = metaPayload?.meta?.external?.weather_collector ?? null;
  if (!w) {
    el.textContent = "Weather: unavailable";
    el.classList.add("warn");
    return;
  }
  el.textContent = `Weather: ${w.stale ? "stale" : "ok"} (${fmtAge(w.heartbeat_age_s)})${w.is_rainy_now ? " · rain" : ""}`;
  el.classList.add(w.stale ? "warn" : "ok");
}

function explorerHref(params) {
  const items = [];
  for (const [k, v] of Object.entries(params || {})) {
    if (v == null || v === "") continue;
    items.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
  }
  const hash = items.length ? `#${items.join("&")}` : "";
  return `/explorer${hash}`;
}

function createCard({ tone = "support", kicker, title, badge, right, actions } = {}) {
  const card = document.createElement("section");
  card.className = `card ${tone}`;

  const header = document.createElement("div");
  header.className = "card-header";

  const left = document.createElement("div");
  left.className = "card-head-left";

  const kick = document.createElement("div");
  kick.className = "card-kicker";
  kick.textContent = kicker || "";

  const t = document.createElement("div");
  t.className = "card-title";
  t.textContent = title || "";

  left.appendChild(kick);
  left.appendChild(t);

  const headRight = document.createElement("div");
  headRight.className = "card-head-right";

  if (badge) {
    const b = document.createElement("span");
    b.className = `badge ${badge.tone || ""}`;
    b.textContent = badge.text || "";
    headRight.appendChild(b);
  }

  if (right) {
    const r = document.createElement("div");
    r.className = "card-right";
    r.innerHTML = right;
    headRight.appendChild(r);
  }

  header.appendChild(left);
  header.appendChild(headRight);

  const body = document.createElement("div");
  body.className = "card-body";

  card.appendChild(header);
  card.appendChild(body);

  if (Array.isArray(actions) && actions.length) {
    const row = document.createElement("div");
    row.className = "row row-actions card-actions";
    for (const a of actions) {
      if (a.type === "link") {
        const el = document.createElement("a");
        el.className = `btn ${a.primary ? "btn-primary" : ""}`;
        el.href = a.href;
        el.textContent = a.label;
        row.appendChild(el);
        continue;
      }
      const b = document.createElement("button");
      b.className = `btn ${a.primary ? "btn-primary" : ""}`;
      b.textContent = a.label;
      b.addEventListener("click", () => a.onClick?.());
      row.appendChild(b);
    }
    card.appendChild(row);
  }

  return { card, body };
}

window.MBA = {
  fetchJson,
  fmtAge,
  shortId,
  setStatusText,
  downloadJson,
  copyText,
  setModePill,
  setWeatherPill,
  explorerHref,
  createCard,
};

function getAdminToken() {
  try {
    return localStorage.getItem("metrobikeatlas.admin_token") || "";
  } catch {
    return "";
  }
}

async function adminFetch(url, options = {}) {
  const token = getAdminToken();
  const headers = new Headers(options.headers || {});
  if (token) headers.set("X-Admin-Token", token);
  return await fetch(url, { ...options, headers });
}

async function adminFetchJson(url) {
  const res = await adminFetch(url);
  const text = await res.text().catch(() => "");
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    payload = { detail: text };
  }
  if (!res.ok) {
    const detail = payload?.detail;
    const msg =
      typeof detail === "string"
        ? detail
        : typeof detail === "object" && detail
          ? detail.message || detail.code || JSON.stringify(detail)
          : `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return payload;
}

async function adminPostJson(url, body) {
  const res = await adminFetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body == null ? "{}" : JSON.stringify(body),
  });
  const text = await res.text().catch(() => "");
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    payload = { detail: text };
  }
  if (!res.ok) {
    const detail = payload?.detail;
    const msg =
      typeof detail === "string"
        ? detail
        : typeof detail === "object" && detail
          ? detail.message || detail.code || JSON.stringify(detail)
          : `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return payload;
}

window.MBA.getAdminToken = getAdminToken;
window.MBA.adminFetchJson = adminFetchJson;
window.MBA.adminPostJson = adminPostJson;
