export async function fetchJson(url, { signal } = {}) {
  const res = await fetch(url, { signal });
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  return await res.json();
}

export function fmtAge(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s)) return "—";
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  if (s < 86400) return `${(s / 3600).toFixed(1)}h`;
  return `${(s / 86400).toFixed(1)}d`;
}

export function shortId(x, n = 8) {
  if (!x) return "—";
  const s = String(x);
  return s.length <= n ? s : s.slice(0, n);
}

export function setStatusText(text) {
  const el = document.getElementById("statusText");
  if (el) el.textContent = text || "";
}

export function downloadJson(filename, payload) {
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

export async function copyText(text) {
  await navigator.clipboard.writeText(text);
}

export function setModePill(demoMode) {
  const el = document.getElementById("modePill");
  if (!el) return;
  el.textContent = demoMode ? "Demo mode" : "Real data mode";
  el.classList.remove("ok", "warn", "bad");
  el.classList.add(demoMode ? "warn" : "ok");
}

export function setWeatherPill(metaPayload) {
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

export function explorerHref(params) {
  const items = [];
  for (const [k, v] of Object.entries(params || {})) {
    if (v == null || v === "") continue;
    items.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
  }
  const hash = items.length ? `#${items.join("&")}` : "";
  return `/explorer${hash}`;
}

export function getAdminToken() {
  try {
    return localStorage.getItem("metrobikeatlas.admin_token") || "";
  } catch {
    return "";
  }
}

export async function adminFetch(url, options = {}) {
  const token = getAdminToken();
  const headers = new Headers(options.headers || {});
  if (token) headers.set("X-Admin-Token", token);
  return await fetch(url, { ...options, headers });
}

function decodeAdminError(payload, statusCode) {
  const detail = payload?.detail;
  return typeof detail === "string"
    ? detail
    : typeof detail === "object" && detail
      ? detail.message || detail.code || JSON.stringify(detail)
      : `HTTP ${statusCode}`;
}

export async function adminFetchJson(url) {
  const res = await adminFetch(url);
  const text = await res.text().catch(() => "");
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    payload = { detail: text };
  }
  if (!res.ok) throw new Error(decodeAdminError(payload, res.status));
  return payload;
}

export async function adminPostJson(url, body) {
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
  if (!res.ok) throw new Error(decodeAdminError(payload, res.status));
  return payload;
}

