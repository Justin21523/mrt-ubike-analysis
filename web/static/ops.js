import * as Core from "./lib/mba_core.js";
import { createCard } from "./lib/mba_cards.js";

const MBA = { ...Core, createCard };

function getAdminToken() {
  try {
    return localStorage.getItem("metrobikeatlas.admin_token") || "";
  } catch {
    return "";
  }
}

function setAdminToken(value) {
  try {
    if (!value) localStorage.removeItem("metrobikeatlas.admin_token");
    else localStorage.setItem("metrobikeatlas.admin_token", value);
  } catch {
    // ignore
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
    const err = new Error(msg);
    err.status = res.status;
    err.code = typeof detail === "object" && detail ? detail.code : null;
    err.detail = detail;
    throw err;
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
    const err = new Error(msg);
    err.status = res.status;
    err.code = typeof detail === "object" && detail ? detail.code : null;
    err.detail = detail;
    throw err;
  }
  return payload;
}

function setOverlayVisible(visible, text) {
  const el = document.getElementById("loadingOverlay");
  const t = document.getElementById("overlayText");
  if (!el) return;
  if (t && text) t.textContent = text;
  el.classList.toggle("hidden", !visible);
  el.setAttribute("aria-hidden", visible ? "false" : "true");
  el.hidden = !visible;
}

function storyParagraphCard() {
  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Ops Flow",
    title: "Problem → Evidence → Implication → Action",
    badge: { tone: "muted", text: "guide" },
  });
  body.innerHTML = `
    <div><span class="mono">Problem</span> · 資料不新鮮/缺檔/限流/卡住。</div>
    <div><span class="mono">Evidence</span> · /status + jobs + external inputs。</div>
    <div><span class="mono">Implication</span> · 影響前端敘事與洞察可信度。</div>
    <div><span class="mono">Action</span> · Start/Build/Refresh，並保留 log 與版本。</div>
  `;
  return card;
}

function credibilityCard(status, meta) {
  const resolved = meta?.meta ?? {};
  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Data Credibility",
    title: "Sources · freshness · traceability",
    badge: { tone: status?.demo_mode ? "warn" : "ok", text: status?.demo_mode ? "demo" : "real" },
    right: `<span class="mono">build ${MBA.shortId(resolved.silver_build_id)}</span>`,
    actions: [
      {
        label: "Download JSON",
        onClick: () => MBA.downloadJson(`metrobikeatlas-ops-meta-${new Date().toISOString()}.json`, { status, meta }),
      },
      {
        label: "Copy summary",
        onClick: async () => {
          const h = status?.health ?? {};
          const line = `collector=${h.collector_running ? "running" : "stopped"} bronze=${MBA.fmtAge(h.bronze_bike_availability_age_s)} silver=${MBA.fmtAge(
            Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity))
          )} build=${MBA.shortId(resolved.silver_build_id)} hash=${MBA.shortId(resolved.inputs_hash, 10)}`;
          await MBA.copyText(line);
          MBA.setStatusText("Copied");
        },
      },
    ],
  });
  body.innerHTML = `<div class="hint">Silver: <span class="mono">${MBA.shortId(resolved.silver_build_id)}</span> · hash <span class="mono">${MBA.shortId(
    resolved.inputs_hash,
    10
  )}</span> · source <span class="mono">${resolved.fallback_source || "—"}</span></div>`;
  return card;
}

function diagnosisLine(status, meta) {
  const h = status?.health ?? {};
  const w = meta?.meta?.external?.weather_collector ?? null;
  const issues = [];
  if (!h.collector_running) issues.push("collector stopped");
  if (!Number.isFinite(Number(h.bronze_bike_availability_age_s))) issues.push("bronze missing");
  const silverAge = Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity));
  if (!Number.isFinite(silverAge)) issues.push("silver missing");
  if (Number(h.metro_tdx_404_count || 0)) issues.push(`metro 404×${h.metro_tdx_404_count}`);
  if (w?.stale) issues.push("weather stale");
  return issues.length ? `Needs attention: ${issues.join(" · ")}` : "All systems look healthy.";
}

function healthCard(status, meta) {
  const h = status?.health ?? {};
  const w = meta?.meta?.external?.weather_collector ?? null;
  const { card, body } = MBA.createCard({
    tone: "primary",
    kicker: "Health",
    title: "One-line diagnosis",
    badge: { tone: "ok", text: "status" },
    actions: [{ label: "Refresh", primary: true, onClick: () => document.body.dispatchEvent(new Event("mba_refresh")) }],
  });
  body.innerHTML = `
    <div style="font-weight:900;">${diagnosisLine(status, meta)}</div>
    <div class="hint mono" style="margin-top:8px;">
      collector=${h.collector_running ? "running" : "stopped"} · bronze=${MBA.fmtAge(h.bronze_bike_availability_age_s)} · silver=${MBA.fmtAge(
        Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity))
      )} · weather=${w ? (w.stale ? "stale" : "ok") : "—"}
    </div>
  `;
  return card;
}

function quickFixCard({ onStart, onStop, onBuild, onRefreshWeather }) {
  const { card, body } = MBA.createCard({
    tone: "primary",
    kicker: "Quick Fix",
    title: "Start/Stop/Build/Refresh",
    badge: { tone: "ok", text: "actions" },
    actions: [
      { label: "Start collector", primary: true, onClick: onStart },
      { label: "Stop collector", onClick: onStop },
      { label: "Build Silver", onClick: onBuild },
      { label: "Refresh weather", onClick: onRefreshWeather },
    ],
  });
  body.innerHTML = `<div class="hint">Admin endpoints are localhost/token-limited. If you see forbidden, set the admin token below.</div>`;
  return card;
}

function adminTokenCard() {
  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Access",
    title: "Admin token (optional)",
    badge: { tone: "muted", text: "security" },
    actions: [
      {
        label: "Save token",
        primary: true,
        onClick: () => {
          const v = String(document.getElementById("adminTokenInput")?.value || "").trim();
          setAdminToken(v);
          MBA.setStatusText(v ? "Admin token saved" : "Admin token cleared");
        },
      },
      {
        label: "Clear",
        onClick: () => {
          setAdminToken("");
          const input = document.getElementById("adminTokenInput");
          if (input) input.value = "";
          MBA.setStatusText("Admin token cleared");
        },
      },
    ],
  });
  body.innerHTML = `<div class="row row-actions" style="margin-top:0;">
      <input class="input" id="adminTokenInput" type="password" placeholder="X-Admin-Token (optional)" />
    </div>`;
  return card;
}

function jobsCard(jobs) {
  const items = Array.isArray(jobs) ? jobs.slice(0, 5) : [];
  const { card, body } = MBA.createCard({
    tone: "support",
    kicker: "Jobs",
    title: "Recent jobs (Top 5)",
    badge: { tone: "ok", text: String(items.length) },
    actions: [{ label: "Download JSON", onClick: () => MBA.downloadJson(`metrobikeatlas-jobs-${new Date().toISOString()}.json`, jobs) }],
  });
  if (!items.length) {
    body.innerHTML = `<div class="hint">No jobs found.</div>`;
    return card;
  }
  const ul = document.createElement("ul");
  ul.className = "list";
  for (const j of items) {
    const li = document.createElement("li");
    li.innerHTML = `<div style="font-weight:900; font-size:12px;">${j.action || "job"} · ${String(j.status || "unknown")}</div>
      <div class="hint mono" style="margin-top:2px;">${MBA.shortId(j.id)} · ${j.started_at_utc || ""}</div>`;
    ul.appendChild(li);
  }
  body.appendChild(ul);
  return card;
}

function externalCard(title, payload, { previewPath, downloadPath }) {
  const ok = Boolean(payload?.ok);
  const rows = payload?.row_count ?? "—";
  const { card, body } = MBA.createCard({
    tone: "support",
    kicker: "External Inputs",
    title,
    badge: { tone: ok ? "ok" : "warn", text: ok ? "ok" : "check" },
    right: `<span class="mono">rows ${rows}</span>`,
    actions: [
      { type: "link", label: "Download", href: downloadPath },
      { label: "Copy path", onClick: async () => MBA.copyText(String(payload?.path || previewPath)) },
    ],
  });
  if (!payload) {
    body.innerHTML = `<div class="hint">Unavailable (localhost/token-only).</div>`;
    return card;
  }
  const issues = Array.isArray(payload.issues) ? payload.issues.slice(0, 3) : [];
  body.innerHTML = `
    <div class="hint mono">${payload.path || previewPath}</div>
    ${issues.length ? `<div class="hint">Issues: ${issues.map((i) => i.message || "issue").join(" · ")}</div>` : `<div class="hint">No issues.</div>`}
  `;
  return card;
}

function adminAccessCard(err) {
  const { card, body } = MBA.createCard({
    tone: "support",
    kicker: "Access",
    title: "Admin endpoints are restricted",
    badge: { tone: "warn", text: "forbidden" },
    actions: [
      { type: "link", label: "Open via 127.0.0.1", href: "http://127.0.0.1:8000/ops" },
      {
        label: "Copy help",
        onClick: async () => {
          const lines = [
            "Admin endpoints are localhost-only by default.",
            "Fix options:",
            "1) Open the UI from the same machine: http://127.0.0.1:8000/ops",
            "2) Or set METROBIKEATLAS_ADMIN_TOKEN and paste it in the token box on this page.",
          ];
          await MBA.copyText(lines.join("\n"));
          MBA.setStatusText("Copied");
        },
      },
    ],
  });
  const msg = err?.code === "admin_forbidden" ? "Need localhost or X-Admin-Token." : `Admin error: ${err?.message || "forbidden"}`;
  body.innerHTML = `
    <div style="font-weight:800;">${msg}</div>
    <div class="hint">This page shows Jobs and External CSV previews only when admin access is allowed.</div>
  `;
  return card;
}

async function main() {
  MBA.setStatusText("Loading…");
  const root = document.getElementById("opsCards");
  if (!root) return;

  document.body.addEventListener("mba_refresh", () => refresh().catch(() => {}));

  const refresh = async () => {
    MBA.setStatusText("Refreshing…");
    const [status, meta] = await Promise.all([MBA.fetchJson("/status"), MBA.fetchJson("/meta")]);
    MBA.setModePill(Boolean(status?.demo_mode));
    MBA.setWeatherPill(meta);

    let jobs = [];
    let adminErr = null;
    try {
      jobs = await adminFetchJson("/admin/jobs?limit=20");
    } catch (e) {
      jobs = [];
      if (!adminErr && Number(e?.status) === 403) adminErr = e;
    }

    let metro = null;
    let cal = null;
    let weather = null;
    try {
      metro = await adminFetchJson("/external/metro_stations/preview?limit=1");
    } catch (e) {
      if (!adminErr && Number(e?.status) === 403) adminErr = e;
    }
    try {
      cal = await adminFetchJson("/external/calendar/preview?limit=1");
    } catch (e) {
      if (!adminErr && Number(e?.status) === 403) adminErr = e;
    }
    try {
      weather = await adminFetchJson("/external/weather_hourly/preview?limit=1");
    } catch (e) {
      if (!adminErr && Number(e?.status) === 403) adminErr = e;
    }

    root.innerHTML = "";
    root.appendChild(credibilityCard(status, meta));
    root.appendChild(storyParagraphCard());
    root.appendChild(healthCard(status, meta));
    if (adminErr) root.appendChild(adminAccessCard(adminErr));

    root.appendChild(
      quickFixCard({
        onStart: async () => {
          setOverlayVisible(true, "Starting collector…");
          try {
            await adminPostJson("/admin/collector/start", null);
            MBA.setStatusText("Collector started");
          } catch (e) {
            MBA.setStatusText(`Start failed: ${e.message}`);
          } finally {
            setOverlayVisible(false);
            await refresh();
          }
        },
        onStop: async () => {
          setOverlayVisible(true, "Stopping collector…");
          try {
            await adminPostJson("/admin/collector/stop", null);
            MBA.setStatusText("Collector stopped");
          } catch (e) {
            MBA.setStatusText(`Stop failed: ${e.message}`);
          } finally {
            setOverlayVisible(false);
            await refresh();
          }
        },
        onBuild: async () => {
          setOverlayVisible(true, "Starting Silver build…");
          try {
            await adminPostJson("/admin/build_silver_async", null);
            MBA.setStatusText("Silver build job started");
          } catch (e) {
            MBA.setStatusText(`Build failed: ${e.message}`);
          } finally {
            setOverlayVisible(false);
            await refresh();
          }
        },
        onRefreshWeather: async () => {
          setOverlayVisible(true, "Refreshing weather…");
          try {
            await adminPostJson("/admin/weather/refresh", null);
            MBA.setStatusText("Weather refresh requested");
          } catch (e) {
            MBA.setStatusText(`Refresh failed: ${e.message}`);
          } finally {
            setOverlayVisible(false);
            await refresh();
          }
        },
      })
    );

    root.appendChild(adminTokenCard());
    const tokenInput = document.getElementById("adminTokenInput");
    if (tokenInput) tokenInput.value = getAdminToken();

    root.appendChild(jobsCard(jobs));

    root.appendChild(
      externalCard("Metro stations (CSV)", metro, {
        previewPath: "/external/metro_stations/preview?limit=1",
        downloadPath: "/external/metro_stations/download",
      })
    );
    root.appendChild(
      externalCard("Calendar (CSV)", cal, {
        previewPath: "/external/calendar/preview?limit=1",
        downloadPath: "/external/calendar/download",
      })
    );
    root.appendChild(
      externalCard("Weather hourly (CSV)", weather, {
        previewPath: "/external/weather_hourly/preview?limit=1",
        downloadPath: "/external/weather_hourly/download",
      })
    );

    MBA.setStatusText("Ready");
  };

  await refresh();
}

main().catch((e) => {
  console.error(e);
  MBA.setStatusText(`Error: ${e.message}`);
});
