function clamp(n, lo, hi) {
  const x = Number(n);
  if (!Number.isFinite(x)) return lo;
  return Math.max(lo, Math.min(hi, x));
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  return await res.json();
}

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

function fmtAge(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s)) return "—";
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  if (s < 86400) return `${(s / 3600).toFixed(1)}h`;
  return `${(s / 86400).toFixed(1)}d`;
}

function setStatusText(text) {
  const el = document.getElementById("statusText");
  if (el) el.textContent = text;
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

function setModePill(demoMode) {
  const el = document.getElementById("modePill");
  if (!el) return;
  el.textContent = demoMode ? "Demo mode" : "Real data mode";
  el.classList.remove("ok", "warn", "bad");
  el.classList.add(demoMode ? "warn" : "ok");
}

function setWeatherPill(meta) {
  const el = document.getElementById("weatherPill");
  if (!el) return;
  el.classList.remove("ok", "warn", "bad");
  const w = meta?.meta?.external?.weather_collector ?? null;
  if (!w) {
    el.textContent = "Weather: unavailable";
    el.classList.add("warn");
    return;
  }
  el.textContent = `Weather: ${w.stale ? "stale" : "ok"} (${fmtAge(w.heartbeat_age_s)})${w.is_rainy_now ? " · rain" : ""}`;
  el.classList.add(w.stale ? "warn" : "ok");
}

function renderHealthCards(status) {
  const root = document.getElementById("healthCards");
  if (!root) return;
  const h = status?.health ?? {};
  root.innerHTML = "";
  const mk = (title, value, meta) => {
    const el = document.createElement("div");
    el.className = "health-card";
    el.innerHTML = `<div class="health-title">${title}</div><div class="health-value">${value}</div><div class="health-meta mono">${meta || ""}</div>`;
    return el;
  };
  root.appendChild(mk("Collector", h.collector_running ? "running" : "stopped", h.collector_last_ok_utc || ""));
  root.appendChild(mk("Bronze", fmtAge(h.bronze_bike_availability_age_s), h.bronze_bike_availability_last_utc || ""));
  root.appendChild(
    mk(
      "Silver",
      fmtAge(Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity))),
      "links/bike_timeseries"
    )
  );
}

function renderAlerts(status) {
  const root = document.getElementById("opsAlerts");
  if (!root) return;
  const alerts = status?.alerts ?? [];
  root.innerHTML = "";
  if (!alerts.length) {
    root.innerHTML = `<div class="hint">No alerts.</div>`;
    return;
  }
  for (const a of alerts.slice(0, 8)) {
    const div = document.createElement("div");
    div.className = `briefing-callout ${(a.level || "info").toLowerCase()}`;
    div.style.marginTop = "8px";
    div.innerHTML = `<div class="briefing-callout-title">${a.title || "Alert"}</div><div class="briefing-callout-body">${a.message || ""}</div>`;
    root.appendChild(div);
  }
}

function renderJobs(jobs) {
  const root = document.getElementById("jobCenter");
  if (!root) return;
  const arr = Array.isArray(jobs) ? jobs : [];
  if (!arr.length) {
    root.innerHTML = `<div class="hint">No jobs.</div>`;
    return;
  }
  const list = document.createElement("ul");
  list.className = "list";
  for (const j of arr.slice(0, 20)) {
    const li = document.createElement("li");
    const s = `${String(j.status || "unknown")}`.toLowerCase();
    li.innerHTML = `<div style="font-weight:800; font-size:12px;">${j.action || "job"} · ${s}</div><div class="hint mono">${j.id || ""}</div>`;
    list.appendChild(li);
  }
  root.innerHTML = "";
  root.appendChild(list);
}

async function refreshExternalPanel(id, path) {
  const root = document.getElementById(id);
  if (!root) return;
  try {
    const payload = await adminFetchJson(path);
    root.innerHTML = `<div class="hint mono">${payload.path || path}</div><div class="hint">ok=${payload.ok} · rows=${payload.row_count ?? "—"}</div>`;
  } catch (e) {
    root.innerHTML = `<div class="hint">Unavailable (localhost/token-only): ${e.message}</div>`;
  }
}

async function main() {
  setStatusText("Loading…");
  const tokenInput = document.getElementById("adminTokenInput");
  if (tokenInput) tokenInput.value = getAdminToken();

  document.getElementById("btnSaveAdminToken")?.addEventListener("click", () => {
    const v = String(document.getElementById("adminTokenInput")?.value || "").trim();
    setAdminToken(v);
    setStatusText(v ? "Admin token saved" : "Admin token cleared");
  });
  document.getElementById("btnClearAdminToken")?.addEventListener("click", () => {
    setAdminToken("");
    if (tokenInput) tokenInput.value = "";
    setStatusText("Admin token cleared");
  });

  const refresh = async () => {
    const [status, meta] = await Promise.all([fetchJson("/status"), fetchJson("/meta")]);
    setModePill(Boolean(status?.demo_mode));
    setWeatherPill(meta);
    renderHealthCards(status);
    renderAlerts(status);
    document.getElementById("statusUpdatedAt").textContent = `updated ${new Date().toISOString()}`;
    try {
      const jobs = await adminFetchJson("/admin/jobs?limit=20");
      renderJobs(jobs);
    } catch (e) {
      document.getElementById("jobCenter").innerHTML = `<div class="hint">Jobs unavailable: ${e.message}</div>`;
    }
    await refreshExternalPanel("externalMetro", "/external/metro_stations/preview?limit=1");
    await refreshExternalPanel("externalCalendar", "/external/calendar/preview?limit=1");
    await refreshExternalPanel("externalWeather", "/external/weather_hourly/preview?limit=1");
  };

  document.getElementById("btnRefreshStatus")?.addEventListener("click", () => refresh().catch(() => {}));

  document.getElementById("btnStartCollector")?.addEventListener("click", async () => {
    setOverlayVisible(true, "Starting collector…");
    try {
      const out = await adminPostJson("/admin/collector/start", null);
      document.getElementById("adminResult").textContent = JSON.stringify(out, null, 2);
      await refresh();
      setStatusText("Collector started");
    } catch (e) {
      setStatusText(`Start failed: ${e.message}`);
    } finally {
      setOverlayVisible(false);
    }
  });
  document.getElementById("btnStopCollector")?.addEventListener("click", async () => {
    setOverlayVisible(true, "Stopping collector…");
    try {
      const out = await adminPostJson("/admin/collector/stop", null);
      document.getElementById("adminResult").textContent = JSON.stringify(out, null, 2);
      await refresh();
      setStatusText("Collector stopped");
    } catch (e) {
      setStatusText(`Stop failed: ${e.message}`);
    } finally {
      setOverlayVisible(false);
    }
  });
  document.getElementById("btnBuildSilver")?.addEventListener("click", async () => {
    setOverlayVisible(true, "Starting Silver build…");
    try {
      const out = await adminPostJson("/admin/build_silver_async", null);
      document.getElementById("adminResult").textContent = JSON.stringify(out, null, 2);
      await refresh();
      setStatusText("Silver build job started");
    } catch (e) {
      setStatusText(`Build failed: ${e.message}`);
    } finally {
      setOverlayVisible(false);
    }
  });
  document.getElementById("btnRefreshWeather")?.addEventListener("click", async () => {
    setOverlayVisible(true, "Refreshing weather…");
    try {
      const out = await adminPostJson("/admin/weather/refresh", null);
      document.getElementById("adminResult").textContent = JSON.stringify(out, null, 2);
      await refresh();
      setStatusText("Weather refresh requested");
    } catch (e) {
      setStatusText(`Refresh failed: ${e.message}`);
    } finally {
      setOverlayVisible(false);
    }
  });

  await refresh();
  setStatusText("Ready");
}

main().catch((e) => {
  console.error(e);
  setStatusText(`Error: ${e.message}`);
});

