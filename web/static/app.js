async function fetchJson(url, { signal } = {}) {
  const res = await fetch(url, { signal });
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

function guideAdminAuth(message) {
  const msg = String(message || "").toLowerCase();
  if (!msg.includes("admin_forbidden") && !msg.includes("localhost") && !msg.includes("x-admin-token")) return;
  try {
    const details = document.getElementById("detailsDataStatus");
    if (details) details.open = true;
    const input = document.getElementById("adminTokenInput");
    input?.focus?.();
    input?.scrollIntoView?.({ behavior: "smooth", block: "center" });
  } catch {
    // ignore
  }
}

async function postJson(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body == null ? "{}" : JSON.stringify(body),
  });
  const text = await res.text();
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

async function adminFetch(url, options = {}) {
  const token = getAdminToken();
  const headers = new Headers(options.headers || {});
  if (token) headers.set("X-Admin-Token", token);
  return await fetch(url, { ...options, headers });
}

async function adminFetchJson(url, { signal } = {}) {
  const res = await adminFetch(url, { signal });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    let payload = null;
    try {
      payload = text ? JSON.parse(text) : null;
    } catch {
      payload = { detail: text };
    }
    const detail = payload?.detail;
    const msg =
      typeof detail === "string"
        ? detail
        : typeof detail === "object" && detail
          ? detail.message || detail.code || JSON.stringify(detail)
          : `HTTP ${res.status}`;
    guideAdminAuth(msg);
    throw new Error(msg);
  }
  return await res.json();
}

async function adminPostJson(url, body, { retries = 2 } = {}) {
  let attempt = 0;
  while (true) {
    try {
      const res = await adminFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: body == null ? "{}" : JSON.stringify(body),
      });
      const text = await res.text();
      let payload = null;
      try {
        payload = text ? JSON.parse(text) : null;
      } catch {
        payload = { detail: text };
      }
      if (!res.ok) {
        if (res.status === 429 && attempt < retries) {
          attempt += 1;
          setStatusText?.(`Rate limited; retrying (${attempt}/${retries})…`);
          await new Promise((r) => setTimeout(r, 600 * attempt));
          continue;
        }
        const detail = payload?.detail;
        const msg =
          typeof detail === "string"
            ? detail
            : typeof detail === "object" && detail
              ? detail.message || detail.code || JSON.stringify(detail)
              : `HTTP ${res.status}`;
        guideAdminAuth(msg);
        throw new Error(msg);
      }
      return payload;
    } catch (e) {
      if (attempt < retries && String(e?.message || "").toLowerCase().includes("rate")) {
        attempt += 1;
        await new Promise((r) => setTimeout(r, 600 * attempt));
        continue;
      }
      throw e;
    }
  }
}

function debounce(fn, ms) {
  let t = null;
  return (...args) => {
    if (t) clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

function isTypingTarget(el) {
  if (!el) return false;
  const tag = (el.tagName || "").toLowerCase();
  return tag === "input" || tag === "textarea" || tag === "select" || el.isContentEditable;
}

function clamp(x, lo, hi) {
  return Math.min(hi, Math.max(lo, x));
}

function qs(params) {
  const items = [];
  for (const [k, v] of Object.entries(params || {})) {
    if (v == null || v === "") continue;
    items.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
  }
  return items.length ? `?${items.join("&")}` : "";
}

function parseHashParams() {
  const raw = String(location.hash || "").replace(/^#/, "");
  if (!raw) return {};
  const out = {};
  for (const part of raw.split("&")) {
    const [k, v] = part.split("=");
    if (!k) continue;
    out[decodeURIComponent(k)] = v == null ? "" : decodeURIComponent(v);
  }
  return out;
}

function setHashParams(params) {
  const items = [];
  for (const [k, v] of Object.entries(params || {})) {
    if (v == null || v === "") continue;
    items.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
  }
  const next = items.length ? `#${items.join("&")}` : "";
  if (next === location.hash) return;
  history.replaceState(null, "", `${location.pathname}${location.search}${next}`);
}

const CLUSTER_COLORS = ["#2a6fdb", "#db2a6f", "#2adb6f", "#dbb52a", "#8a2adb", "#2adbb5", "#db6f2a"];

function clusterColor(cluster) {
  if (cluster == null) return "#2a6fdb";
  const idx = Number(cluster);
  if (!Number.isFinite(idx)) return "#2a6fdb";
  return CLUSTER_COLORS[Math.abs(idx) % CLUSTER_COLORS.length];
}

function seriesLabel(series) {
  if (!series) return "No data";
  const proxy = series.is_proxy ? " (proxy)" : "";
  const src = series.source ? ` · ${series.source}` : "";
  return `${series.metric}${proxy}${src}`;
}

function toChartData(points) {
  return {
    labels: points.map((p) => new Date(p.ts).toLocaleString()),
    values: points.map((p) => p.value),
  };
}

function buildChart(canvas, label, { heightPx = 190 } = {}) {
  if (!canvas) return null;
  canvas.style.width = "100%";
  canvas.style.height = `${Math.max(80, Number(heightPx) || 190)}px`;
  return new Chart(canvas, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label,
          data: [],
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.25,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: { type: "category", ticks: { maxTicksLimit: 6 } },
        y: { beginAtZero: true },
      },
      plugins: {
        legend: { display: false },
        tooltip: { intersect: false, mode: "index" },
      },
    },
  });
}

function setChartData(chart, points) {
  const d = toChartData(points);
  chart.data.labels = d.labels;
  chart.data.datasets[0].data = d.values;
  chart.update();
}

function setNearbyList(items) {
  const list = document.getElementById("nearbyList");
  list.innerHTML = "";
  for (const s of items) {
    const li = document.createElement("li");
    li.textContent = `${s.name} · ${Math.round(s.distance_m)}m`;
    list.appendChild(li);
  }
}

function setFactors(factorsPayload) {
  const hint = document.getElementById("factorsHint");
  const body = document.getElementById("factorsTableBody");
  body.innerHTML = "";

  if (!factorsPayload?.available) {
    hint.textContent = "No factors available. In real data mode, run: python scripts/build_features.py";
    return;
  }

  hint.textContent = "";
  for (const f of factorsPayload.factors ?? []) {
    const tr = document.createElement("tr");
    const pct = f.percentile == null ? "" : `${Math.round(f.percentile * 100)}%`;

    tr.innerHTML = `
      <td class="mono">${f.name}</td>
      <td class="mono">${f.value ?? ""}</td>
      <td>${pct}</td>
    `;
    body.appendChild(tr);
  }
}

function setSimilarStations(items, onPickStation) {
  const list = document.getElementById("similarList");
  list.innerHTML = "";
  for (const s of items ?? []) {
    const li = document.createElement("li");
    const name = s.name ?? s.id;
    const cluster = s.cluster == null ? "" : ` · cluster ${s.cluster}`;
    li.textContent = `${name} · d=${s.distance.toFixed(3)}${cluster}`;
    li.style.cursor = "pointer";
    li.addEventListener("click", () => onPickStation(s.id));
    list.appendChild(li);
  }
}

function setOverview(payload) {
  const hint = document.getElementById("overviewHint");
  const list = document.getElementById("correlationList");
  list.innerHTML = "";

  if (!payload?.available) {
    hint.textContent = "No analytics available. Run: python scripts/build_analytics.py";
    return;
  }

  const r2 = payload.regression?.r2;
  hint.textContent = r2 == null ? "" : `Regression R²: ${r2.toFixed(3)}`;

  for (const c of payload.correlations?.slice(0, 10) ?? []) {
    const li = document.createElement("li");
    li.textContent = `${c.feature} · corr=${Number(c.correlation).toFixed(3)} · n=${c.n}`;
    list.appendChild(li);
  }
}

function fmtTs(value) {
  if (!value) return "";
  const d = new Date(value);
  if (!Number.isFinite(d.getTime())) return String(value);
  return d.toLocaleString();
}

function formatAge(seconds) {
  if (seconds == null) return "—";
  const s = Math.max(0, Math.floor(Number(seconds)));
  if (!Number.isFinite(s)) return "—";
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h`;
  const d = Math.floor(h / 24);
  return `${d}d`;
}

function stationShortName(name) {
  const s = String(name || "").trim();
  if (!s) return "MRT";
  // If Chinese, keep first 2 chars; otherwise keep first 8 chars.
  if (/[\u4e00-\u9fff]/.test(s)) return s.slice(0, 2);
  return s.length <= 8 ? s : `${s.slice(0, 8)}…`;
}

function setHealthCards(status) {
  const root = document.getElementById("healthCards");
  if (!root) return;
  root.innerHTML = "";
  const h = status?.health ?? {};
  const cards = [
    {
      title: "Collector",
      value: h.collector_running ? "Running" : "Stopped",
      meta: h.collector_pid ? `pid ${h.collector_pid}` : "",
      bad: !h.collector_running,
    },
    {
      title: "Bronze freshness",
      value: formatAge(h.bronze_bike_availability_age_s),
      meta: h.bronze_bike_availability_last_utc ? fmtTs(h.bronze_bike_availability_last_utc) : "",
      bad: h.bronze_bike_availability_age_s != null && Number(h.bronze_bike_availability_age_s) > 3600,
    },
    {
      title: "Silver freshness",
      value: formatAge(
        Math.min(
          Number(h.silver_metro_bike_links_age_s ?? Infinity),
          Number(h.silver_bike_timeseries_age_s ?? Infinity)
        )
      ),
      meta: "links/bike_timeseries",
      bad:
        (h.silver_metro_bike_links_age_s != null && Number(h.silver_metro_bike_links_age_s) > 86400) ||
        (h.silver_bike_timeseries_age_s != null && Number(h.silver_bike_timeseries_age_s) > 86400),
    },
  ];
  if (Number(h.metro_tdx_404_count || 0) > 0) {
    cards.push({
      title: "Metro TDX 404",
      value: String(h.metro_tdx_404_count),
      meta: h.metro_tdx_404_last_utc ? `last ${fmtTs(h.metro_tdx_404_last_utc)}` : "",
      bad: true,
    });
  }
  for (const c of cards) {
    const el = document.createElement("div");
    el.className = `health-card ${c.bad ? "bad" : ""}`;
    el.innerHTML = `<div class="health-title">${c.title}</div><div class="health-value">${c.value}</div><div class="health-meta mono">${c.meta || ""}</div>`;
    root.appendChild(el);
  }
}

function renderStatusRows(container, rows) {
  container.innerHTML = "";
  for (const row of rows ?? []) {
    const ok = Boolean(row.exists ?? row.latest_file?.exists ?? false);
    const dot = document.createElement("div");
    dot.className = `dot ${ok ? "ok" : "bad"}`;

    const name = document.createElement("div");
    name.className = "mono";
    name.textContent = row.label ?? row.path ?? "";

    const meta = document.createElement("div");
    meta.className = "status-meta mono";
    if (row.mtime_utc) {
      meta.textContent = fmtTs(row.mtime_utc);
    } else if (row.latest_file?.mtime_utc) {
      meta.textContent = `${row.file_count ?? 0} files · ${fmtTs(row.latest_file.mtime_utc)}`;
    } else {
      meta.textContent = row.file_count != null ? `${row.file_count} files` : (ok ? "ok" : "missing");
    }

    const line = document.createElement("div");
    line.className = "status-row";
    line.appendChild(dot);
    line.appendChild(name);
    line.appendChild(meta);
    container.appendChild(line);
  }
}

function setStatusPanel(payload) {
  document.getElementById("statusUpdatedAt").textContent = payload?.now_utc
    ? `updated ${fmtTs(payload.now_utc)}`
    : "";
  document.getElementById("statusMode").textContent = payload?.demo_mode ? "demo" : "real";
  document.getElementById("statusBronzeDir").textContent = payload?.bronze_dir ?? "—";
  document.getElementById("statusSilverDir").textContent = payload?.silver_dir ?? "—";

  const tdxBase = payload?.tdx?.base_url ? String(payload.tdx.base_url) : "";
  const tdxMetroPath = payload?.tdx?.metro_stations_path_template ? String(payload.tdx.metro_stations_path_template) : "";
  const diag = [];
  if (payload?.metro_tdx_404_count) {
    const last = payload.metro_tdx_404_last_utc ? fmtTs(payload.metro_tdx_404_last_utc) : "";
    diag.push(`metro_404=${payload.metro_tdx_404_count}${last ? ` (last ${last})` : ""}`);
  }
  if (tdxBase) diag.push(`tdx=${tdxBase}`);
  if (tdxMetroPath) diag.push(`metro_path=${tdxMetroPath}`);
  if (diag.length) {
    setStatusText(diag.join(" · "));
  }

  setHealthCards(payload);

  try {
    const h = payload?.health ?? {};
    const parts = [];
    parts.push(`collector ${h.collector_running ? "running" : "stopped"}`);
    parts.push(`bronze ${formatAge(h.bronze_bike_availability_age_s)}`);
    parts.push(
      `silver ${formatAge(Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity)))}`
    );
    const ws = window.__mba_state?.weatherSummary ?? null;
    if (ws && typeof ws === "object") {
      parts.push(`weather ${ws.stale ? "stale" : "ok"} ${formatAge(ws.heartbeat_age_s)}`);
    } else {
      parts.push("weather —");
    }
    document.getElementById("statusDiagnosis").textContent = parts.join(" · ");
  } catch {
    document.getElementById("statusDiagnosis").textContent = "—";
  }

  renderStatusRows(document.getElementById("statusSilverTables"), payload?.silver_tables ?? []);
  renderStatusRows(document.getElementById("statusBronzeDatasets"), payload?.bronze_datasets ?? []);

  const alertsEl = document.getElementById("statusAlerts");
  alertsEl.innerHTML = "";
  for (const a of payload?.alerts ?? []) {
    const card = document.createElement("div");
    const level = (a.level || "info").toLowerCase();
    card.className = `alert ${
      level === "critical" ? "critical" : level === "warning" ? "warning" : level === "error" ? "error" : ""
    }`;

    const title = document.createElement("div");
    title.className = "alert-title";
    title.textContent = a.title || "Alert";

    const msg = document.createElement("div");
    msg.className = "alert-message";
    msg.textContent = a.message || "";

    card.appendChild(title);
    card.appendChild(msg);

    if (level === "critical") {
      const actions = document.createElement("div");
      actions.className = "row row-actions";
      const t = String(a.title || "");
      if (t.toLowerCase().includes("collector heartbeat")) {
        const btn = document.createElement("button");
        btn.className = "btn";
        btn.textContent = "Restart collector";
        btn.addEventListener("click", async () => {
          setOverlayVisible(true, "Restarting collector…");
          try {
            const res = await adminPostJson("/admin/collector/restart_if_stale?force=true", {});
            setStatusText(res?.detail || "Collector restart requested");
            document.getElementById("adminResult").innerHTML = `<div class="mono">${res?.detail || ""}</div>`;
            await refreshStatus({ quiet: true });
          } catch (e) {
            setStatusText(`Restart failed: ${e.message}`);
          } finally {
            setOverlayVisible(false);
          }
        });
        actions.appendChild(btn);
      }
      if (t.toLowerCase().includes("silver build failures")) {
        const btn = document.createElement("button");
        btn.className = "btn";
        btn.textContent = "Open Job Center";
        btn.addEventListener("click", () => {
          const el = document.getElementById("detailsDataStatus");
          if (el && el.open !== true) el.open = true;
          document.getElementById("jobCenter")?.scrollIntoView?.({ behavior: "smooth", block: "start" });
        });
        actions.appendChild(btn);
      }
      if (actions.childElementCount) card.appendChild(actions);
    }

    for (const cmd of a.commands ?? []) {
      const row = document.createElement("div");
      row.className = "cmd";
      const code = document.createElement("code");
      code.className = "mono";
      code.textContent = cmd;
      const btn = document.createElement("button");
      btn.className = "btn";
      btn.textContent = "Copy";
      btn.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(cmd);
          setStatusText("Copied command");
        } catch {
          setStatusText("Copy failed");
        }
      });
      row.appendChild(code);
      row.appendChild(btn);
      card.appendChild(row);
    }

    alertsEl.appendChild(card);
  }

  const collector = document.getElementById("statusCollector");
  collector.innerHTML = "";
  if (!payload?.collector) {
    collector.innerHTML = `<div class="hint">No collector info found. Start ingestion to populate logs/tdx_collect.*</div>`;
    return;
  }
  const c = payload.collector;
  const running = Boolean(c.running);
  const header = document.createElement("div");
  header.className = "status-kv";
  header.innerHTML = `<div class="label">Running</div><div class="mono">${running ? "true" : "false"}${c.pid ? ` (pid ${c.pid})` : ""}</div>`;

  const pidPath = document.createElement("div");
  pidPath.className = "status-kv";
  pidPath.innerHTML = `<div class="label">PID file</div><div class="mono">${c.pid_path ?? ""}</div>`;

  const logPath = document.createElement("div");
  logPath.className = "status-kv";
  logPath.innerHTML = `<div class="label">Log</div><div class="mono">${c.log_path ?? ""}</div>`;

  collector.appendChild(header);
  collector.appendChild(pidPath);
  collector.appendChild(logPath);

  const tail = document.createElement("pre");
  tail.className = "log-tail mono";
  tail.textContent = (c.log_tail ?? []).join("\n") || "—";
  collector.appendChild(tail);
}

function jobStatusBadge(status) {
  const s = String(status || "unknown").toLowerCase();
  if (s === "running") return `<span class="badge badge-running">running</span>`;
  if (s === "succeeded") return `<span class="badge badge-ok">succeeded</span>`;
  if (s === "failed") return `<span class="badge badge-bad">failed</span>`;
  if (s === "canceled") return `<span class="badge badge-warn">canceled</span>`;
  return `<span class="badge">unknown</span>`;
}

function fmtJobEvent(ev) {
  const ts = ev.ts_utc ? fmtTs(ev.ts_utc) : "—";
  const stage = ev.stage ? String(ev.stage) : "";
  const pct = ev.progress_pct == null ? "" : `${ev.progress_pct}%`;
  const msg = ev.message ? String(ev.message) : "";
  return `${ts} · ${stage}${pct ? ` · ${pct}` : ""}${msg ? ` · ${msg}` : ""}`.trim();
}

function setJobCenter(jobs, { onCancel, onDownload, onEvents } = {}) {
  const root = document.getElementById("jobCenter");
  if (!root) return;
  root.innerHTML = "";

  const list = document.createElement("div");
  list.className = "job-list";

  const items = jobs ?? [];
  if (!items.length) {
    root.innerHTML = `<div class="hint">No jobs yet. Use “Build Silver (async)” to create one.</div>`;
    return;
  }

  for (const j of items) {
    const row = document.createElement("div");
    row.className = "job-row";
    const started = j.started_at_utc ? fmtTs(j.started_at_utc) : "";
    const finished = j.finished_at_utc ? fmtTs(j.finished_at_utc) : "";
    const dur =
      j.started_at_utc && j.finished_at_utc
        ? ` · ${(new Date(j.finished_at_utc) - new Date(j.started_at_utc)) / 1000}s`
        : "";
    const pct = j.progress_pct == null ? "" : ` · ${j.progress_pct}%`;
    const stage = j.stage ? ` · ${j.stage}` : "";
    row.innerHTML = `
      <div class="job-main">
        <div class="mono">#${j.id.slice(0, 8)} · ${j.kind}</div>
        <div class="hint mono">${jobStatusBadge(j.status)}${pct}${stage}${started ? ` · started ${started}` : ""}${finished ? ` · finished ${finished}` : ""}${dur}</div>
      </div>
      <div class="job-actions"></div>
    `;
    const actions = row.querySelector(".job-actions");

    const eventsBox = document.createElement("pre");
    eventsBox.className = "log-tail mono hidden";
    eventsBox.textContent = "Loading events…";

    const btnLog = document.createElement("button");
    btnLog.className = "btn";
    btnLog.textContent = "Log";
    btnLog.addEventListener("click", () => onDownload?.(j));
    actions.appendChild(btnLog);

    const btnEvents = document.createElement("button");
    btnEvents.className = "btn";
    btnEvents.textContent = "Events";
    btnEvents.addEventListener("click", async () => {
      const willShow = eventsBox.classList.contains("hidden");
      eventsBox.classList.toggle("hidden", !willShow);
      if (!willShow) return;
      if (eventsBox.dataset.loaded === "1") return;
      try {
        const payload = await onEvents?.(j);
        const evs = payload?.events ?? [];
        const lines = [];
        for (const ev of evs) {
          lines.push(fmtJobEvent(ev));
          const artifacts = ev?.artifacts ?? [];
          for (const a of artifacts) {
            if (!a?.path) continue;
            const mt = a.mtime_utc ? ` · ${String(a.mtime_utc)}` : "";
            lines.push(`  - ${a.path}${mt}`);
          }
        }
        eventsBox.textContent = lines.join("\n") || "No MBA_EVENT entries found.";
        eventsBox.dataset.loaded = "1";
      } catch (e) {
        eventsBox.textContent = `Events unavailable (${e.message})`;
      }
    });
    actions.appendChild(btnEvents);

    const btnCancel = document.createElement("button");
    btnCancel.className = "btn";
    btnCancel.textContent = "Cancel";
    btnCancel.disabled = String(j.status || "").toLowerCase() !== "running";
    btnCancel.addEventListener("click", () => onCancel?.(j));
    actions.appendChild(btnCancel);

    const btnRerun = document.createElement("button");
    btnRerun.className = "btn";
    btnRerun.textContent = "Re-run";
    btnRerun.disabled = String(j.status || "").toLowerCase() === "running";
    btnRerun.addEventListener("click", () => onCancel?.({ ...j, _action: "rerun" }));
    actions.appendChild(btnRerun);

    list.appendChild(row);
    list.appendChild(eventsBox);
  }

  root.appendChild(list);
}

function renderExternalMetroPanel(payload, { onValidate, onUpload, onBuild } = {}) {
  const root = document.getElementById("externalMetro");
  if (!root) return;
  const ok = Boolean(payload?.ok);
  const issues = payload?.issues ?? [];
  const head = payload?.head ?? [];

  root.innerHTML = `
    <div class="status-kv">
      <div class="label">Path</div>
      <div class="mono">${payload?.path ?? "—"}</div>
    </div>
    <div class="status-kv">
      <div class="label">Rows</div>
      <div class="mono">${payload?.row_count ?? 0} ${ok ? "· ok" : "· not ok"}</div>
    </div>
    <div class="row row-actions">
      <button class="btn" id="btnExternalValidate">Validate</button>
      <input class="input" id="externalFile" type="file" accept=".csv,text/csv" />
      <button class="btn" id="btnExternalUpload">Upload</button>
      <button class="btn" id="btnExternalBuild" ${ok ? "" : "disabled"}>Build Silver</button>
    </div>
    <div class="hint">Localhost-only: upload/validate is disabled when opened remotely.</div>
    <div class="hint mono">${issues.map((i) => `${i.level}: ${i.message}`).join("\n") || ""}</div>
    <pre class="log-tail mono">${head.length ? JSON.stringify(head, null, 2) : "—"}</pre>
  `;

  root.querySelector("#btnExternalValidate").addEventListener("click", () => onValidate?.());
  root.querySelector("#btnExternalUpload").addEventListener("click", () => {
    const input = root.querySelector("#externalFile");
    const file = input?.files?.[0] ?? null;
    if (!file) return alert("Pick a CSV file first.");
    onUpload?.(file);
  });
  root.querySelector("#btnExternalBuild").addEventListener("click", () => onBuild?.());
}

function renderExternalCsvPanel(rootId, payload, { onValidate, onUpload, onBuild, onDownload } = {}) {
  const root = document.getElementById(rootId);
  if (!root) return;
  const ok = Boolean(payload?.ok);
  const issues = payload?.issues ?? [];
  const head = payload?.head ?? [];
  const noteHtml = payload?.note_html ? String(payload.note_html) : "";

  root.innerHTML = `
    ${noteHtml ? `<div class="hint">${noteHtml}</div>` : ""}
    <div class="status-kv">
      <div class="label">Path</div>
      <div class="mono">${payload?.path ?? "—"}</div>
    </div>
    <div class="status-kv">
      <div class="label">Rows</div>
      <div class="mono">${payload?.row_count ?? 0} ${ok ? "· ok" : "· not ok"}</div>
    </div>
    <div class="row row-actions">
      <button class="btn" data-action="validate">Validate</button>
      <input class="input" data-action="file" type="file" accept=".csv,text/csv" />
      <button class="btn" data-action="upload">Upload</button>
      <button class="btn" data-action="build" ${ok ? "" : "disabled"}>Build Silver</button>
      <button class="btn" data-action="download">Download</button>
    </div>
    <div class="hint">Localhost-only: upload/validate is disabled when opened remotely.</div>
    <div class="hint mono">${issues.map((i) => `${i.level}: ${i.message}`).join("\n") || ""}</div>
    <pre class="log-tail mono">${head.length ? JSON.stringify(head, null, 2) : "—"}</pre>
  `;

  root.querySelector('[data-action="validate"]').addEventListener("click", () => onValidate?.());
  root.querySelector('[data-action="upload"]').addEventListener("click", () => {
    const input = root.querySelector('[data-action="file"]');
    const file = input?.files?.[0] ?? null;
    if (!file) return alert("Pick a CSV file first.");
    onUpload?.(file);
  });
  root.querySelector('[data-action="build"]').addEventListener("click", () => onBuild?.());
  root.querySelector('[data-action="download"]').addEventListener("click", () => onDownload?.());
}

function setRequestMetaText(text) {
  const el = document.getElementById("requestMeta");
  if (!el) return;
  el.textContent = text || "";
}

function summarizeMeta(meta) {
  const m = meta || {};
  const build = m.silver_build_id || m?.silver_build_meta?.build_id || null;
  const hash = m.inputs_hash || m?.silver_build_meta?.inputs_hash || null;
  const src = m.fallback_source || null;
  const demo = m.demo_mode != null ? Boolean(m.demo_mode) : null;
  const parts = [];
  if (build) parts.push(`build=${String(build).slice(0, 8)}`);
  if (hash) parts.push(`hash=${String(hash).slice(0, 10)}`);
  if (src) parts.push(`source=${src}`);
  if (demo != null) parts.push(`demo=${demo ? "true" : "false"}`);
  return parts.join(" · ");
}

function loadStoredJson(key) {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function storeJson(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // ignore
  }
}

function defaultSettingsFromConfig(cfg) {
  return {
    join_method: cfg.spatial.join_method,
    radius_m: Math.round(cfg.spatial.radius_m),
    nearest_k: cfg.spatial.nearest_k,
    granularity: cfg.temporal.granularity,
    timezone: cfg.temporal.timezone,
    window_days: 7,
    metro_metric: "auto",
    bike_metric: "bike_available_bikes_total",
    similar_top_k: cfg.analytics.similarity.top_k,
    similar_metric: cfg.analytics.similarity.metric,
    similar_standardize: cfg.analytics.similarity.standardize,
    show_nearby_bikes: true,
    show_bike_heat: false,
    heat_metric: "available",
    heat_agg: "sum",
    heat_ts_index: -1,
    heat_follow_latest: true,
    problem_focus: false,
    problem_mode: "shortage",
    problem_top_n: 10,
    rain_mode: false,
    show_buffer: true,
    show_links: false,
    live: false,
    live_interval_sec: 30,
    left_collapsed: false,
    right_collapsed: false,
    app_view: "explorer",
    story_step: "problem",
    insights_mode: "hotspots",
    insights_top_k: 10,
  };
}

function mergeSettings(base, patch) {
  const out = { ...base };
  for (const [k, v] of Object.entries(patch || {})) out[k] = v;
  return out;
}

function buildPermalinkState(state) {
  return {
    station_id: state.selectedStationId || "",
    join_method: state.settings.join_method,
    radius_m: state.settings.radius_m,
    nearest_k: state.settings.nearest_k,
    granularity: state.settings.granularity,
    timezone: state.settings.timezone,
    window_days: state.settings.window_days,
    metro_metric: state.settings.metro_metric,
    bike_metric: state.settings.bike_metric,
    show_bike_heat: state.settings.show_bike_heat ? 1 : 0,
    heat_metric: state.settings.heat_metric,
    heat_agg: state.settings.heat_agg,
    heat_follow_latest: state.settings.heat_follow_latest ? 1 : 0,
  };
}

function setModePill(cfg) {
  const pill = document.getElementById("modePill");
  pill.textContent = cfg.demo_mode ? "Demo mode" : "Real data mode";
}

function setWeatherPill(metaPayload) {
  const el = document.getElementById("weatherPill");
  if (!el) return;
  el.classList.remove("ok", "warn", "bad");

  const summary = metaPayload?.meta?.external?.weather_collector ?? null;
  if (!summary || typeof summary !== "object") {
    el.classList.add("warn");
    el.textContent = "Weather: unavailable";
    el.title = "Weather collector heartbeat not found.";
    return;
  }

  const stale = Boolean(summary.stale);
  const rainy = Boolean(summary.is_rainy_now);
  const age = summary.heartbeat_age_s;
  const precip = summary.latest_observed_precip_mm;

  el.classList.add(stale ? "bad" : "ok");
  const ageTxt = age == null ? "—" : formatAge(age);
  const rainTxt = rainy ? ` · rain${precip != null ? ` ${Number(precip).toFixed(1)}mm` : ""}` : "";
  el.textContent = `Weather: ${stale ? "stale" : "ok"} · ${ageTxt}${rainTxt}`;
  el.title = (summary.commands ?? []).join("\n") || "";
}

function updateHud({ station, settings }) {
  const hudStation = document.getElementById("hudStation");
  const hudParams = document.getElementById("hudParams");
  hudStation.textContent = station ? `${station.name}` : "No station selected";

  const join =
    settings.join_method === "buffer"
      ? `buffer ${Math.round(settings.radius_m)}m`
      : `nearest k=${Math.round(settings.nearest_k)}`;
  hudParams.textContent = `gran=${settings.granularity} · ${join} · window=${settings.window_days}d`;
}

function setStatusText(text) {
  document.getElementById("statusText").textContent = text;
}

function pushAction({ level = "ok", title = "", message = "", actions = [] } = {}) {
  const root = document.getElementById("actionDrawer");
  if (!root) return;
  const el = document.createElement("div");
  const tone = ["ok", "warn", "bad"].includes(level) ? level : "ok";
  el.className = `action-toast ${tone}`;
  el.innerHTML = `
    <div class="action-toast-title">${title || "Action"}</div>
    <div class="action-toast-body">${message || ""}</div>
    <div class="action-toast-actions"></div>
  `;
  const actionsEl = el.querySelector(".action-toast-actions");
  for (const a of actions ?? []) {
    const b = document.createElement("button");
    b.className = `btn ${a.primary ? "btn-primary" : ""}`;
    b.textContent = a.label || "OK";
    b.addEventListener("click", () => {
      try {
        a.onClick?.();
      } finally {
        el.remove();
      }
    });
    actionsEl.appendChild(b);
  }
  root.prepend(el);
  // Keep it compact.
  while (root.childElementCount > 4) root.lastElementChild?.remove?.();
  setTimeout(() => el.remove(), 7000);
}

function updateStationMeta(station) {
  if (!station) {
    document.getElementById("stationName").textContent = "Select a station";
    document.getElementById("stationMeta").textContent = "";
    setRequestMetaText("");
    return;
  }
  document.getElementById("stationName").textContent = station.name;
  const metaParts = [
    station.id,
    station.city,
    station.district,
    station.source ? `src ${station.source}` : null,
    station.cluster == null ? null : `cluster ${station.cluster}`,
  ].filter(Boolean);
  document.getElementById("stationMeta").textContent = metaParts.join(" · ");
}

function pickSeries(timeseriesPayload, metric) {
  const series = timeseriesPayload?.series ?? [];
  if (metric === "auto") {
    const ridership = series.find((s) => s.metric === "metro_ridership");
    if (ridership && ridership.points?.length) return ridership;
    return series.find((s) => s.metric === "metro_flow_proxy_from_bike_rent") ?? null;
  }
  return series.find((s) => s.metric === metric) ?? null;
}

function toggleHidden(el, hide) {
  if (!el) return;
  el.classList.toggle("hidden", Boolean(hide));
}

function initSplitters(state) {
  const root = document.documentElement;
  const panels = loadStoredJson("metrobikeatlas.panels.v1") || {};
  const leftSize = panels.left_size ?? null;
  const rightSize = panels.right_size ?? null;
  if (typeof leftSize === "number") root.style.setProperty("--left-size", `${leftSize}px`);
  if (typeof rightSize === "number") root.style.setProperty("--right-size", `${rightSize}px`);

  function onDragSplitter(splitterId, side) {
    const splitter = document.getElementById(splitterId);
    if (!splitter) return;
    splitter.addEventListener("pointerdown", (e) => {
      splitter.setPointerCapture(e.pointerId);
      const startX = e.clientX;
      const startLeft = parseFloat(getComputedStyle(root).getPropertyValue("--left-size")) || 300;
      const startRight = parseFloat(getComputedStyle(root).getPropertyValue("--right-size")) || 380;

      function onMove(ev) {
        const dx = ev.clientX - startX;
        if (side === "left") {
          const next = clamp(startLeft + dx, 220, 600);
          root.style.setProperty("--left-size", `${next}px`);
          panels.left_size = next;
          storeJson("metrobikeatlas.panels.v1", panels);
        } else {
          const next = clamp(startRight - dx, 260, 700);
          root.style.setProperty("--right-size", `${next}px`);
          panels.right_size = next;
          storeJson("metrobikeatlas.panels.v1", panels);
        }
      }

      function onUp(ev) {
        splitter.releasePointerCapture(ev.pointerId);
        splitter.removeEventListener("pointermove", onMove);
        splitter.removeEventListener("pointerup", onUp);
      }

      splitter.addEventListener("pointermove", onMove);
      splitter.addEventListener("pointerup", onUp);
    });
  }

  onDragSplitter("splitterLeft", "left");
  onDragSplitter("splitterRight", "right");

  document.body.classList.toggle("left-collapsed", Boolean(state.settings.left_collapsed));
  document.body.classList.toggle("right-collapsed", Boolean(state.settings.right_collapsed));
}

function initHelpModal() {
  const modal = document.getElementById("helpModal");
  const btnHelp = document.getElementById("btnHelp");
  const btnClose = document.getElementById("btnCloseHelp");
  const backdrop = document.getElementById("helpBackdrop");

  function open() {
    modal.classList.remove("hidden");
  }
  function close() {
    modal.classList.add("hidden");
  }

  btnHelp.addEventListener("click", open);
  btnClose.addEventListener("click", close);
  backdrop.addEventListener("click", close);

  return { open, close, isOpen: () => !modal.classList.contains("hidden") };
}

function initOnboardingModal() {
  const modal = document.getElementById("onboardingModal");
  const btnClose = document.getElementById("btnCloseOnboarding");
  const backdrop = document.getElementById("onboardingBackdrop");
  const body = document.getElementById("onboardingBody");

  function open() {
    modal.classList.remove("hidden");
  }
  function close() {
    modal.classList.add("hidden");
    try {
      localStorage.setItem("metrobikeatlas.onboarding.dismissed.v1", "true");
    } catch {
      // ignore
    }
  }

  function shouldAutoOpen() {
    try {
      return localStorage.getItem("metrobikeatlas.onboarding.dismissed.v1") !== "true";
    } catch {
      return true;
    }
  }

  function render(status) {
    body.innerHTML = "";
    const alerts = status?.alerts ?? [];
    const required = ["metro_stations.csv", "bike_stations.csv", "metro_bike_links.csv"];
    const missing = (status?.silver_tables ?? [])
      .filter((t) => required.some((r) => String(t.path || "").endsWith(r)) && !t.exists)
      .map((t) => t.path);

    if (missing.length) {
      const div = document.createElement("div");
      div.innerHTML = `
        <div class="hint">Required Silver tables are missing.</div>
        <div class="mono">${missing.map((p) => `× ${p}`).join("<br/>")}</div>
        <div class="hint">Actions</div>
      `;
      body.appendChild(div);

      const actions = document.createElement("div");
      actions.className = "row row-actions";
      actions.innerHTML = `
        <button class="btn btn-primary" id="onboardStartCollector">Start collector</button>
        <button class="btn" id="onboardBuildSilver">Build Silver</button>
      `;
      body.appendChild(actions);

      actions.querySelector("#onboardStartCollector").addEventListener("click", () => {
        document.getElementById("btnStartCollector").click();
      });
      actions.querySelector("#onboardBuildSilver").addEventListener("click", () => {
        document.getElementById("btnBuildSilver").click();
      });
    }

    const sample = alerts.find((a) => (a.title || "").includes("sample data"));
    if (sample) {
      const div = document.createElement("div");
      div.className = "alert warning";
      div.innerHTML = `
        <div class="alert-title">${sample.title}</div>
        <div class="alert-message">${sample.message}</div>
      `;
      body.appendChild(div);
    }

    const cmds = alerts.flatMap((a) => a.commands ?? []);
    if (cmds.length) {
      const title = document.createElement("div");
      title.className = "hint";
      title.textContent = "Suggested commands";
      body.appendChild(title);
      for (const cmd of cmds.slice(0, 6)) {
        const row = document.createElement("div");
        row.className = "cmd";
        row.innerHTML = `<code class="mono"></code><button class="btn">Copy</button>`;
        row.querySelector("code").textContent = cmd;
        row.querySelector("button").addEventListener("click", async () => {
          try {
            await navigator.clipboard.writeText(cmd);
            setStatusText("Copied command");
          } catch {
            setStatusText("Copy failed");
          }
        });
        body.appendChild(row);
      }
    }
  }

  btnClose.addEventListener("click", close);
  backdrop.addEventListener("click", close);

  return { open, close, render, shouldAutoOpen, isOpen: () => !modal.classList.contains("hidden") };
}

function configureChartsTheme() {
  if (!window.Chart) return;
  Chart.defaults.font.family =
    'ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial';
  Chart.defaults.color = "#0f172a";
  Chart.defaults.borderColor = "rgba(15, 23, 42, 0.10)";
  Chart.defaults.elements.line.borderColor = "rgba(37, 99, 235, 0.95)";
  Chart.defaults.elements.line.borderWidth = 2;
  Chart.defaults.elements.point.radius = 0;
  Chart.defaults.plugins.legend.display = false;
}

function setNavActive(view) {
  const map = {
    home: "btnNavHome",
    explorer: "btnNavExplorer",
    insights: "btnNavInsights",
    ops: "btnNavOps",
    about: "btnNavAbout",
  };
  for (const [v, id] of Object.entries(map)) {
    const el = document.getElementById(id);
    if (!el) continue;
    const active = v === view;
    el.classList.toggle("active", active);
    el.setAttribute("aria-selected", active ? "true" : "false");
  }
}

function setAppView(view) {
  const v = view || "explorer";
  document.body.classList.toggle("view-home", v === "home");
  document.body.classList.toggle("view-explorer", v === "explorer");
  document.body.classList.toggle("view-insights", v === "insights");
  document.body.classList.toggle("view-ops", v === "ops");
  document.body.classList.toggle("view-about", v === "about");

  const briefing = document.getElementById("briefingPanel");
  const explorerPanel = document.getElementById("explorerPanel");
  const insightsPanel = document.getElementById("insightsPanel");
  const aboutPanel = document.getElementById("aboutPanel");
  if (briefing) briefing.classList.toggle("hidden", v !== "home");
  if (explorerPanel) explorerPanel.classList.toggle("hidden", v !== "explorer");
  if (insightsPanel) insightsPanel.classList.toggle("hidden", v !== "insights");
  if (aboutPanel) aboutPanel.classList.toggle("hidden", v !== "about");
  document.body.classList.toggle("mode-briefing", v === "home");

  if (v === "ops") {
    const details = document.getElementById("detailsDataStatus");
    if (details) details.open = true;
  }
  setNavActive(v);
}

function safeNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function quantile(values, q) {
  const arr = (values ?? []).map((v) => Number(v)).filter((v) => Number.isFinite(v));
  if (!arr.length) return null;
  arr.sort((a, b) => a - b);
  const pos = (arr.length - 1) * clamp(Number(q) || 0, 0, 1);
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  if (lo === hi) return arr[lo];
  const t = pos - lo;
  return arr[lo] * (1 - t) + arr[hi] * t;
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

function buildKpisFromStatus(status) {
  const h = status?.health ?? {};
  const kpis = [
    {
      label: "Collector",
      value: h.collector_running ? "Running" : "Stopped",
      tone: h.collector_running ? "ok" : "warn",
      meta: h.collector_pid ? `pid ${h.collector_pid}` : "",
    },
    {
      label: "Bronze freshness",
      value: formatAge(h.bronze_bike_availability_age_s),
      tone:
        h.bronze_bike_availability_age_s == null
          ? "muted"
          : Number(h.bronze_bike_availability_age_s) > 3600
            ? "warn"
            : "ok",
      meta: h.bronze_bike_availability_last_utc ? fmtTs(h.bronze_bike_availability_last_utc) : "",
    },
    {
      label: "Silver freshness",
      value: formatAge(
        Math.min(
          Number(h.silver_metro_bike_links_age_s ?? Infinity),
          Number(h.silver_bike_timeseries_age_s ?? Infinity)
        )
      ),
      tone:
        h.silver_metro_bike_links_age_s == null && h.silver_bike_timeseries_age_s == null
          ? "muted"
          : Math.max(Number(h.silver_metro_bike_links_age_s ?? 0), Number(h.silver_bike_timeseries_age_s ?? 0)) > 86400
            ? "warn"
            : "ok",
      meta: "links/bike_timeseries",
    },
  ];

  if (Number(h.metro_tdx_404_count || 0) > 0) {
    kpis.push({
      label: "Metro TDX 404",
      value: String(h.metro_tdx_404_count),
      tone: "bad",
      meta: h.metro_tdx_404_last_utc ? `last ${fmtTs(h.metro_tdx_404_last_utc)}` : "",
    });
  }

  return kpis;
}

function generatePolicyCards({ status, station, timeseries, heatByStation }) {
  const cards = [];
  const h = status?.health ?? {};
  const demo = Boolean(status?.demo_mode);
  const metro404 = Number(h.metro_tdx_404_count || 0) > 0;

  if (!demo && metro404) {
    cards.push({
      title: "修復資料來源：Metro stations 不可用（404）",
      impact: "避免 station layer/links 缺失導致分析無法串接",
      beneficiaries: "決策者、資料團隊、前端使用者",
      risk: "短期需採 fallback；長期需權限/路徑確認",
      needs: "external metro_stations.csv 或調整 TDX dataset/path",
      actions: [
        { label: "Go to Ops page", primary: true, type: "nav", view: "ops" },
        { label: "Open onboarding", type: "onboarding" },
      ],
    });
  }

  const ts = timeseries;
  const series = ts?.series ?? [];
  const rent = series.find((s) => s.metric === "bike_rent_proxy_total");
  const ret = series.find((s) => s.metric === "bike_return_proxy_total");
  const available = series.find((s) => s.metric === "bike_available_bikes_total");
  const lastRent = rent?.points?.length ? safeNum(rent.points[rent.points.length - 1].value) : null;
  const lastReturn = ret?.points?.length ? safeNum(ret.points[ret.points.length - 1].value) : null;
  const lastAvail = available?.points?.length ? safeNum(available.points[available.points.length - 1].value) : null;

  if (station && lastRent != null && lastAvail != null && lastRent > Math.max(lastAvail * 0.15, 5)) {
    cards.push({
      title: "增補供給：高租借壓力站點（疑似供給不足）",
      impact: "降低尖峰缺車、改善轉乘體驗",
      beneficiaries: "通勤族、轉乘旅客",
      risk: "若只是短時段尖峰，固定增補可能造成離峰閒置",
      needs: "更長時間序列（至少 1–2 週）、尖峰時段拆解",
      actions: [
        { label: `Focus ${station.name}`, primary: true, type: "focus_station", station_id: station.id },
        { label: "Heat: rent_proxy", type: "set_heat", metric: "rent_proxy", agg: "sum" },
      ],
    });
  }

  if (station && lastReturn != null && lastAvail != null && lastReturn > Math.max(lastAvail * 0.15, 5)) {
    cards.push({
      title: "疏導回流：高歸還壓力站點（疑似滿柱/滿車架風險）",
      impact: "降低無法還車、改善步行轉乘成本",
      beneficiaries: "通勤族、夜間返家族群",
      risk: "需要配合營運調度；可能受活動/天候影響",
      needs: "回流熱點清單、尖峰時段、周邊替代站點",
      actions: [
        { label: `Focus ${station.name}`, primary: true, type: "focus_station", station_id: station.id },
        { label: "Heat: return_proxy", type: "set_heat", metric: "return_proxy", agg: "sum" },
      ],
    });
  }

  const holidayEff = timeseries?.meta?.holiday_effect ?? null;
  if (holidayEff?.pct != null) {
    const pct = safeNum(holidayEff.pct);
    if (pct != null && Math.abs(pct) >= 0.1) {
      cards.push({
        title: "假日效應：需求型態不同（調度/引導可拆分）",
        impact: "假日/平日分流調度，避免用同一套策略誤判",
        beneficiaries: "週末旅遊族群、商圈活動周邊使用者",
        risk: "需要 calendar 資料維護；活動日可能比假日更敏感",
        needs: "calendar.csv（假日標記）＋至少 2 週序列",
        actions: [
          { label: "Open Evidence", primary: true, type: "story_step", step: "evidence" },
          { label: "Build Silver", type: "admin", action: "build_silver" },
        ],
      });
    }
  }

  const rainEff = timeseries?.meta?.rain_effect ?? null;
  if (rainEff?.pct != null) {
    const pct = safeNum(rainEff.pct);
    if (pct != null && Math.abs(pct) >= 0.1) {
      cards.push({
        title: "天候效應：雨天需求變化（備援供給/導引）",
        impact: "雨天預先部署或轉乘導引，提高服務韌性",
        beneficiaries: "通勤族、轉乘旅客",
        risk: "天候資料需穩定；區域降雨差異大",
        needs: "weather_hourly.csv（雨量）＋門檻與對照分析",
        actions: [
          { label: "Heat: rent_proxy", primary: true, type: "set_heat", metric: "rent_proxy", agg: "sum" },
          { label: "Open Explorer", type: "nav", view: "explorer" },
        ],
      });
    }
  }

  const heatVals = Array.from((heatByStation ?? new Map()).values());
  const q90 = quantile(heatVals, 0.9);
  const q10 = quantile(heatVals, 0.1);
  if (q90 != null && q10 != null && q90 > Math.max(q10 * 2, 10)) {
    cards.push({
      title: "調度時段：供給/需求落差大（熱點/冷點）",
      impact: "以時間段調度降低不均，提高整體服務水平",
      beneficiaries: "全體使用者、營運單位",
      risk: "調度成本增加；需避免干擾道路/鄰里",
      needs: "熱點清單、尖離峰拆解、調度資源盤點",
      actions: [
        { label: "Show hotspot list", primary: true, type: "story_step", step: "insight" },
        { label: "Heat: available", type: "set_heat", metric: "available", agg: "sum" },
      ],
    });
  }

  if (!cards.length) {
    cards.push({
      title: "先把資料跑穩：再談政策溝通",
      impact: "提高數據新鮮度與可追溯性，支持跨部門對話",
      beneficiaries: "決策者、資料團隊、營運團隊",
      risk: "資料不足時容易誤判",
      needs: "collector 長跑、Silver 定時更新、jobs logs",
      actions: [
        { label: "Start collector", primary: true, type: "admin", action: "start_collector" },
        { label: "Build Silver", type: "admin", action: "build_silver" },
      ],
    });
  }

  return cards.slice(0, 4);
}

function renderBriefing(status, state, { onboarding } = {}) {
  const kpisEl = document.getElementById("briefingKpis");
  const readinessEl = document.getElementById("briefingReadiness");
  const calloutsEl = document.getElementById("briefingCallouts");
  const homeConclusionEl = document.getElementById("homeConclusion");
  const homeActionsEl = document.getElementById("homeActions");
  const homeRainUsageEl = document.getElementById("homeRainUsageCard");
  const homeRainRiskEl = document.getElementById("homeRainRiskCard");
  const stepperEl = document.getElementById("briefingStepper");
  const stepTitleEl = document.getElementById("briefingStepTitle");
  const stepConclusionEl = document.getElementById("briefingStepConclusion");
  const stepActionEl = document.getElementById("briefingStepAction");
  const vizTitleEl = document.getElementById("briefingVizTitle");
  const vizMetaEl = document.getElementById("briefingVizMeta");
  const policyCardsEl = document.getElementById("policyCards");
  if (
    !kpisEl ||
    !readinessEl ||
    !calloutsEl ||
    !homeConclusionEl ||
    !homeActionsEl ||
    !homeRainUsageEl ||
    !homeRainRiskEl ||
    !stepperEl ||
    !stepTitleEl ||
    !stepConclusionEl ||
    !stepActionEl ||
    !vizTitleEl ||
    !vizMetaEl ||
    !policyCardsEl
  )
    return;

  const kpis = buildKpisFromStatus(status);
  kpisEl.innerHTML = "";
  for (const k of kpis) {
    const el = document.createElement("div");
    el.className = `briefing-kpi ${k.tone || ""}`;
    el.tabIndex = 0;
    el.setAttribute("role", "button");
    el.innerHTML = `
      <div class="briefing-kpi-label">${k.label}</div>
      <div class="briefing-kpi-value">${k.value}</div>
      <div class="briefing-kpi-meta mono">${k.meta || ""}</div>
    `;
    el.addEventListener("click", () => {
      const label = String(k.label || "").toLowerCase();
      if (label.includes("bronze")) document.body.dispatchEvent(new CustomEvent("nav", { detail: { view: "ops", anchor: "bronze" } }));
      if (label.includes("silver")) document.body.dispatchEvent(new CustomEvent("nav", { detail: { view: "ops", anchor: "silver" } }));
      if (label.includes("404")) document.body.dispatchEvent(new CustomEvent("nav", { detail: { view: "ops" } }));
    });
    kpisEl.appendChild(el);
  }

  const missing = (status?.alerts ?? []).some((a) => String(a.title || "").toLowerCase().includes("silver missing"));
  const demo = Boolean(status?.demo_mode);
  const lines = [];
  if (demo) lines.push("Demo mode：使用示範資料，可直接探索流程與 UI。");
  else lines.push("Real data mode：使用本機 Bronze/Silver 產物支撐分析。");
  if (missing) lines.push("目前 Silver 缺檔：請先建立 Silver（可在此頁一鍵啟動）。");
  readinessEl.textContent = lines.join(" ");

  // Home executive summary (story-first; avoids "engineering dashboard" feel)
  const h = status?.health ?? {};
  const bronzeAge = h.bronze_bike_availability_age_s != null ? formatAge(h.bronze_bike_availability_age_s) : "—";
  const silverAge =
    h.silver_metro_bike_links_age_s != null || h.silver_bike_timeseries_age_s != null
      ? formatAge(
          Math.min(
            Number(h.silver_metro_bike_links_age_s ?? Infinity),
            Number(h.silver_bike_timeseries_age_s ?? Infinity)
          )
        )
      : "—";
  const metro404 = Number(h.metro_tdx_404_count ?? 0) || 0;
  const weather = state?.weatherSummary ?? null;
  const rainingNow = Boolean(weather?.is_rainy_now);
  const weatherAge = weather?.heartbeat_age_s != null ? formatAge(weather.heartbeat_age_s) : "—";

  const today = [];
  today.push(demo ? "Demo mode" : "Real mode");
  today.push(`Bronze ${bronzeAge}`);
  today.push(`Silver ${silverAge}`);
  if (weather) today.push(`Weather ${weather?.stale ? "stale" : "ok"} (${weatherAge})${rainingNow ? " · raining now" : ""}`);
  if (metro404) today.push(`Metro 404 ×${metro404}`);
  if (missing) today.push("Silver missing");
  homeConclusionEl.textContent = today.join(" · ");

  homeActionsEl.innerHTML = "";
  const addActionBtn = (label, onClick, { primary = false } = {}) => {
    const b = document.createElement("button");
    b.className = `btn ${primary ? "btn-primary" : ""}`;
    b.textContent = label;
    b.addEventListener("click", onClick);
    homeActionsEl.appendChild(b);
  };
  addActionBtn("Open Explorer", () => document.body.dispatchEvent(new CustomEvent("nav", { detail: { view: "explorer" } })), {
    primary: true,
  });
  addActionBtn("Open Ops", () => document.body.dispatchEvent(new CustomEvent("nav", { detail: { view: "ops" } })));
  addActionBtn("Toggle heat", () => {
    const el = document.getElementById("toggleHeat");
    if (!el) return;
    el.checked = !el.checked;
    el.dispatchEvent(new Event("change", { bubbles: true }));
  });
  addActionBtn("Enable rain mode", () => {
    const el = document.getElementById("toggleRainMode");
    if (!el) return;
    el.checked = true;
    el.dispatchEvent(new Event("change", { bubbles: true }));
  });

  // Story card: Rain × Usage (24h)
  const homeUsage = state?.weatherUsage ?? null;
  if (homeUsage?.precip_total_mm != null && homeUsage?.rent_proxy_total != null) {
    const p = Number(homeUsage.precip_total_mm);
    const r = Number(homeUsage.rent_proxy_total);
    const ret2 = Number(homeUsage.return_proxy_total);
    const rainyHours = homeUsage.rainy_hours;
    const city = homeUsage.city || "—";
    homeRainUsageEl.textContent =
      `${city} · ` +
      `${p > 0 ? `rain ${p.toFixed(1)}mm` : "no rain"}` +
      `${rainyHours != null ? ` · rainy hours ${rainyHours}` : ""}` +
      ` · rent_proxy ${Number.isFinite(r) ? Math.round(r) : "—"}` +
      ` · return_proxy ${Number.isFinite(ret2) ? Math.round(ret2) : "—"}`;
  } else {
    homeRainUsageEl.textContent = "Weather usage insight not available yet.";
  }

  // Story card: Rain risk stations (now)
  const homeRisk = state?.rainRisk ?? null;
  homeRainRiskEl.innerHTML = "";
  if (homeRisk?.is_rainy_now && Array.isArray(homeRisk.items) && homeRisk.items.length) {
    const top = homeRisk.items.slice(0, 5);
    const hint = document.createElement("div");
    hint.className = "hint";
    hint.textContent = `Raining now · Top ${top.length} by low nearby availability (click to open in Explorer)`;
    homeRainRiskEl.appendChild(hint);
    const row = document.createElement("div");
    row.className = "row row-actions";
    for (const it of top) {
      const b = document.createElement("button");
      b.className = "btn";
      const val = Number(it.mean_available_bikes);
      b.textContent = `${it.name || it.station_id} · ${Number.isFinite(val) ? Math.round(val) : "—"}`;
      b.addEventListener("click", () =>
        document.body.dispatchEvent(new CustomEvent("focus_station", { detail: { station_id: it.station_id } }))
      );
      row.appendChild(b);
    }
    homeRainRiskEl.appendChild(row);
  } else {
    const msg = document.createElement("div");
    msg.className = "hint";
    msg.textContent = rainingNow ? "Raining now, but rain-risk list is empty." : "Not raining now.";
    homeRainRiskEl.appendChild(msg);
  }

  // Policy cards
  const station = state?.stationById?.get?.(state?.selectedStationId) ?? null;
  const cards = generatePolicyCards({
    status,
    station,
    timeseries: state?.lastTimeseries,
    heatByStation: state?.heatByStation,
  });
  policyCardsEl.innerHTML = "";
  for (const c of cards) {
    const el = document.createElement("div");
    el.className = "policy-card";
    el.innerHTML = `
      <div class="policy-card-title">${c.title}</div>
      <div class="policy-card-meta">
        <div><span class="mono">Impact</span> · ${c.impact}</div>
        <div><span class="mono">Beneficiaries</span> · ${c.beneficiaries}</div>
        <div><span class="mono">Risk</span> · ${c.risk}</div>
        <div><span class="mono">Needs</span> · ${c.needs}</div>
      </div>
      <div class="policy-card-actions"></div>
    `;
    const actions = el.querySelector(".policy-card-actions");
    for (const a of c.actions ?? []) {
      const b = document.createElement("button");
      b.className = `btn ${a.primary ? "btn-primary" : ""}`;
      b.textContent = a.label;
      b.addEventListener("click", () => {
        if (a.type === "nav") document.body.dispatchEvent(new CustomEvent("nav", { detail: { view: a.view } }));
        if (a.type === "onboarding") onboarding?.open?.();
        if (a.type === "admin") {
          if (a.action === "start_collector") document.getElementById("btnStartCollector").click();
          if (a.action === "build_silver") document.getElementById("btnBuildSilver").click();
        }
        if (a.type === "focus_station" && a.station_id) {
          document.body.dispatchEvent(new CustomEvent("focus_station", { detail: { station_id: a.station_id } }));
        }
        if (a.type === "set_heat") {
          document.body.dispatchEvent(new CustomEvent("set_heat", { detail: { metric: a.metric, agg: a.agg } }));
        }
        if (a.type === "story_step") {
          document.body.dispatchEvent(new CustomEvent("story_step", { detail: { step: a.step } }));
        }
      });
      actions.appendChild(b);
    }
    policyCardsEl.appendChild(el);
  }

  // Story stepper (problem → evidence → insight → options → next)
  const steps = [
    { id: "problem", label: "問題" },
    { id: "evidence", label: "證據" },
    { id: "insight", label: "洞察" },
    { id: "options", label: "政策選項" },
    { id: "next", label: "下一步" },
  ];
  const active = state?.settings?.story_step ?? "problem";
  stepperEl.innerHTML = "";
  for (const s of steps) {
    const b = document.createElement("button");
    b.className = `btn step-btn ${s.id === active ? "active" : ""}`;
    b.textContent = s.label;
    b.addEventListener("click", () => document.body.dispatchEvent(new CustomEvent("story_step", { detail: { step: s.id } })));
    stepperEl.appendChild(b);
  }

  // Per-step content
  const ts = state?.lastTimeseries ?? null;
  const series = ts?.series ?? [];
  const avail = series.find((s) => s.metric === "bike_available_bikes_total");
  const rent = series.find((s) => s.metric === "bike_rent_proxy_total");
  const ret = series.find((s) => s.metric === "bike_return_proxy_total");
  const selectedName = station?.name ?? "（未選站）";

  stepActionEl.innerHTML = "";
  if (active === "problem") {
    stepTitleEl.textContent = "問題：哪些 MRT 站附近共享單車供需失衡？";
    stepConclusionEl.textContent =
      "我們以站點周邊共享單車可用量與 proxy（租借/歸還壓力）作為可解釋的指標，快速定位「缺車」或「滿柱」風險站點。";
    vizTitleEl.textContent = "Evidence · Heat snapshot";
    vizMetaEl.textContent = "建議：先開啟 heat layer，或選一個站點看 time series。";
  } else if (active === "evidence") {
    stepTitleEl.textContent = `證據：${selectedName} 的時間序列`;
    stepConclusionEl.textContent =
      station ? "用時間序列看「可用車」與「租借壓力」是否同時上升，並找出尖峰時段。" : "先點地圖選一個站點，才能看到對應的證據圖。";
    vizTitleEl.textContent = "Evidence · bike_available_bikes_total";
    vizMetaEl.textContent = ts?.meta?.silver_artifacts?.find?.((a) => a.path?.includes?.("bike_timeseries"))?.mtime_utc
      ? `last updated ${fmtTs(ts.meta.silver_artifacts.find((a) => a.path.includes("bike_timeseries")).mtime_utc)}`
      : "";
    const btn = document.createElement("button");
    btn.className = "btn btn-primary";
    btn.textContent = "Open Explorer";
    btn.addEventListener("click", () => document.body.dispatchEvent(new CustomEvent("nav", { detail: { view: "explorer" } })));
    stepActionEl.appendChild(btn);
  } else if (active === "insight") {
    stepTitleEl.textContent = "洞察：熱點/冷點清單（可點選定位）";
    stepConclusionEl.textContent =
      "把地圖的 heat 值排序後，快速列出熱點（高壓）與冷點（低壓），用於內部討論與外部溝通的「故事證據」。";
    vizTitleEl.textContent = "Evidence · rent_proxy / return_proxy";
    vizMetaEl.textContent = "提示：可切換 heat metric（rent_proxy / return_proxy）看不同壓力型態。";
  } else if (active === "options") {
    stepTitleEl.textContent = "政策選項：把數據翻成可執行的策略卡";
    stepConclusionEl.textContent =
      "策略卡聚焦在可操作性：影響範圍、受益族群、風險、與還需要哪些資料。";
    vizTitleEl.textContent = "Evidence · policy options";
    vizMetaEl.textContent = "下方已自動生成 2–4 張策略卡。";
    const btn = document.createElement("button");
    btn.className = "btn";
    btn.textContent = "Jump to policy cards";
    btn.addEventListener("click", () => policyCardsEl.scrollIntoView({ behavior: "smooth", block: "start" }));
    stepActionEl.appendChild(btn);
  } else {
    stepTitleEl.textContent = "下一步：資料穩定 → 指標對齊 → 對外簡報";
    stepConclusionEl.textContent =
      "建議先確保資料管線穩定（collector + Silver），再定義可對外說清楚的 KPI 與介入方案；需要時可一鍵匯出 brief 素材。";
    vizTitleEl.textContent = "Next · actions";
    vizMetaEl.textContent = "";
    const a1 = document.createElement("button");
    a1.className = "btn btn-primary";
    a1.textContent = "Start collector";
    a1.addEventListener("click", () => document.getElementById("btnStartCollector").click());
    const a2 = document.createElement("button");
    a2.className = "btn";
    a2.textContent = "Build Silver";
    a2.addEventListener("click", () => document.getElementById("btnBuildSilver").click());
    stepActionEl.appendChild(a1);
    stepActionEl.appendChild(a2);
  }

  // Insight: render hotspot list as action buttons
  if (active === "insight") {
    // Prefer backend-driven insight list when available.
    const hot = (state?.lastHotspots?.hot ?? []).slice(0, 5).map((h) => ({ id: h.station_id, name: h.name || h.station_id, value: h.value }));
    const cold = (state?.lastHotspots?.cold ?? []).slice(0, 5).map((h) => ({ id: h.station_id, name: h.name || h.station_id, value: h.value }));
    const makeList = (title, items) => {
      const box = document.createElement("div");
      box.className = "status-block";
      box.innerHTML = `<div class="hint">${title}</div>`;
      for (const it of items) {
        const b = document.createElement("button");
        b.className = "btn";
        b.textContent = `${it.name} · ${Math.round(it.value)}`;
        b.addEventListener("click", () => document.body.dispatchEvent(new CustomEvent("focus_station", { detail: { station_id: it.id } })));
        box.appendChild(b);
      }
      return box;
    };
    stepActionEl.appendChild(makeList("Hotspots", hot));
    stepActionEl.appendChild(makeList("Coldspots", cold));
  }

  // Briefing chart render (single series)
  const chart = state?.briefingChart ?? null;
  if (chart) {
    let points = [];
    if (active === "evidence" && avail?.points?.length) points = avail.points;
    else if (active === "insight" && rent?.points?.length) points = rent.points;
    else if (active === "insight" && ret?.points?.length) points = ret.points;
    else points = [];
    setChartData(chart, points);
  }

  const alerts = (status?.alerts ?? []).slice(0, 3);
  const effects = [];
  if (ts?.meta?.holiday_effect?.pct != null) {
    const pct = Number(ts.meta.holiday_effect.pct);
    if (Number.isFinite(pct)) {
      effects.push({
        level: Math.abs(pct) >= 0.2 ? "warning" : "info",
        title: "假日效應",
        message: `假日平均 ${Math.round(pct * 100)}%（n=${ts.meta.holiday_effect.n_holiday}/${ts.meta.holiday_effect.n_non_holiday}）`,
        commands: [],
      });
    }
  }
  if (ts?.meta?.rain_effect?.pct != null) {
    const pct = Number(ts.meta.rain_effect.pct);
    if (Number.isFinite(pct)) {
      effects.push({
        level: Math.abs(pct) >= 0.2 ? "warning" : "info",
        title: "雨天效應",
        message: `雨天平均 ${Math.round(pct * 100)}%（city=${ts.meta.rain_effect.city} threshold=${ts.meta.rain_effect.precip_threshold_mm}mm）`,
        commands: [],
      });
    }
  }
  const weatherCards = [];
  const ws = state?.weatherSummary ?? null;
  if (ws?.stale) {
    weatherCards.push({
      level: "warning",
      title: "Weather stale",
      message: `Weather data is stale (age ${formatAge(ws.heartbeat_age_s)}).`,
      commands: (ws.commands ?? ["docker compose up -d weather"]).slice(0, 1),
    });
  }

  const usage = state?.weatherUsage ?? null;
  if (usage && usage.precip_total_mm != null && usage.rent_proxy_total != null) {
    const p = Number(usage.precip_total_mm);
    const r = Number(usage.rent_proxy_total);
    const ret2 = Number(usage.return_proxy_total);
    const rainyHours = usage.rainy_hours;
    weatherCards.push({
      level: p > 0 ? "info" : "info",
      title: "最近 24h · 降雨 × 借還 proxy",
      message:
        `${p > 0 ? `Rain ${p.toFixed(1)}mm` : "No rain"}${rainyHours != null ? ` (rainy hours ${rainyHours})` : ""} · ` +
        `rent_proxy ${Math.round(r)} · return_proxy ${Math.round(ret2)}`,
      commands: [],
    });
  }

  const risk = state?.rainRisk ?? null;
  if (risk?.is_rainy_now && Array.isArray(risk.items) && risk.items.length) {
    weatherCards.push({
      level: "warning",
      title: "雨天風險站點（現在）",
      message: `Top ${Math.min(risk.items.length, 5)} stations by low nearby bike availability.`,
      commands: [],
      stations: risk.items.slice(0, 5).map((it) => ({ id: it.station_id, name: it.name || it.station_id, meta: it.mean_available_bikes })),
    });
  }

  const allCallouts = [...weatherCards, ...effects, ...alerts].slice(0, 5);
  calloutsEl.innerHTML = "";
  for (const a of allCallouts) {
    const div = document.createElement("div");
    const level = (a.level || "info").toLowerCase();
    div.className = `briefing-callout ${level}`;
    div.innerHTML = `
      <div class="briefing-callout-title">${a.title || "Note"}</div>
      <div class="briefing-callout-body">${a.message || ""}</div>
    `;
    const cmds = a.commands ?? [];
    if (cmds.length) {
      const cmd = cmds[0];
      const row = document.createElement("div");
      row.className = "briefing-cmd";
      row.innerHTML = `<code class="mono"></code><button class="btn">Copy</button>`;
      row.querySelector("code").textContent = cmd;
      row.querySelector("button").addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(cmd);
          setStatusText("Copied command");
        } catch {
          setStatusText("Copy failed");
        }
      });
      div.appendChild(row);
    }

    if (Array.isArray(a.stations) && a.stations.length) {
      const box = document.createElement("div");
      box.className = "row row-actions";
      for (const it of a.stations.slice(0, 5)) {
        const b = document.createElement("button");
        b.className = "btn";
        const metaTxt = it.meta == null ? "" : ` · ${Number(it.meta).toFixed(1)}`;
        b.textContent = `${it.name}${metaTxt}`;
        b.addEventListener("click", () =>
          document.body.dispatchEvent(new CustomEvent("focus_station", { detail: { station_id: it.id } }))
        );
        box.appendChild(b);
      }
      div.appendChild(box);
    }
    calloutsEl.appendChild(div);
  }
}

async function main() {
  const cfg = await fetchJson("/config");
  setModePill(cfg);
  document.getElementById("appTitle").textContent = cfg.app_name;

  const storedSettings = loadStoredJson("metrobikeatlas.settings.v1");
  const settings = mergeSettings(defaultSettingsFromConfig(cfg), storedSettings);

  const state = {
    cfg,
    settings,
    stations: [],
    stationById: new Map(),
    selectedStationId: null,
    lastTimeseries: null,
    lastNearby: null,
    lastStatusSnapshot: null,
    jobs: [],
    liveTimer: null,
    statusTimer: null,
    aborter: null,
    selectedMarker: null,
    briefingChart: null,
    heatIndex: [],
    heatByStation: new Map(),
    lastHotspots: null,
    weatherUsage: null,
    rainRisk: null,
    weatherSummary: null,
  };
  // Allow small UI helpers outside `main()` (e.g. status panel) to read current meta.
  window.__mba_state = state;

  // Apply permalink hash overrides (if present).
  const hash = parseHashParams();
  const initialStationId = hash.station_id ? String(hash.station_id) : null;
  const guided = String(hash.guided || "") === "1";
  const guidedKind = hash.guided_kind ? String(hash.guided_kind) : null;
  const guidedTitle = hash.guided_title ? String(hash.guided_title) : null;
  const patch = {};
  for (const key of [
    "join_method",
    "radius_m",
    "nearest_k",
    "granularity",
    "timezone",
    "window_days",
    "metro_metric",
    "bike_metric",
    "heat_metric",
    "heat_agg",
  ]) {
    if (hash[key] != null && String(hash[key]).trim() !== "") patch[key] = hash[key];
  }
  if (hash.show_bike_heat != null) patch.show_bike_heat = String(hash.show_bike_heat) === "1";
  if (hash.heat_follow_latest != null) patch.heat_follow_latest = String(hash.heat_follow_latest) === "1";
  if (patch.radius_m != null) patch.radius_m = Number(patch.radius_m);
  if (patch.nearest_k != null) patch.nearest_k = Number(patch.nearest_k);
  if (patch.window_days != null) patch.window_days = Number(patch.window_days);
  state.settings = mergeSettings(state.settings, patch);

  configureChartsTheme();

  // Explorer is a dedicated page now; keep view fixed here.
  state.settings.app_view = "explorer";
  persistSettings();
  setAppView("explorer");

  // Page navigation (multi-page, not tabbed panels).
  document.getElementById("btnNavHome")?.addEventListener("click", () => (window.location.href = "/home"));
  document.getElementById("btnNavExplorer")?.addEventListener("click", () => (window.location.href = "/explorer"));
  document.getElementById("btnNavInsights")?.addEventListener("click", () => (window.location.href = "/insights"));
  document.getElementById("btnNavOps")?.addEventListener("click", () => (window.location.href = "/ops"));
  document.getElementById("btnNavAbout")?.addEventListener("click", () => (window.location.href = "/about"));
  document.body.addEventListener("focus_station", (ev) => {
    const id = ev.detail?.station_id;
    if (!id) return;
    selectStationById(id, { focus: true });
    refreshSelectedStation({ reason: "briefing" });
  });
  document.body.addEventListener("set_heat", async (ev) => {
    const metric = ev.detail?.metric;
    const agg = ev.detail?.agg;
    if (metric) state.settings.heat_metric = metric;
    if (agg) state.settings.heat_agg = agg;
    persistSettings();
    applySettingsToControls();
    if (state.settings.show_bike_heat) await refreshHeatAtIndex(document.getElementById("heatTimeRange").value);
  });
  document.body.addEventListener("story_step", (ev) => {
    const step = ev.detail?.step;
    if (!step) return;
    state.settings.story_step = step;
    persistSettings();
    if (state.settings.app_view === "home" && state.lastStatusSnapshot) {
      renderBriefing(state.lastStatusSnapshot, state, { onboarding });
    }
  });

  document.getElementById("btnBriefingStartCollector")?.addEventListener("click", () =>
    document.getElementById("btnStartCollector")?.click()
  );
  document.getElementById("btnBriefingBuildSilver")?.addEventListener("click", () =>
    document.getElementById("btnBuildSilver")?.click()
  );
  document.getElementById("btnBriefingCopy")?.addEventListener("click", async () => {
    const status = state.lastStatusSnapshot ?? null;
    const station = state.stationById.get(state.selectedStationId) ?? null;
    const lines = [];
    lines.push(`MetroBikeAtlas brief (${new Date().toISOString()})`);
    if (status?.health) {
      const h = status.health;
      lines.push(`Collector: ${h.collector_running ? "running" : "stopped"}`);
      lines.push(`Bronze age: ${formatAge(h.bronze_bike_availability_age_s)}`);
      lines.push(`Silver age: ${formatAge(Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity)))}`);
      if (h.metro_tdx_404_count) lines.push(`Metro 404: ${h.metro_tdx_404_count}`);
    }
    if (station) lines.push(`Station: ${station.name} (${station.id})`);
    if (state.lastTimeseries?.meta?.resolved) lines.push(`Resolved: ${JSON.stringify(state.lastTimeseries.meta.resolved)}`);
    try {
      await navigator.clipboard.writeText(lines.join("\n"));
      setStatusText("Copied brief");
    } catch {
      setStatusText("Copy failed");
    }
  });
  document.getElementById("btnBriefingSave")?.addEventListener("click", () => saveSnapshot());
  document.getElementById("btnBriefingDownload")?.addEventListener("click", () => downloadBriefingExport("zip"));

  async function refreshSnapshots() {
    const select = document.getElementById("snapshotSelect");
    if (!select) return;
    try {
      const snaps = await adminFetchJson("/briefing/snapshots?limit=30");
      select.innerHTML = "";
      select.appendChild(new Option("Select a snapshot…", ""));
      for (const s of snaps ?? []) {
        const label = `${(s.created_at_utc || "").replace("T", " ").slice(0, 19)} · ${(s.snapshot?.station_id || "—")}`;
        select.appendChild(new Option(label, s.id));
      }
    } catch {
      select.innerHTML = "";
      select.appendChild(new Option("Snapshots unavailable (localhost-only)", ""));
    }
  }

  function applySnapshot(snap) {
    if (!snap?.snapshot) return;
    const payload = snap.snapshot;
    if (payload.settings && typeof payload.settings === "object") {
      state.settings = { ...state.settings, ...payload.settings };
      persistSettings();
      applySettingsToControls();
    }
    if (payload.story_step) {
      state.settings.story_step = payload.story_step;
      persistSettings();
    }
    if (payload.station_id) {
      selectStationById(payload.station_id, { focus: true });
      refreshSelectedStation({ reason: "snapshot" });
    }
    if (state.lastStatusSnapshot) renderBriefing(state.lastStatusSnapshot, state, { onboarding });
  }

  document.getElementById("btnSnapshotRefresh")?.addEventListener("click", () => refreshSnapshots());
  document.getElementById("btnSnapshotLoad")?.addEventListener("click", async () => {
    const select = document.getElementById("snapshotSelect");
    const id = select?.value;
    if (!id) return;
    try {
      const snaps = await adminFetchJson("/briefing/snapshots?limit=30");
      const hit = (snaps ?? []).find((s) => s.id === id);
      if (hit) applySnapshot(hit);
    } catch (e) {
      setStatusText(`Load failed: ${e.message}`);
    }
  });

  // Export pack from backend (zip/json)
  async function downloadBriefingExport(fmt) {
    const station = state.stationById.get(state.selectedStationId) ?? null;
    const params = {
      station_id: station?.id ?? "",
      join_method: state.settings.join_method,
      radius_m: state.settings.join_method === "buffer" ? state.settings.radius_m : null,
      nearest_k: state.settings.join_method === "nearest" ? state.settings.nearest_k : null,
      granularity: state.settings.granularity,
      timezone: state.settings.timezone,
      window_days: state.settings.window_days,
      heat_metric: state.settings.heat_metric,
      heat_agg: state.settings.heat_agg,
      format: fmt,
    };
    const url = `/briefing/export${qs(params)}`;
    window.open(url, "_blank");
  }

  // Persist snapshots (localhost-only backend).
  async function saveSnapshot() {
    try {
      const status = state.lastStatusSnapshot ?? null;
      const station = state.stationById.get(state.selectedStationId) ?? null;
      const body = {
        station_id: station?.id ?? null,
        story_step: state.settings.story_step ?? null,
        kpis: buildKpisFromStatus(status),
        settings: { ...state.settings },
        artifacts: {
          status: status,
          timeseries_meta: state.lastTimeseries?.meta ?? null,
          nearby_meta: state.lastNearby?.meta ?? null,
        },
        policy_cards: [],
      };
      const res = await adminPostJson("/briefing/snapshots", body);
      setStatusText(`Snapshot saved (${res?.id?.slice?.(0, 8) || ""})`);
    } catch (e) {
      setStatusText(`Snapshot failed: ${e.message}`);
    }
  }

  initSplitters(state);
  const help = initHelpModal();
  const onboarding = initOnboardingModal();

  const map = L.map("map", { zoomControl: true, keyboard: false }).setView(
    [cfg.web_map.center_lat, cfg.web_map.center_lon],
    cfg.web_map.zoom
  );
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(map);

  const metroLayer = L.layerGroup().addTo(map);
  const bikeLayer = L.layerGroup().addTo(map);
  const linkLayer = L.layerGroup().addTo(map);
  let bufferCircle = null;
  const heatLegend = document.getElementById("heatLegend");
  const heatMeta = document.getElementById("heatMeta");
  const mapLegend = document.getElementById("mapLegend");

	  function setOverlayVisible(visible, text) {
	    const el = document.getElementById("loadingOverlay");
	    const t = document.getElementById("overlayText");
	    if (t && text) t.textContent = text;
	    el.classList.toggle("hidden", !visible);
	    el.setAttribute("aria-hidden", visible ? "false" : "true");
	    el.hidden = !visible;
	  }

  function heatColor(value, maxValue) {
    const v = Number(value);
    const maxV = Math.max(Number(maxValue) || 0, 1);
    const t = clamp(v / maxV, 0, 1);
    const hue = 220 - 220 * t; // blue -> red
    return `hsl(${hue}, 90%, 55%)`;
  }

  const heatCache = new Map(); // key: ts|metric|agg => Map(station_id -> value)

  function cacheKey(ts, metric, agg) {
    return `${String(ts)}|${String(metric)}|${String(agg)}`;
  }

  async function ensureHeatMetric(ts, metric, agg) {
    const key = cacheKey(ts, metric, agg);
    if (heatCache.has(key)) return heatCache.get(key);
    const payload = await fetchJson(`/stations/heat_at2${qs({ ts, metric, agg })}`);
    const m = new Map();
    for (const row of payload?.points ?? []) m.set(row.station_id, row.value);
    heatCache.set(key, m);
    return m;
  }

  async function refreshHeatIndex() {
    const slider = document.getElementById("heatTimeRange");
    const label = document.getElementById("heatTimeLabel");
    try {
      const payload = await fetchJson("/stations/heat_index2?limit=200");
      state.heatIndex = payload?.timestamps ?? [];
      state.silverBuildMeta = payload?.meta?.silver_build_meta ?? state.silverBuildMeta ?? null;
      if (!state.heatIndex.length) {
        slider.max = "0";
        slider.value = "0";
        label.textContent = "No time index";
        slider.disabled = true;
        return;
      }
      slider.disabled = false;
      slider.max = String(state.heatIndex.length - 1);
      const idx = state.settings.heat_ts_index < 0 ? state.heatIndex.length - 1 : state.settings.heat_ts_index;
      const safe = clamp(idx, 0, state.heatIndex.length - 1);
      slider.value = String(safe);
      label.textContent = fmtTs(state.heatIndex[safe]);
    } catch {
      slider.disabled = true;
      label.textContent = "Heat unavailable";
    }
  }

  async function refreshHeatAtIndex(idx) {
    if (!state.heatIndex.length) return;
    const i = clamp(Number(idx), 0, state.heatIndex.length - 1);
    const ts = state.heatIndex[i];
    const payload = await fetchJson(
      `/stations/heat_at2${qs({ ts, metric: state.settings.heat_metric, agg: state.settings.heat_agg })}`
    );
    state.silverBuildMeta = payload?.meta?.silver_build_meta ?? state.silverBuildMeta ?? null;
    state.heatByStation.clear();
    for (const row of payload?.points ?? []) {
      state.heatByStation.set(row.station_id, row.value);
    }
    heatCache.set(cacheKey(ts, state.settings.heat_metric, state.settings.heat_agg), new Map(state.heatByStation));
    applyHeatToMarkers();
  }

  function updateMapLegend({ showHeat, maxVal } = {}) {
    if (!mapLegend) return;
    const show = Boolean(showHeat);
    const metric = String(state.settings.heat_metric || "available");
    const agg = String(state.settings.heat_agg || "sum");
    const focus = Boolean(state.settings.problem_focus);
    const mode = String(state.settings.problem_mode || "shortage");
    const topN = Number(state.settings.problem_top_n) || 10;
    const rainyNow = Boolean(state.weatherSummary?.is_rainy_now);
    const rainMode = Boolean(state.settings.rain_mode);
    const tsIdx = Number(state.settings.heat_ts_index);
    const ts = state.heatIndex?.[tsIdx] ?? null;
    const updated = ts ? fmtTs(ts) : "—";
    const maxTxt = maxVal == null ? "—" : String(Math.round(Number(maxVal) || 0));

    const headline = show ? `Heat · ${metric} · ${agg}` : "Stations";
    const focusTxt =
      focus ? `Focus: ${mode} · Top ${topN}` : "Tip: enable Heat or Focus for highlights.";
    const rainTxt = rainMode ? (rainyNow ? "Rain mode: ON (raining now)" : "Rain mode: ON") : "Rain mode: OFF";

    mapLegend.innerHTML = `
      <div class="legend-kicker">Map legend</div>
      <div class="legend-headline">${headline}</div>
      <div class="legend-row">
        <span class="legend-swatch" style="background: rgba(37,99,235,0.20);"></span>
        <div class="legend-text">Click a marker to select a MRT station (then use side panels).</div>
      </div>
      <div class="legend-row">
        <span class="legend-swatch" style="background: rgba(15,23,42,0.06);"></span>
        <div class="legend-text">${focusTxt}</div>
      </div>
      <div class="legend-row">
        <span class="legend-swatch" style="background: rgba(22,163,74,0.16);"></span>
        <div class="legend-text">${rainTxt}</div>
      </div>
      <div class="legend-footnote mono">Heat ts: ${updated} · scale max: ${maxTxt}</div>
    `;
  }

  function markerLabel({ station, value, showValue }) {
    const name = stationShortName(station?.name);
    if (!showValue) return name;
    const v = Number(value);
    const valTxt = Number.isFinite(v) ? String(Math.round(v)) : "—";
    return `${name} ${valTxt}`;
  }

  function applyHeatToMarkers() {
    const show = Boolean(state.settings.show_bike_heat);
    heatLegend.classList.toggle("hidden", !show);
    if (!show) {
      for (const s of state.stations) {
        const marker = state.metroMarkerById?.get?.(s.id);
        if (!marker) continue;
        const c = clusterColor(s.cluster);
        const selected = s.id === state.selectedStationId;
        marker.setStyle({ color: c, fillColor: c, fillOpacity: 0.9, opacity: 1.0, weight: selected ? 5 : 2, radius: selected ? 9 : 6 });
        try {
          marker.setTooltipContent(markerLabel({ station: s, value: null, showValue: false }));
        } catch {}
      }
      updateMapLegend({ showHeat: false, maxVal: null });
      return;
    }
    const legendTitle = heatLegend.querySelector(".legend-title");
    if (legendTitle) legendTitle.textContent = `Heat · ${state.settings.heat_metric} · ${state.settings.heat_agg}`;
    if (heatMeta) {
      heatMeta.textContent = summarizeMeta(state.silverBuildMeta || state.stationsMeta || {});
    }
    let maxVal = 0;
    for (const v of state.heatByStation.values()) maxVal = Math.max(maxVal, Number(v) || 0);
    const vals = Array.from(state.heatByStation.values()).map((v) => Number(v)).filter((v) => Number.isFinite(v));
    const focus = Boolean(state.settings.problem_focus);
    const metric = String(state.settings.heat_metric || "available");
    const mode = String(state.settings.problem_mode || "shortage");
    const topN = clamp(Number(state.settings.problem_top_n) || 10, 1, 50);

    let highlight = null;
    if (focus) {
      highlight = new Set();
      if (mode === "rainy_risk") {
        const ids = (state.rainRisk?.items ?? []).map((it) => it.station_id).filter(Boolean).slice(0, topN);
        for (const id of ids) highlight.add(id);
      } else {
        const wantMetric = mode === "pressure" ? "rent_proxy" : "available";
        const tsIdx = Number(document.getElementById("heatTimeRange")?.value ?? 0);
        const ts = state.heatIndex?.[tsIdx] ?? null;
        let map = wantMetric === metric ? state.heatByStation : (ts ? heatCache.get(cacheKey(ts, wantMetric, state.settings.heat_agg)) : null);
        if (!map && ts) {
          // Lazy fetch the needed metric for problem focus; then re-apply styles.
          pushAction({ level: "warn", title: "Focus mode", message: `Loading metric ${wantMetric}…` });
          ensureHeatMetric(ts, wantMetric, state.settings.heat_agg)
            .then(() => applyHeatToMarkers())
            .catch(() => {});
        }
        if (map) {
          const pairs = [];
          for (const s of state.stations) {
            const v = Number(map.get(s.id));
            if (!Number.isFinite(v)) continue;
            pairs.push([s.id, v]);
          }
          pairs.sort((a, b) => (wantMetric === "available" ? a[1] - b[1] : b[1] - a[1]));
          for (const [id] of pairs.slice(0, topN)) highlight.add(id);
        }
      }
    }
    document.getElementById("legendMin").textContent = "0";
    document.getElementById("legendMax").textContent = String(Math.round(maxVal));
    for (const s of state.stations) {
      const marker = state.metroMarkerById?.get?.(s.id);
      if (!marker) continue;
      const val = state.heatByStation.get(s.id) ?? 0;
      const c = heatColor(val, maxVal);
      const selected = s.id === state.selectedStationId;
      const isHot = focus && highlight != null && highlight.has(s.id);
      const deemphasize = focus && highlight != null && !isHot;
      marker.setStyle({
        color: c,
        fillColor: c,
        fillOpacity: deemphasize ? 0.15 : isHot ? 0.98 : 0.9,
        opacity: deemphasize ? 0.25 : 1.0,
        weight: selected ? 6 : deemphasize ? 1 : isHot ? 4 : 3,
        radius: selected ? 10 : deemphasize ? 5 : isHot ? 9 : 7,
      });
      try {
        marker.setTooltipContent(markerLabel({ station: s, value: val, showValue: !deemphasize }));
      } catch {}
    }
    updateMapLegend({ showHeat: true, maxVal });
  }

  map.on("moveend zoomend", () => {
    const c = map.getCenter();
    document.getElementById("statusCoords").textContent = `lat=${c.lat.toFixed(4)} lon=${c.lng.toFixed(4)} z=${map.getZoom()}`;
  });

  const metroChart = buildChart(document.getElementById("metroChart"), "metro", { heightPx: 190 });
  const bikeChart = buildChart(document.getElementById("bikeChart"), "bike", { heightPx: 190 });
  state.briefingChart = buildChart(document.getElementById("briefingChart"), "brief", { heightPx: 180 });
  if (state.briefingChart) {
    state.briefingChart.options.scales.x.ticks.maxTicksLimit = 4;
    state.briefingChart.options.scales.y.ticks = { maxTicksLimit: 4 };
    state.briefingChart.update();
  }

  async function refreshStatus({ quiet = false } = {}) {
    try {
      if (!quiet) setStatusText("Loading status…");
      const payload = await fetchJson("/status");
      state.lastStatusSnapshot = payload;
      setStatusPanel(payload);
      if (state.settings.app_view === "home") renderBriefing(payload, state, { onboarding });
      refreshMeta({ quiet: true }).catch(() => {});
      // These panels are localhost/token-only. Avoid issuing requests (and noisy 403s) unless user is in Ops.
      const details = document.getElementById("detailsDataStatus");
      const wantsAdmin =
        state.settings.app_view === "ops" ||
        Boolean(details?.open) ||
        Boolean(getAdminToken());
      if (wantsAdmin) {
        refreshJobs({ quiet: true }).catch(() => {});
        refreshExternalMetro({ quiet: true }).catch(() => {});
        refreshExternalCalendar({ quiet: true }).catch(() => {});
        refreshExternalWeather({ quiet: true }).catch(() => {});
      }
      // Narrative insights for Home page
      refreshHotspots({ quiet: true }).catch(() => {});
      refreshWeatherInsights({ quiet: true }).catch(() => {});
      if (onboarding.shouldAutoOpen()) {
        const needsOnboarding = (payload?.alerts ?? []).some((a) =>
          String(a.title || "").toLowerCase().includes("onboarding")
        );
        if (needsOnboarding) {
          onboarding.render(payload);
          onboarding.open();
        }
      }
      if (!quiet) setStatusText("Ready");
    } catch (e) {
      console.warn("Failed to load /status", e);
      if (!quiet) setStatusText("Status unavailable");
    }
  }

  async function refreshMeta({ quiet = false } = {}) {
    try {
      const m = await fetchJson("/meta");
      state.globalMeta = m;
      state.silverBuildMeta = m?.silver_build_meta ?? state.silverBuildMeta ?? null;
      state.weatherSummary = m?.meta?.external?.weather_collector ?? null;
      setWeatherPill(m);
      applyRainMode({ source: "meta" }).catch(() => {});
      if (!quiet) setStatusText("Meta updated");
    } catch {
      // ignore
    }
  }

  async function refreshWeatherInsights({ quiet = false } = {}) {
    const city = state.lastStatusSnapshot?.tdx?.bike_cities?.[0] ?? state.stationById.get(state.selectedStationId)?.city ?? "Taipei";
    try {
      state.weatherUsage = await fetchJson(`/insights/weather_usage${qs({ city, hours: 24 })}`);
    } catch {
      state.weatherUsage = null;
    }
    try {
      state.rainRisk = await fetchJson(`/insights/rain_risk_now${qs({ city, top_k: 5 })}`);
    } catch {
      state.rainRisk = null;
    }
    if (state.settings.app_view === "home" && state.lastStatusSnapshot) renderBriefing(state.lastStatusSnapshot, state, { onboarding });
    if (!quiet) setStatusText("Weather insights updated");
  }

  async function applyRainMode({ source } = {}) {
    if (!state.settings.rain_mode) return;
    const rainy = Boolean(state.weatherSummary?.is_rainy_now);
    if (!rainy) return;

    // Switch to a rain-relevant view, but don't fight user choices if they've already customized heat.
    if (!state.settings.show_bike_heat) {
      setSetting("show_bike_heat", true);
      await refreshHeatIndex();
    }
    if (state.settings.heat_metric === "available") setSetting("heat_metric", "rent_proxy");
    if (state.settings.heat_agg !== "sum") setSetting("heat_agg", "sum");

    if (state.settings.show_bike_heat) {
      const idx = document.getElementById("heatTimeRange")?.value ?? state.settings.heat_ts_index;
      await refreshHeatAtIndex(idx);
    }
  }

  async function refreshHotspots({ quiet = false } = {}) {
    try {
      const idx = state.heatIndex[state.heatIndex.length - 1] ?? null;
      const ts = idx ? String(idx) : null;
      const payload = await fetchJson(`/insights/hotspots${qs({ metric: state.settings.heat_metric, agg: state.settings.heat_agg, ts, top_k: 5 })}`);
      state.lastHotspots = payload;
      if (state.settings.app_view === "home" && state.lastStatusSnapshot) renderBriefing(state.lastStatusSnapshot, state, { onboarding });
      if (!quiet) setStatusText("Insights updated");
    } catch {
      state.lastHotspots = null;
    }
  }

  async function refreshInsightsLists({ quiet = false } = {}) {
    const root = document.getElementById("insightsList");
    const modeEl = document.getElementById("insightsMode");
    const topKEl = document.getElementById("insightsTopK");
    if (!root || !modeEl || !topKEl) return;

    const mode = String(modeEl.value || state.settings.insights_mode || "hotspots");
    const topK = clamp(Number(topKEl.value || state.settings.insights_top_k || 10), 1, 50);
    state.settings.insights_mode = mode;
    state.settings.insights_top_k = topK;
    persistSettings();

    root.innerHTML = `<div class="hint">Loading…</div>`;

    const latestTs = async () => {
      if (!state.heatIndex.length) await refreshHeatIndex();
      return state.heatIndex[state.heatIndex.length - 1] ?? null;
    };

    const renderList = (title, items, { valueLabel = "value" } = {}) => {
      const wrap = document.createElement("div");
      const h = document.createElement("div");
      h.className = "hint";
      h.textContent = `${title} · Top ${items.length}`;
      wrap.appendChild(h);

      const list = document.createElement("ul");
      list.className = "list";
      for (const it of items) {
        const li = document.createElement("li");
        li.style.display = "flex";
        li.style.justifyContent = "space-between";
        li.style.gap = "10px";
        li.style.alignItems = "center";
        const left = document.createElement("div");
        left.style.minWidth = "0";
        const name = document.createElement("div");
        name.style.fontWeight = "700";
        name.style.fontSize = "12px";
        name.style.overflow = "hidden";
        name.style.textOverflow = "ellipsis";
        name.style.whiteSpace = "nowrap";
        name.textContent = it.name || it.station_id || it.id || "—";
        const meta = document.createElement("div");
        meta.className = "hint mono";
        meta.style.margin = "2px 0 0 0";
        meta.textContent = `${valueLabel}: ${it.value_txt ?? it.value ?? "—"}`;
        left.appendChild(name);
        left.appendChild(meta);

        const actions = document.createElement("div");
        actions.className = "row row-actions";
        actions.style.margin = "0";
        const btnFocus = document.createElement("button");
        btnFocus.className = "btn";
        btnFocus.textContent = "Focus";
        btnFocus.addEventListener("click", (e) => {
          e.stopPropagation();
          const id = it.station_id || it.id;
          if (!id) return;
          selectStationById(id, { focus: true });
        });
        const btnOpen = document.createElement("button");
        btnOpen.className = "btn btn-primary";
        btnOpen.textContent = "Open";
        btnOpen.addEventListener("click", (e) => {
          e.stopPropagation();
          const id = it.station_id || it.id;
          if (!id) return;
          document.body.dispatchEvent(new CustomEvent("focus_station", { detail: { station_id: id } }));
        });
        actions.appendChild(btnFocus);
        actions.appendChild(btnOpen);

        li.appendChild(left);
        li.appendChild(actions);
        li.addEventListener("click", () => {
          const id = it.station_id || it.id;
          if (!id) return;
          selectStationById(id, { focus: true });
        });
        list.appendChild(li);
      }
      wrap.appendChild(list);
      return wrap;
    };

    try {
      const city = state.lastStatusSnapshot?.tdx?.bike_cities?.[0] ?? "Taipei";
      const ts = await latestTs();

      let items = [];
      let title = mode;
      let valueLabel = "value";

      if (mode === "hotspots" || mode === "coldspots") {
        if (!ts) throw new Error("Heat index is empty");
        const payload = await fetchJson(
          `/insights/hotspots${qs({ metric: state.settings.heat_metric, agg: state.settings.heat_agg, ts, top_k: topK })}`
        );
        state.lastHotspots = payload;
        items = (mode === "hotspots" ? payload?.hot : payload?.cold) ?? [];
        title = mode === "hotspots" ? "Hotspots" : "Coldspots";
        valueLabel = `${state.settings.heat_metric} (${state.settings.heat_agg})`;
      } else if (mode === "shortage" || mode === "pressure") {
        if (!ts) throw new Error("Heat index is empty");
        const metric = mode === "shortage" ? "available" : "rent_proxy";
        const m = await ensureHeatMetric(String(ts), metric, "sum");
        const pairs = [];
        for (const [id, value] of m.entries()) {
          const station = state.stationById.get(id);
          pairs.push({
            station_id: id,
            name: station?.name ?? id,
            value,
            value_txt: Number.isFinite(Number(value)) ? String(Math.round(Number(value))) : "—",
          });
        }
        pairs.sort((a, b) => (mode === "shortage" ? Number(a.value) - Number(b.value) : Number(b.value) - Number(a.value)));
        items = pairs.filter((p) => Number.isFinite(Number(p.value))).slice(0, topK);
        title = mode === "shortage" ? "Shortage (low availability)" : "Pressure (high rent_proxy)";
        valueLabel = metric;
      } else if (mode === "rainy_risk") {
        const payload = await fetchJson(`/insights/rain_risk_now${qs({ city, top_k: topK })}`);
        state.rainRisk = payload;
        items =
          (payload?.items ?? []).map((it) => ({
            station_id: it.station_id,
            name: it.name || it.station_id,
            value: it.mean_available_bikes,
            value_txt:
              it.mean_available_bikes != null && Number.isFinite(Number(it.mean_available_bikes))
                ? String(Math.round(Number(it.mean_available_bikes)))
                : "—",
          })) ?? [];
        title = payload?.is_rainy_now ? "Rainy-risk stations (now)" : "Rainy-risk stations (not raining now)";
        valueLabel = "mean_available";
      }

      root.innerHTML = "";
      const block = renderList(title, items, { valueLabel });
      root.appendChild(block);
      if (!quiet) setStatusText("Insights list updated");
    } catch (e) {
      root.innerHTML = `<div class="hint">Insights unavailable (${e.message})</div>`;
    }
  }

  async function refreshJobs({ quiet = false } = {}) {
    try {
      const jobs = await adminFetchJson("/admin/jobs?limit=20");
      state.jobs = jobs ?? [];
      setJobCenter(state.jobs, {
        onDownload: (j) => {
          window.open(`/admin/jobs/${encodeURIComponent(j.id)}/log`, "_blank");
        },
        onEvents: (j) => adminFetchJson(`/admin/jobs/${encodeURIComponent(j.id)}/events?limit=500`),
        onCancel: (j) =>
          withConfirm(`Cancel job ${j.id.slice(0, 8)}?`, async () => {
            if (j._action === "rerun") {
              setOverlayVisible(true, "Re-running job…");
              try {
                const raw = prompt("Optional override: max availability files (blank to keep same)", "");
                let body = null;
                if (raw != null && String(raw).trim() !== "") {
                  const n = Number(raw);
                  if (Number.isFinite(n) && n > 0) body = { max_availability_files: Math.floor(n) };
                }
                await adminPostJson(`/admin/jobs/${encodeURIComponent(j.id)}/rerun`, body);
                await refreshJobs({ quiet: true });
              } finally {
                setOverlayVisible(false);
              }
              return;
            }
            setOverlayVisible(true, "Canceling job…");
            try {
              await adminPostJson(`/admin/jobs/${encodeURIComponent(j.id)}/cancel`, null);
              await refreshJobs({ quiet: true });
            } finally {
              setOverlayVisible(false);
            }
          }),
      });
      if (!quiet) setStatusText("Jobs updated");
    } catch (e) {
      const root = document.getElementById("jobCenter");
      if (root) root.innerHTML = `<div class="hint">Jobs unavailable (${e.message})</div>`;
    }
  }

  async function refreshExternalMetro({ quiet = false } = {}) {
    try {
      const payload = await adminFetchJson("/external/metro_stations/preview?limit=30");
      renderExternalMetroPanel(
        {
          ok: payload.ok,
          path: payload.path,
          row_count: payload.row_count,
          issues: payload.issues,
          head: payload.rows?.slice?.(0, 5) ?? [],
        },
        {
          onValidate: () => refreshExternalMetro({ quiet: false }),
          onBuild: () => document.getElementById("btnBuildSilver").click(),
          onUpload: async (file) => {
            setOverlayVisible(true, "Uploading external metro CSV…");
            try {
              const form = new FormData();
              form.append("file", file);
              const res = await adminFetch("/external/metro_stations/upload", { method: "POST", body: form });
              const json = await res.json().catch(() => null);
              if (!res.ok) throw new Error(json?.issues?.[0]?.message || `HTTP ${res.status}`);
              setStatusText("External CSV uploaded");
            } catch (e) {
              setStatusText(`Upload failed: ${e.message}`);
            } finally {
              setOverlayVisible(false);
              await refreshExternalMetro({ quiet: true });
              // Wizard: if the CSV validates, show the build plan and offer to start a build job.
              try {
                const preview = await adminFetchJson("/external/metro_stations/preview?limit=5");
                if (preview?.ok) {
                  const plan = await adminPostJson("/admin/build_silver_async?dry_run=1", null);
                  const src = plan?.meta?.metro_source ? `metro_source=${plan.meta.metro_source}` : "";
                  const reason = plan?.meta?.metro_source_reason ? ` (${plan.meta.metro_source_reason})` : "";
                  const overwrites = (plan?.meta?.would_overwrite ?? [])
                    .filter((a) => a?.exists)
                    .map((a) => a.path)
                    .slice(0, 8);
                  const msg =
                    `External CSV looks OK.\n` +
                    (src ? `${src}${reason}\n\n` : "") +
                    (overwrites.length ? `May overwrite:\n${overwrites.join("\n")}\n\n` : "") +
                    "Start async Silver build now?";
                  if (confirm(msg)) {
                    setOverlayVisible(true, "Starting Silver build…");
                    await adminPostJson("/admin/build_silver_async", null);
                    await refreshJobs({ quiet: true });
                    await refreshStatus({ quiet: true });
                    setStatusText("Silver build job started");
                  }
                }
              } catch {
                // ignore
              } finally {
                setOverlayVisible(false);
              }
            }
          },
        }
      );

      // Add download link (localhost-only)
      const root = document.getElementById("externalMetro");
      if (root && payload?.ok) {
        const row = document.createElement("div");
        row.className = "row row-actions";
        row.innerHTML = `<button class="btn" id="btnExternalDownload">Download CSV</button>`;
        row.querySelector("#btnExternalDownload").addEventListener("click", () => {
          window.open("/external/metro_stations/download", "_blank");
        });
        root.prepend(row);
      }
      if (!quiet) setStatusText("External CSV validated");
    } catch (e) {
      const root = document.getElementById("externalMetro");
      if (root) root.innerHTML = `<div class="hint">External CSV endpoints are localhost-only (${e.message})</div>`;
    }
  }

  async function refreshExternalCalendar({ quiet = false } = {}) {
    try {
      const payload = await adminFetchJson("/external/calendar/preview?limit=30");
      renderExternalCsvPanel(
        "externalCalendar",
        {
          ok: payload.ok,
          path: payload.path,
          row_count: payload.row_count,
          issues: payload.issues,
          head: payload.rows?.slice?.(0, 5) ?? [],
        },
        {
          onValidate: () => refreshExternalCalendar({ quiet: false }),
          onBuild: () => document.getElementById("btnBuildSilver").click(),
          onDownload: () => window.open("/external/calendar/download", "_blank"),
          onUpload: async (file) => {
            setOverlayVisible(true, "Uploading calendar CSV…");
            try {
              const form = new FormData();
              form.append("file", file);
              const res = await adminFetch("/external/calendar/upload", { method: "POST", body: form });
              const json = await res.json().catch(() => null);
              if (!res.ok) throw new Error(json?.issues?.[0]?.message || `HTTP ${res.status}`);
              setStatusText("Calendar CSV uploaded");
            } catch (e) {
              setStatusText(`Upload failed: ${e.message}`);
            } finally {
              setOverlayVisible(false);
              await refreshExternalCalendar({ quiet: true });
              try {
                const preview = await adminFetchJson("/external/calendar/preview?limit=5");
                if (preview?.ok) {
                  const plan = await adminPostJson("/admin/build_silver_async?dry_run=1", null);
                  const overwrites = (plan?.meta?.would_overwrite ?? [])
                    .filter((a) => a?.exists)
                    .map((a) => a.path)
                    .slice(0, 8);
                  const msg =
                    `Calendar CSV looks OK.\n\n` +
                    (overwrites.length ? `May overwrite:\n${overwrites.join("\n")}\n\n` : "") +
                    "Start async Silver build now?";
                  if (confirm(msg)) {
                    setOverlayVisible(true, "Starting Silver build…");
                    await adminPostJson("/admin/build_silver_async", null);
                    await refreshJobs({ quiet: true });
                    await refreshStatus({ quiet: true });
                    setStatusText("Silver build job started");
                  }
                }
              } catch {
                // ignore
              } finally {
                setOverlayVisible(false);
              }
            }
          },
        }
      );
      if (!quiet) setStatusText("External calendar validated");
    } catch (e) {
      const root = document.getElementById("externalCalendar");
      if (root) root.innerHTML = `<div class="hint">External CSV endpoints are localhost-only (${e.message})</div>`;
    }
  }

  async function refreshExternalWeather({ quiet = false } = {}) {
    try {
      const payload = await adminFetchJson("/external/weather_hourly/preview?limit=30");
      const ws = state.weatherSummary;
      const cov = ws?.coverage ?? ws?.heartbeat?.coverage ?? null;
      const minTs = cov?.min_ts_utc ? String(cov.min_ts_utc) : null;
      const maxTs = cov?.max_ts_utc ? String(cov.max_ts_utc) : null;
      const rows = ws?.heartbeat?.rows != null ? Number(ws.heartbeat.rows) : null;
      const note =
        ws && typeof ws === "object"
          ? `Collector: ${ws.stale ? "stale" : "ok"} · age ${formatAge(ws.heartbeat_age_s)} · rows ${rows ?? "—"} · ` +
            `range ${minTs ? fmtTs(minTs) : "—"} → ${maxTs ? fmtTs(maxTs) : "—"}${ws.is_rainy_now ? " · raining now" : ""}`
          : "Collector: unavailable";
      renderExternalCsvPanel(
        "externalWeather",
        {
          ok: payload.ok,
          path: payload.path,
          row_count: payload.row_count,
          issues: payload.issues,
          head: payload.rows?.slice?.(0, 5) ?? [],
          note_html: note,
        },
        {
          onValidate: () => refreshExternalWeather({ quiet: false }),
          onBuild: () => document.getElementById("btnBuildSilver").click(),
          onDownload: () => window.open("/external/weather_hourly/download", "_blank"),
          onUpload: async (file) => {
            setOverlayVisible(true, "Uploading weather CSV…");
            try {
              const form = new FormData();
              form.append("file", file);
              const res = await adminFetch("/external/weather_hourly/upload", { method: "POST", body: form });
              const json = await res.json().catch(() => null);
              if (!res.ok) throw new Error(json?.issues?.[0]?.message || `HTTP ${res.status}`);
              setStatusText("Weather CSV uploaded");
            } catch (e) {
              setStatusText(`Upload failed: ${e.message}`);
            } finally {
              setOverlayVisible(false);
              await refreshExternalWeather({ quiet: true });
              try {
                const preview = await adminFetchJson("/external/weather_hourly/preview?limit=5");
                if (preview?.ok) {
                  const plan = await adminPostJson("/admin/build_silver_async?dry_run=1", null);
                  const overwrites = (plan?.meta?.would_overwrite ?? [])
                    .filter((a) => a?.exists)
                    .map((a) => a.path)
                    .slice(0, 8);
                  const msg =
                    `Weather CSV looks OK.\n\n` +
                    (overwrites.length ? `May overwrite:\n${overwrites.join("\n")}\n\n` : "") +
                    "Start async Silver build now?";
                  if (confirm(msg)) {
                    setOverlayVisible(true, "Starting Silver build…");
                    await adminPostJson("/admin/build_silver_async", null);
                    await refreshJobs({ quiet: true });
                    await refreshStatus({ quiet: true });
                    setStatusText("Silver build job started");
                  }
                }
              } catch {
                // ignore
              } finally {
                setOverlayVisible(false);
              }
            }
          },
        }
      );
      if (!quiet) setStatusText("External weather validated");
    } catch (e) {
      const root = document.getElementById("externalWeather");
      if (root) root.innerHTML = `<div class="hint">External CSV endpoints are localhost-only (${e.message})</div>`;
    }
  }

  function withConfirm(message, fn) {
    if (!confirm(message)) return;
    fn();
  }

  function setAdminBusy(busy) {
    const ids = ["btnStartCollector", "btnStopCollector", "btnBuildSilver", "btnRefreshWeather"];
    for (const id of ids) {
      const el = document.getElementById(id);
      if (el) el.disabled = Boolean(busy);
    }
  }

  function setStatusPolling(ms) {
    const next = clamp(Number(ms) || 30000, 3000, 300000);
    if (state.statusTimer) clearInterval(state.statusTimer);
    state.statusPollMs = next;
    state.statusTimer = setInterval(() => refreshStatus({ quiet: true }), next);
  }

  function setSsePill(status, detail) {
    const el = document.getElementById("ssePill");
    if (!el) return;
    el.classList.remove("ok", "warn", "bad");
    if (status === "connected") el.classList.add("ok");
    else if (status === "reconnecting") el.classList.add("warn");
    else if (status === "disconnected") el.classList.add("bad");
    const suffix = detail ? ` · ${detail}` : "";
    el.textContent = `Live: ${status}${suffix}`;
  }

  document.getElementById("btnRefreshStatus").addEventListener("click", () => refreshStatus({ quiet: false }));
  // Insights tab controls
  document.getElementById("btnRefreshInsights")?.addEventListener("click", () => refreshInsightsLists({ quiet: false }));
  document.getElementById("btnInsightsToExplorer")?.addEventListener("click", () =>
    document.body.dispatchEvent(new CustomEvent("nav", { detail: { view: "explorer" } }))
  );
  document.getElementById("insightsMode")?.addEventListener("change", () => refreshInsightsLists({ quiet: true }));
  document.getElementById("insightsTopK")?.addEventListener("change", () => refreshInsightsLists({ quiet: true }));
  document.getElementById("btnCopyDataMeta").addEventListener("click", async () => {
    const payload = state.lastDataMeta || { stations_meta: state.stationsMeta, silver_build_meta: state.silverBuildMeta };
    try {
      await navigator.clipboard.writeText(JSON.stringify(payload, null, 2));
      setStatusText("Copied data meta");
    } catch {
      setStatusText("Copy failed");
    }
  });
  document.getElementById("btnCopyPermalink").addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(String(location.href));
      setStatusText("Copied permalink");
    } catch {
      setStatusText("Copy failed");
    }
  });

  // Admin token controls (optional, for remote admin usage).
  const tokenInput = document.getElementById("adminTokenInput");
  if (tokenInput) tokenInput.value = getAdminToken();
  document.getElementById("btnSaveAdminToken").addEventListener("click", () => {
    const v = String(document.getElementById("adminTokenInput")?.value || "").trim();
    setAdminToken(v);
    setStatusText(v ? "Admin token saved" : "Admin token cleared");
  });
  document.getElementById("btnClearAdminToken").addEventListener("click", () => {
    setAdminToken("");
    const el = document.getElementById("adminTokenInput");
    if (el) el.value = "";
    setStatusText("Admin token cleared");
  });
  document.getElementById("btnStartCollector").addEventListener("click", () =>
    withConfirm("Start background collector now?", async () => {
      setAdminBusy(true);
      setOverlayVisible(true, "Starting collector…");
      try {
        setStatusText("Starting collector…");
        const res = await adminPostJson("/admin/collector/start", null);
        await refreshStatus({ quiet: true });
        setStatusText(res?.detail || "Collector started");
        pushAction({ level: "ok", title: "Collector", message: res?.detail || "Collector started" });
        document.getElementById("adminResult").innerHTML = `<div class="mono">${res?.detail || ""}</div>`;
      } catch (e) {
        setStatusText(`Start failed: ${e.message}`);
        pushAction({ level: "bad", title: "Collector", message: `Start failed: ${e.message}` });
      } finally {
        setOverlayVisible(false);
        setAdminBusy(false);
      }
    })
  );
  document.getElementById("btnStopCollector").addEventListener("click", () =>
    withConfirm("Stop background collector now?", async () => {
      setAdminBusy(true);
      setOverlayVisible(true, "Stopping collector…");
      try {
        setStatusText("Stopping collector…");
        const res = await adminPostJson("/admin/collector/stop", null);
        await refreshStatus({ quiet: true });
        setStatusText(res?.detail || "Collector stopped");
        pushAction({ level: "ok", title: "Collector", message: res?.detail || "Collector stopped" });
        document.getElementById("adminResult").innerHTML = `<div class="mono">${res?.detail || ""}</div>`;
      } catch (e) {
        setStatusText(`Stop failed: ${e.message}`);
        pushAction({ level: "bad", title: "Collector", message: `Stop failed: ${e.message}` });
      } finally {
        setOverlayVisible(false);
        setAdminBusy(false);
      }
    })
  );
  document.getElementById("btnBuildSilver").addEventListener("click", async () => {
    setAdminBusy(true);
    setOverlayVisible(true, "Preparing Silver build…");
    try {
      const plan = await adminPostJson("/admin/build_silver_async?dry_run=1", null);
      const cmd = Array.isArray(plan?.meta?.command) ? plan.meta.command.join(" ") : "python scripts/build_silver.py";
      const overwrites = (plan?.meta?.would_overwrite ?? plan?.artifacts ?? [])
        .filter((a) => a?.exists)
        .map((a) => a.path)
        .slice(0, 8);
      const msg =
        `Build Silver will run:\n${cmd}\n\n` +
        (overwrites.length ? `May overwrite:\n${overwrites.join("\n")}\n\n` : "") +
        "Start async Silver build job now? This may take a while.";
      if (!confirm(msg)) return;

      setOverlayVisible(true, "Starting Silver build…");
      setStatusText("Starting Silver build…");
      const res = await adminPostJson("/admin/build_silver_async", null);
      await refreshJobs({ quiet: true });
      await refreshStatus({ quiet: true });
      setStatusText(res?.detail || "Silver build job started");
      pushAction({ level: "ok", title: "Build Silver", message: res?.detail || "Silver build job started" });
      document.getElementById("adminResult").innerHTML = `<div class="mono">${res?.detail || ""} ${res?.job_id ? `job ${res.job_id}` : ""}</div>`;
    } catch (e) {
      setStatusText(`Build failed: ${e.message}`);
      pushAction({ level: "bad", title: "Build Silver", message: `Build failed: ${e.message}` });
    } finally {
      setOverlayVisible(false);
      setAdminBusy(false);
    }
  });

  document.getElementById("btnRefreshWeather").addEventListener("click", () =>
    withConfirm("Request an immediate weather refresh now?", async () => {
      setAdminBusy(true);
      setOverlayVisible(true, "Refreshing weather…");
      try {
        const res = await adminPostJson("/admin/weather/refresh", null);
        setStatusText(res?.detail || "Weather refresh requested");
        pushAction({ level: "ok", title: "Weather", message: res?.detail || "Refresh requested" });
        document.getElementById("adminResult").innerHTML = `<div class="mono">${res?.detail || ""}</div>`;
        await refreshMeta({ quiet: true });
        await refreshWeatherInsights({ quiet: true });
      } catch (e) {
        setStatusText(`Weather refresh failed: ${e.message}`);
        pushAction({ level: "bad", title: "Weather", message: `Refresh failed: ${e.message}` });
      } finally {
        setOverlayVisible(false);
        setAdminBusy(false);
      }
    })
  );

  function startEventStream() {
    if (!window.EventSource) return;
    const es = new EventSource("/events?interval_s=3");
    state.sse = { connected: false, lastEventAtMs: null, lastErrorAtMs: null };

    setSsePill("reconnecting");
    // When SSE is unavailable, keep polling more frequently.
    setStatusPolling(10000);

    es.onopen = () => {
      state.sse.connected = true;
      state.sse.lastEventAtMs = Date.now();
      setSsePill("connected");
      // When SSE is healthy, poll less frequently as a safety net.
      setStatusPolling(60000);
    };
    es.onerror = () => {
      state.sse.connected = false;
      state.sse.lastErrorAtMs = Date.now();
      setSsePill("reconnecting");
      // When SSE is unhealthy (proxy issues, server restarts), poll more often.
      setStatusPolling(10000);
    };

    const markEvent = () => {
      state.sse.lastEventAtMs = Date.now();
    };
    es.addEventListener("status", (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg?.status) {
          markEvent();
          state.lastStatusSnapshot = msg.status;
          setStatusPanel(msg.status);
          if (state.settings.app_view === "home") renderBriefing(msg.status, state, { onboarding });
        }
      } catch {}
    });
    es.addEventListener("alerts", (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (Array.isArray(msg?.alerts) && state.lastStatusSnapshot) {
          markEvent();
          state.lastStatusSnapshot.alerts = msg.alerts;
          setStatusPanel(state.lastStatusSnapshot);
        }
      } catch {}
    });
    es.addEventListener("collector_heartbeat", (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg?.collector_heartbeat && state.lastStatusSnapshot) {
          markEvent();
          state.lastStatusSnapshot.health = state.lastStatusSnapshot.health || {};
          state.lastStatusSnapshot.health.collector_heartbeat = msg.collector_heartbeat;
          setStatusPanel(state.lastStatusSnapshot);
        }
      } catch {}
    });
    es.addEventListener("silver_freshness_changed", (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg?.silver_freshness && state.lastStatusSnapshot) {
          markEvent();
          state.lastStatusSnapshot.health = state.lastStatusSnapshot.health || {};
          Object.assign(state.lastStatusSnapshot.health, msg.silver_freshness);
          setStatusPanel(state.lastStatusSnapshot);
        }
      } catch {}
    });
    es.addEventListener("jobs", (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (Array.isArray(msg?.jobs)) {
          markEvent();
          state.jobs = msg.jobs;
          setJobCenter(state.jobs, {
            onDownload: (j) => window.open(`/admin/jobs/${encodeURIComponent(j.id)}/log`, "_blank"),
            onEvents: (j) => adminFetchJson(`/admin/jobs/${encodeURIComponent(j.id)}/events?limit=500`),
            onCancel: (j) =>
              withConfirm(`Cancel job ${j.id.slice(0, 8)}?`, async () => {
                if (j._action === "rerun") await adminPostJson(`/admin/jobs/${encodeURIComponent(j.id)}/rerun`, null);
                else await adminPostJson(`/admin/jobs/${encodeURIComponent(j.id)}/cancel`, null);
              }),
          });
        }
      } catch {}
    });
    es.addEventListener("job_update", (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg?.job) {
          markEvent();
          const j = msg.job;
          const idx = state.jobs.findIndex((x) => x.id === j.id);
          if (idx >= 0) state.jobs[idx] = j;
          else state.jobs.unshift(j);
          setJobCenter(state.jobs, {
            onDownload: (x) => window.open(`/admin/jobs/${encodeURIComponent(x.id)}/log`, "_blank"),
            onEvents: (x) => adminFetchJson(`/admin/jobs/${encodeURIComponent(x.id)}/events?limit=500`),
            onCancel: (x) =>
              withConfirm(`Cancel job ${x.id.slice(0, 8)}?`, async () => {
                if (x._action === "rerun") await adminPostJson(`/admin/jobs/${encodeURIComponent(x.id)}/rerun`, null);
                else await adminPostJson(`/admin/jobs/${encodeURIComponent(x.id)}/cancel`, null);
              }),
          });
        }
      } catch {}
    });
    es.addEventListener("heat", async (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        const latest = msg?.heat_latest_ts;
        markEvent();
        if (state.settings.heat_follow_latest && latest) {
          const last = state.heatIndex[state.heatIndex.length - 1] ?? null;
          const latestIso = new Date(latest).toISOString();
          const lastIso = last ? new Date(last).toISOString() : null;
          if (!lastIso || latestIso !== lastIso) {
            await refreshHeatIndex();
          }
          if (state.settings.show_bike_heat) {
            const idx = Math.max(0, state.heatIndex.length - 1);
            document.getElementById("heatTimeRange").value = String(idx);
            document.getElementById("heatTimeLabel").textContent = fmtTs(state.heatIndex[idx]);
            setSetting("heat_ts_index", idx);
            await refreshHeatAtIndex(idx);
          }
        }
      } catch {}
    });
    es.addEventListener("sse_error", (ev) => {
      try {
        const msg = JSON.parse(ev.data || "{}");
        setSsePill(state?.sse?.connected ? "connected" : "reconnecting", msg?.detail || "server error");
      } catch {}
    });
  }

  refreshStatus({ quiet: true });
  setStatusPolling(30000);
  startEventStream();

	  function applySettingsToControls() {
    document.getElementById("joinMethodSelect").value = state.settings.join_method;
    document.getElementById("granularitySelect").value = state.settings.granularity;

    document.getElementById("radiusInput").value = String(state.settings.radius_m);
    document.getElementById("radiusNumber").value = String(state.settings.radius_m);
    document.getElementById("nearestKInput").value = String(state.settings.nearest_k);
    document.getElementById("nearestKNumber").value = String(state.settings.nearest_k);
    document.getElementById("windowDaysInput").value = String(state.settings.window_days);
    document.getElementById("windowDaysNumber").value = String(state.settings.window_days);

    document.getElementById("metroMetricSelect").value = state.settings.metro_metric;
    document.getElementById("bikeMetricSelect").value = state.settings.bike_metric;

    document.getElementById("similarTopK").value = String(state.settings.similar_top_k);
    document.getElementById("similarMetricSelect").value = state.settings.similar_metric;
    document.getElementById("similarStandardize").checked = Boolean(state.settings.similar_standardize);

    document.getElementById("toggleNearbyBikes").checked = Boolean(state.settings.show_nearby_bikes);
    document.getElementById("toggleHeat").checked = Boolean(state.settings.show_bike_heat);
    document.getElementById("toggleRainMode").checked = Boolean(state.settings.rain_mode);
    document.getElementById("heatMetricSelect").value = state.settings.heat_metric;
    document.getElementById("heatAggSelect").value = state.settings.heat_agg;
    document.getElementById("toggleHeatFollowLatest").checked = Boolean(state.settings.heat_follow_latest);
    document.getElementById("toggleProblemStations").checked = Boolean(state.settings.problem_focus);
    document.getElementById("problemModeSelect").value = String(state.settings.problem_mode || "shortage");
    document.getElementById("problemTopN").value = String(Number(state.settings.problem_top_n) || 10);
    document.getElementById("toggleBuffer").checked = Boolean(state.settings.show_buffer);
    document.getElementById("toggleLinks").checked = Boolean(state.settings.show_links);
	    document.getElementById("toggleLive").checked = Boolean(state.settings.live);
	    document.getElementById("liveInterval").value = String(state.settings.live_interval_sec);

	    const insightsMode = document.getElementById("insightsMode");
	    const insightsTopK = document.getElementById("insightsTopK");
	    if (insightsMode) insightsMode.value = String(state.settings.insights_mode || "hotspots");
	    if (insightsTopK) insightsTopK.value = String(Number(state.settings.insights_top_k) || 10);

	    const showRadius = state.settings.join_method === "buffer";
	    toggleHidden(document.getElementById("radiusField"), !showRadius);
	    toggleHidden(document.getElementById("nearestField"), showRadius);
	  }

  function persistSettings() {
    storeJson("metrobikeatlas.settings.v1", state.settings);
  }

  function setSetting(key, value) {
    state.settings[key] = value;
    persistSettings();
    updateHud({ station: state.stationById.get(state.selectedStationId), settings: state.settings });
    setHashParams(buildPermalinkState(state));
    if (key === "heat_metric" || key === "heat_agg" || key === "show_bike_heat") {
      pushAction({ level: "ok", title: "Map updated", message: `Heat: ${state.settings.heat_metric} · ${state.settings.heat_agg}` });
    }
    if (key === "rain_mode") {
      pushAction({ level: "ok", title: "Rain mode", message: state.settings.rain_mode ? "Enabled" : "Disabled" });
    }
    if (key === "problem_focus") {
      pushAction({ level: "ok", title: "Focus mode", message: state.settings.problem_focus ? "Enabled" : "Disabled" });
    }
  }

  function clearLiveTimer() {
    if (state.liveTimer) clearInterval(state.liveTimer);
    state.liveTimer = null;
  }

  function ensureLiveTimer(refreshFn) {
    clearLiveTimer();
    if (!state.settings.live) return;
    const interval = clamp(Number(state.settings.live_interval_sec) || 30, 5, 3600);
    state.liveTimer = setInterval(refreshFn, interval * 1000);
  }

  function setBufferCircle(station) {
    if (bufferCircle) {
      bufferCircle.remove();
      bufferCircle = null;
    }
    if (!station) return;
    if (!state.settings.show_buffer) return;
    if (state.settings.join_method !== "buffer") return;
    bufferCircle = L.circle([station.lat, station.lon], {
      radius: Number(state.settings.radius_m) || 500,
      color: "rgba(42,111,219,0.95)",
      weight: 2,
      fillColor: "rgba(42,111,219,0.15)",
      fillOpacity: 0.5,
    }).addTo(map);
  }

  function renderNearbyOnMap(station, nearby) {
    bikeLayer.clearLayers();
    linkLayer.clearLayers();
    if (!station) return;
    if (!state.settings.show_nearby_bikes && !state.settings.show_links) return;

    const maxLines = 30;
    for (const [i, b] of (nearby ?? []).entries()) {
      const d = Number(b.distance_m) || 0;
      const t = clamp(d / 800, 0, 1);
      const hue = 140 - 80 * t; // green -> yellow/orange
      const color = `hsla(${hue}, 85%, 45%, 0.95)`;
      const fill = `hsla(${hue}, 85%, 55%, 0.35)`;
      const m = L.circleMarker([b.lat, b.lon], {
        radius: 5,
        color,
        fillColor: fill,
        fillOpacity: 0.9,
        weight: 2,
      });
      m.bindTooltip(`${b.name} (${Math.round(b.distance_m)}m)`, { direction: "top", opacity: 0.9 });
      if (state.settings.show_nearby_bikes) m.addTo(bikeLayer);

      if (state.settings.show_links && i < maxLines) {
        L.polyline(
          [
            [station.lat, station.lon],
            [b.lat, b.lon],
          ],
          { color: "rgba(37,99,235,0.35)", weight: 2 }
        ).addTo(linkLayer);
      }
    }
  }

  function selectStationById(id, { focus = true } = {}) {
    const station = state.stationById.get(id);
    if (!station) return;
    state.selectedStationId = id;
    document.getElementById("stationSelect").value = id;

    updateStationMeta(station);
    updateHud({ station, settings: state.settings });
    pushAction({ level: "ok", title: "Station selected", message: `${station.name} · ${station.id}` });

    if (focus) map.setView([station.lat, station.lon], Math.max(map.getZoom(), 13));

    if (state.selectedMarker) {
      const prev = state.selectedMarker;
      prev.setStyle({ weight: 2, radius: 6 });
    }
    const marker = state.metroMarkerById.get(id);
    if (marker) {
      marker.setStyle({ weight: 4, radius: 8 });
      state.selectedMarker = marker;
    }

    setBufferCircle(station);
    setHashParams(buildPermalinkState(state));
    applyHeatToMarkers();
  }

  async function refreshSelectedStation({ reason = "refresh" } = {}) {
    const id = state.selectedStationId;
    if (!id) return;
    const station = state.stationById.get(id);
    if (!station) return;

    if (state.aborter) state.aborter.abort();
    const ctrl = new AbortController();
    state.aborter = ctrl;

    const joinParams = {
      join_method: state.settings.join_method,
      radius_m: state.settings.join_method === "buffer" ? state.settings.radius_m : null,
      nearest_k: state.settings.join_method === "nearest" ? state.settings.nearest_k : null,
    };

    const tsParams = {
      ...joinParams,
      granularity: state.settings.granularity,
      timezone: state.settings.timezone,
      window_days: state.settings.window_days,
    };

    const simParams = {
      top_k: state.settings.similar_top_k,
      metric: state.settings.similar_metric,
      standardize: state.settings.similar_standardize,
    };

    try {
      setStatusText(`${reason}…`);

      const [ts, nearby, factors, similar] = await Promise.all([
        fetchJson(`/station/${encodeURIComponent(id)}/timeseries${qs(tsParams)}`, { signal: ctrl.signal }),
        fetchJson(`/station/${encodeURIComponent(id)}/nearby_bike2${qs({ ...joinParams, limit: 50 })}`, { signal: ctrl.signal }),
        fetchJson(`/station/${encodeURIComponent(id)}/factors`, { signal: ctrl.signal }),
        fetchJson(`/station/${encodeURIComponent(id)}/similar${qs(simParams)}`, { signal: ctrl.signal }),
      ]);

      state.lastTimeseries = ts;
      state.lastNearby = nearby;
      const nearbyItems = nearby?.items ?? nearby ?? [];
      if (state.settings.app_view === "home" && state.lastStatusSnapshot) {
        renderBriefing(state.lastStatusSnapshot, state, { onboarding });
      }

      // Charts
      const metroMetric = state.settings.metro_metric;
      const metroSeries = pickSeries(ts, metroMetric);
      const bikeSeries = pickSeries({ series: ts.series }, state.settings.bike_metric);

      document.getElementById("metroChartTitle").textContent = seriesLabel(metroSeries);
      document.getElementById("bikeChartTitle").textContent = seriesLabel(bikeSeries);
      metroChart.data.datasets[0].label = seriesLabel(metroSeries);
      bikeChart.data.datasets[0].label = seriesLabel(bikeSeries);

      setChartData(metroChart, metroSeries?.points ?? []);
      setChartData(bikeChart, bikeSeries?.points ?? []);

      // Lists
      setNearbyList(nearbyItems);
      setFactors(factors);
      setSimilarStations(similar, (sid) => {
        selectStationById(sid, { focus: true });
        refreshSelectedStation({ reason: "similar" });
      });

      renderNearbyOnMap(station, nearbyItems);
      setBufferCircle(station);

      const resolved = ts?.meta?.resolved ?? null;
      if (resolved) {
        const join =
          resolved.join_method === "buffer"
            ? `buffer ${Math.round(resolved.radius_m)}m`
            : `nearest k=${Math.round(resolved.nearest_k)}`;
        const metaLine = summarizeMeta(ts?.meta) || summarizeMeta(state.stationsMeta) || "";
        const line = `timeseries · ${resolved.granularity} · ${join} · window=${resolved.window_days ?? "all"}d`;
        setRequestMetaText(metaLine ? `${line}\n${metaLine}` : line);
      }
      state.lastDataMeta = {
        stations_meta: state.stationsMeta,
        timeseries_meta: ts?.meta ?? null,
        nearby_meta: nearby?.meta ?? null,
      };
      setStatusText(`Loaded ${id} · ${ts.granularity} · ${nearbyItems.length} bikes`);
    } catch (err) {
      if (err?.name === "AbortError") return;
      console.error(err);
      setStatusText(`Error: ${err.message}`);
    }
  }

  function refreshChartsFromCache() {
    const ts = state.lastTimeseries;
    if (!ts) return;
    const metroSeries = pickSeries(ts, state.settings.metro_metric);
    const bikeSeries = pickSeries({ series: ts.series }, state.settings.bike_metric);
    document.getElementById("metroChartTitle").textContent = seriesLabel(metroSeries);
    document.getElementById("bikeChartTitle").textContent = seriesLabel(bikeSeries);
    setChartData(metroChart, metroSeries?.points ?? []);
    setChartData(bikeChart, bikeSeries?.points ?? []);
  }

  function updatePanelsCollapsed() {
    document.body.classList.toggle("left-collapsed", Boolean(state.settings.left_collapsed));
    document.body.classList.toggle("right-collapsed", Boolean(state.settings.right_collapsed));
  }

  // Load stations and render metro markers
  try {
    const payload = await fetchJson("/stations2");
    state.stations = payload?.items ?? [];
    state.stationsMeta = payload?.meta ?? null;
  } catch (e) {
    console.warn("Stations failed", e);
    setStatusText(`Stations unavailable: ${e.message}`);
    // Try to surface onboarding guidance.
    try {
      const status = await fetchJson("/status");
      onboarding.render(status);
      onboarding.open();
    } catch {
      // ignore
    }
    state.stations = [];
  }
  state.stationById = new Map(state.stations.map((s) => [s.id, s]));
  state.metroMarkerById = new Map();

  const stationSelect = document.getElementById("stationSelect");
  stationSelect.innerHTML = `<option value="">Select…</option>`;
  for (const s of state.stations) {
    const opt = document.createElement("option");
    opt.value = s.id;
    const district = s.district ? ` · ${s.district}` : "";
    opt.textContent = `${s.name}${district}`;
    stationSelect.appendChild(opt);
  }

  for (const s of state.stations) {
    const marker = L.circleMarker([s.lat, s.lon], {
      radius: 6,
      color: clusterColor(s.cluster),
      fillColor: clusterColor(s.cluster),
      fillOpacity: 0.9,
      weight: 2,
      station_id: s.id,
    });
    marker.bindTooltip(stationShortName(s.name), {
      permanent: true,
      direction: "center",
      className: "station-label",
      opacity: 1.0,
      interactive: false,
    });
    marker.on("click", () => {
      selectStationById(s.id, { focus: true });
      refreshSelectedStation({ reason: "click" });
    });
    marker.addTo(metroLayer);
    state.metroMarkerById.set(s.id, marker);
  }

  if (initialStationId && state.stationById.has(initialStationId)) {
    selectStationById(initialStationId, { focus: false });
    refreshSelectedStation({ reason: "permalink" });
  }

  // Guided mode: when opened from a policy card, surface a short instruction.
  if (guided) {
    const parts = [];
    if (guidedTitle) parts.push(guidedTitle);
    if (guidedKind) parts.push(`mode=${guidedKind}`);
    if (state.settings.show_bike_heat) parts.push(`heat=${state.settings.heat_metric}/${state.settings.heat_agg}`);
    if (initialStationId) parts.push(`station=${initialStationId}`);
    pushAction({
      level: "ok",
      title: "Guided mode",
      message:
        (parts.length ? parts.join(" · ") : "Opened from a policy card.") +
        " · Next: check the Charts panel and the station evidence.",
    });
    // Ensure the right panel is visible for evidence.
    if (state.settings.right_collapsed) {
      setSetting("right_collapsed", false);
      updatePanelsCollapsed();
    }
  }

  // Heat layer initialization (after markers exist).
  refreshHeatIndex()
    .then(async () => {
      const slider = document.getElementById("heatTimeRange");
      if (state.settings.show_bike_heat) {
        await refreshHeatAtIndex(slider.value);
      } else {
        applyHeatToMarkers();
      }
    })
    .catch(() => {});

  applySettingsToControls();
  updatePanelsCollapsed();
  updateHud({ station: null, settings: state.settings });

  // Overview is global; fetch once.
  fetchJson("/analytics/overview")
    .then((overview) => setOverview(overview))
    .catch((err) => console.warn("Overview failed", err));

  // --- Controls wiring ---
  const debouncedRefresh = debounce(() => refreshSelectedStation({ reason: "apply" }), 250);

  stationSelect.addEventListener("change", () => {
    const id = stationSelect.value;
    if (!id) return;
    selectStationById(id, { focus: true });
    refreshSelectedStation({ reason: "select" });
  });

  function buildTimeseriesUrl(stationId) {
    const joinParams = {
      join_method: state.settings.join_method,
      radius_m: state.settings.join_method === "buffer" ? state.settings.radius_m : null,
      nearest_k: state.settings.join_method === "nearest" ? state.settings.nearest_k : null,
    };
    const tsParams = {
      ...joinParams,
      granularity: state.settings.granularity,
      timezone: state.settings.timezone,
      window_days: state.settings.window_days,
      metro_series: state.settings.metro_metric === "auto" ? "auto" : "auto",
    };
    return `${window.location.origin}/station/${encodeURIComponent(stationId)}/timeseries${qs(tsParams)}`;
  }

  function buildNearbyUrl(stationId) {
    const joinParams = {
      join_method: state.settings.join_method,
      radius_m: state.settings.join_method === "buffer" ? state.settings.radius_m : null,
      nearest_k: state.settings.join_method === "nearest" ? state.settings.nearest_k : null,
      limit: 50,
    };
    return `${window.location.origin}/station/${encodeURIComponent(stationId)}/nearby_bike2${qs(joinParams)}`;
  }

  async function copyToClipboard(text) {
    try {
      await navigator.clipboard.writeText(text);
      setStatusText("Copied");
    } catch {
      setStatusText("Copy failed");
    }
  }

  document.getElementById("btnCopyTimeseriesUrl").addEventListener("click", async () => {
    if (!state.selectedStationId) return setStatusText("Pick a station first");
    await copyToClipboard(buildTimeseriesUrl(state.selectedStationId));
  });
  document.getElementById("btnCopyNearbyUrl").addEventListener("click", async () => {
    if (!state.selectedStationId) return setStatusText("Pick a station first");
    await copyToClipboard(buildNearbyUrl(state.selectedStationId));
  });

  document.getElementById("btnApply").addEventListener("click", () => refreshSelectedStation({ reason: "apply" }));
  document.getElementById("btnResetSettings").addEventListener("click", () => {
    state.settings = defaultSettingsFromConfig(cfg);
    persistSettings();
    applySettingsToControls();
    updatePanelsCollapsed();
    setBufferCircle(state.stationById.get(state.selectedStationId));
    refreshSelectedStation({ reason: "reset" });
  });

  document.getElementById("btnResetView").addEventListener("click", () => {
    map.setView([cfg.web_map.center_lat, cfg.web_map.center_lon], cfg.web_map.zoom);
  });

  document.getElementById("btnToggleLeft").addEventListener("click", () => {
    setSetting("left_collapsed", !state.settings.left_collapsed);
    updatePanelsCollapsed();
  });
  document.getElementById("btnToggleRight").addEventListener("click", () => {
    setSetting("right_collapsed", !state.settings.right_collapsed);
    updatePanelsCollapsed();
  });

  // join method
  document.getElementById("joinMethodSelect").addEventListener("change", (e) => {
    setSetting("join_method", e.target.value);
    applySettingsToControls();
    setBufferCircle(state.stationById.get(state.selectedStationId));
    debouncedRefresh();
  });

  function bindRangeWithNumber(rangeId, numberId, key, { min, max } = {}) {
    const range = document.getElementById(rangeId);
    const num = document.getElementById(numberId);
    function set(v) {
      const n = Number(v);
      const val = clamp(n, min ?? n, max ?? n);
      range.value = String(val);
      num.value = String(val);
      setSetting(key, val);
    }
    range.addEventListener("input", () => {
      set(range.value);
      setBufferCircle(state.stationById.get(state.selectedStationId));
      debouncedRefresh();
    });
    num.addEventListener("change", () => {
      set(num.value);
      setBufferCircle(state.stationById.get(state.selectedStationId));
      debouncedRefresh();
    });
  }

  bindRangeWithNumber("radiusInput", "radiusNumber", "radius_m", { min: 50, max: 5000 });
  bindRangeWithNumber("nearestKInput", "nearestKNumber", "nearest_k", { min: 1, max: 100 });
  bindRangeWithNumber("windowDaysInput", "windowDaysNumber", "window_days", { min: 1, max: 365 });

  document.getElementById("granularitySelect").addEventListener("change", (e) => {
    setSetting("granularity", e.target.value);
    debouncedRefresh();
  });

  document.getElementById("metroMetricSelect").addEventListener("change", (e) => {
    setSetting("metro_metric", e.target.value);
    refreshChartsFromCache();
  });
  document.getElementById("bikeMetricSelect").addEventListener("change", (e) => {
    setSetting("bike_metric", e.target.value);
    refreshChartsFromCache();
  });

  document.getElementById("similarTopK").addEventListener("change", (e) => {
    setSetting("similar_top_k", Number(e.target.value) || cfg.analytics.similarity.top_k);
    debouncedRefresh();
  });
  document.getElementById("similarMetricSelect").addEventListener("change", (e) => {
    setSetting("similar_metric", e.target.value);
    debouncedRefresh();
  });
  document.getElementById("similarStandardize").addEventListener("change", (e) => {
    setSetting("similar_standardize", Boolean(e.target.checked));
    debouncedRefresh();
  });

  function bindToggle(id, key, after) {
    const el = document.getElementById(id);
    el.addEventListener("change", (e) => {
      setSetting(key, Boolean(e.target.checked));
      after?.();
      debouncedRefresh();
    });
  }

  bindToggle("toggleNearbyBikes", "show_nearby_bikes", () => {});
  bindToggle("toggleLinks", "show_links", () => {});
  bindToggle("toggleBuffer", "show_buffer", () => {
    setBufferCircle(state.stationById.get(state.selectedStationId));
  });

  document.getElementById("toggleRainMode").addEventListener("change", async (e) => {
    setSetting("rain_mode", Boolean(e.target.checked));
    await applyRainMode({ source: "ui" });
  });

  document.getElementById("toggleHeat").addEventListener("change", async (e) => {
    setSetting("show_bike_heat", Boolean(e.target.checked));
    if (state.settings.show_bike_heat) {
      await refreshHeatIndex();
      await refreshHeatAtIndex(document.getElementById("heatTimeRange").value);
    } else {
      applyHeatToMarkers();
    }
  });

  document.getElementById("heatMetricSelect").addEventListener("change", async (e) => {
    setSetting("heat_metric", e.target.value);
    if (state.settings.show_bike_heat) await refreshHeatAtIndex(document.getElementById("heatTimeRange").value);
    else applyHeatToMarkers();
  });
  document.getElementById("heatAggSelect").addEventListener("change", async (e) => {
    setSetting("heat_agg", e.target.value);
    if (state.settings.show_bike_heat) await refreshHeatAtIndex(document.getElementById("heatTimeRange").value);
    else applyHeatToMarkers();
  });
  document.getElementById("toggleHeatFollowLatest").addEventListener("change", (e) => {
    setSetting("heat_follow_latest", Boolean(e.target.checked));
  });
  document.getElementById("toggleProblemStations").addEventListener("change", (e) => {
    setSetting("problem_focus", Boolean(e.target.checked));
    if (Boolean(e.target.checked) && !state.settings.show_bike_heat) {
      // Focus needs station-wide values; enable heat automatically.
      const mode = String(state.settings.problem_mode || "shortage");
      if (mode === "pressure") setSetting("heat_metric", "rent_proxy");
      if (mode === "shortage") setSetting("heat_metric", "available");
      setSetting("show_bike_heat", true);
      refreshHeatIndex()
        .then(() => refreshHeatAtIndex(document.getElementById("heatTimeRange").value))
        .catch(() => applyHeatToMarkers());
      return;
    }
    applyHeatToMarkers();
  });
  document.getElementById("problemModeSelect").addEventListener("change", async (e) => {
    setSetting("problem_mode", e.target.value);
    if (state.settings.problem_focus) {
      if (state.settings.problem_mode === "pressure") setSetting("heat_metric", "rent_proxy");
      if (state.settings.problem_mode === "shortage") setSetting("heat_metric", "available");
    }
    // If focus depends on a metric not currently loaded, prefetch it.
    try {
      const tsIdx = Number(document.getElementById("heatTimeRange")?.value ?? 0);
      const ts = state.heatIndex?.[tsIdx] ?? null;
      if (ts) {
        const mode = String(state.settings.problem_mode || "shortage");
        const wantMetric = mode === "pressure" ? "rent_proxy" : mode === "shortage" ? "available" : null;
        if (wantMetric && !heatCache.get(cacheKey(ts, wantMetric, state.settings.heat_agg))) {
          await ensureHeatMetric(ts, wantMetric, state.settings.heat_agg);
        }
      }
    } catch {
      // ignore
    }
    applyHeatToMarkers();
  });
  document.getElementById("problemTopN").addEventListener("change", (e) => {
    setSetting("problem_top_n", Number(e.target.value) || 10);
    applyHeatToMarkers();
  });

  document.getElementById("heatTimeRange").addEventListener(
    "input",
    debounce(async (e) => {
      const idx = Number(e.target.value);
      setSetting("heat_ts_index", idx);
      if (state.heatIndex[idx]) document.getElementById("heatTimeLabel").textContent = fmtTs(state.heatIndex[idx]);
      if (state.settings.show_bike_heat) await refreshHeatAtIndex(idx);
    }, 150)
  );

  document.getElementById("toggleLive").addEventListener("change", (e) => {
    setSetting("live", Boolean(e.target.checked));
    ensureLiveTimer(() => refreshSelectedStation({ reason: "live" }));
  });

  document.getElementById("liveInterval").addEventListener("change", (e) => {
    setSetting("live_interval_sec", Number(e.target.value) || 30);
    ensureLiveTimer(() => refreshSelectedStation({ reason: "live" }));
  });

  ensureLiveTimer(() => refreshSelectedStation({ reason: "live" }));

  // --- Search UI ---
  const searchInput = document.getElementById("stationSearch");
  const resultsBox = document.getElementById("stationResults");
  let activeIdx = -1;
  let currentResults = [];

  function closeResults() {
    activeIdx = -1;
    currentResults = [];
    resultsBox.innerHTML = "";
    resultsBox.classList.add("hidden");
  }

  function openResults(items) {
    resultsBox.innerHTML = "";
    currentResults = items;
    activeIdx = items.length ? 0 : -1;

    for (const [idx, s] of items.entries()) {
      const div = document.createElement("div");
      div.className = `result-item${idx === activeIdx ? " active" : ""}`;
      const district = s.district ? ` · ${s.district}` : "";
      div.textContent = `${s.name}${district} · ${s.id}`;
      div.addEventListener("click", () => {
        closeResults();
        selectStationById(s.id, { focus: true });
        refreshSelectedStation({ reason: "search" });
      });
      resultsBox.appendChild(div);
    }

    resultsBox.classList.toggle("hidden", items.length === 0);
  }

  function renderSearch() {
    const q = searchInput.value.trim().toLowerCase();
    if (!q) {
      closeResults();
      return;
    }
    const items = state.stations
      .filter((s) => {
        const text = `${s.name} ${s.id} ${s.district ?? ""}`.toLowerCase();
        return text.includes(q);
      })
      .slice(0, 20);
    openResults(items);
  }

  searchInput.addEventListener("input", renderSearch);
  searchInput.addEventListener("blur", () => setTimeout(closeResults, 150));
  searchInput.addEventListener("keydown", (e) => {
    if (resultsBox.classList.contains("hidden")) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      activeIdx = clamp(activeIdx + 1, 0, currentResults.length - 1);
      openResults(currentResults);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      activeIdx = clamp(activeIdx - 1, 0, currentResults.length - 1);
      openResults(currentResults);
    } else if (e.key === "Enter") {
      e.preventDefault();
      const s = currentResults[activeIdx];
      if (!s) return;
      closeResults();
      selectStationById(s.id, { focus: true });
      refreshSelectedStation({ reason: "search" });
    } else if (e.key === "Escape") {
      closeResults();
    }
  });

  // --- Keyboard shortcuts ---
  document.addEventListener("keydown", (e) => {
    if (isTypingTarget(document.activeElement)) return;

    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      searchInput.focus();
      searchInput.select();
      return;
    }

    if (e.key === "Escape") {
      if (help.isOpen()) help.close();
      closeResults();
      searchInput.blur();
      return;
    }

    if (e.key === "?" || (e.key === "/" && e.shiftKey)) {
      e.preventDefault();
      help.open();
      return;
    }

    const step = e.shiftKey ? 220 : 120;
    if (["w", "a", "s", "d"].includes(e.key.toLowerCase())) {
      e.preventDefault();
      const k = e.key.toLowerCase();
      const dx = k === "a" ? -step : k === "d" ? step : 0;
      const dy = k === "w" ? -step : k === "s" ? step : 0;
      map.panBy([dx, dy], { animate: false });
      return;
    }

    if (e.key.toLowerCase() === "q") {
      e.preventDefault();
      map.zoomOut();
      return;
    }
    if (e.key.toLowerCase() === "e") {
      e.preventDefault();
      map.zoomIn();
      return;
    }

    if (e.key.toLowerCase() === "r") {
      e.preventDefault();
      map.setView([cfg.web_map.center_lat, cfg.web_map.center_lon], cfg.web_map.zoom);
      return;
    }

    if (e.key.toLowerCase() === "f") {
      e.preventDefault();
      const s = state.stationById.get(state.selectedStationId);
      if (s) map.setView([s.lat, s.lon], Math.max(map.getZoom(), 13));
      return;
    }

    if (e.key.toLowerCase() === "b") {
      e.preventDefault();
      const next = !state.settings.show_nearby_bikes;
      document.getElementById("toggleNearbyBikes").checked = next;
      setSetting("show_nearby_bikes", next);
      debouncedRefresh();
      return;
    }

    if (e.key.toLowerCase() === "c") {
      e.preventDefault();
      const next = !state.settings.show_buffer;
      document.getElementById("toggleBuffer").checked = next;
      setSetting("show_buffer", next);
      setBufferCircle(state.stationById.get(state.selectedStationId));
      debouncedRefresh();
      return;
    }

    if (e.key.toLowerCase() === "l") {
      e.preventDefault();
      const next = !state.settings.show_links;
      document.getElementById("toggleLinks").checked = next;
      setSetting("show_links", next);
      debouncedRefresh();
      return;
    }

    if (e.key.toLowerCase() === "g") {
      e.preventDefault();
      const order = ["15min", "hour", "day"];
      const idx = order.indexOf(state.settings.granularity);
      const next = order[(idx + 1) % order.length];
      document.getElementById("granularitySelect").value = next;
      setSetting("granularity", next);
      debouncedRefresh();
      return;
    }

    if (e.key === "[" && state.settings.join_method === "buffer") {
      e.preventDefault();
      const delta = e.shiftKey ? 200 : 50;
      const next = clamp(Number(state.settings.radius_m) - delta, 50, 5000);
      document.getElementById("radiusInput").value = String(next);
      document.getElementById("radiusNumber").value = String(next);
      setSetting("radius_m", next);
      setBufferCircle(state.stationById.get(state.selectedStationId));
      debouncedRefresh();
      return;
    }

    if (e.key === "]" && state.settings.join_method === "buffer") {
      e.preventDefault();
      const delta = e.shiftKey ? 200 : 50;
      const next = clamp(Number(state.settings.radius_m) + delta, 50, 5000);
      document.getElementById("radiusInput").value = String(next);
      document.getElementById("radiusNumber").value = String(next);
      setSetting("radius_m", next);
      setBufferCircle(state.stationById.get(state.selectedStationId));
      debouncedRefresh();
      return;
    }

    if (e.key.toLowerCase() === "n" || e.key.toLowerCase() === "p") {
      const ids = state.stations.map((s) => s.id);
      const curIdx = ids.indexOf(state.selectedStationId);
      if (curIdx < 0) return;
      const nextIdx = e.key.toLowerCase() === "n" ? curIdx + 1 : curIdx - 1;
      const wrapped = (nextIdx + ids.length) % ids.length;
      const nextId = ids[wrapped];
      selectStationById(nextId, { focus: true });
      refreshSelectedStation({ reason: "nav" });
      return;
    }
  });

  // Auto-select first station in demo mode to make the UI feel alive.
  if (cfg.demo_mode && state.stations.length) {
    selectStationById(state.stations[0].id, { focus: false });
    refreshSelectedStation({ reason: "auto" });
  }
}

main().catch((err) => {
  console.error(err);
  alert(`Failed to load app: ${err.message}`);
});
