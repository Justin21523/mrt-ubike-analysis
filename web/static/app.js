async function fetchJson(url, { signal } = {}) {
  const res = await fetch(url, { signal });
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  return await res.json();
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

function buildChart(canvas, label) {
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
    show_buffer: true,
    show_links: false,
    live: false,
    live_interval_sec: 30,
    left_collapsed: false,
    right_collapsed: false,
  };
}

function mergeSettings(base, patch) {
  const out = { ...base };
  for (const [k, v] of Object.entries(patch || {})) out[k] = v;
  return out;
}

function setModePill(cfg) {
  const pill = document.getElementById("modePill");
  pill.textContent = cfg.demo_mode ? "Demo mode" : "Real data mode";
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

function updateStationMeta(station) {
  if (!station) {
    document.getElementById("stationName").textContent = "Select a station";
    document.getElementById("stationMeta").textContent = "";
    return;
  }
  document.getElementById("stationName").textContent = station.name;
  const metaParts = [
    station.id,
    station.city,
    station.district,
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
    liveTimer: null,
    aborter: null,
    selectedMarker: null,
  };

  initSplitters(state);
  const help = initHelpModal();

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

  map.on("moveend zoomend", () => {
    const c = map.getCenter();
    document.getElementById("statusCoords").textContent = `lat=${c.lat.toFixed(4)} lon=${c.lng.toFixed(4)} z=${map.getZoom()}`;
  });

  const metroChart = buildChart(document.getElementById("metroChart"), "metro");
  const bikeChart = buildChart(document.getElementById("bikeChart"), "bike");

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
    document.getElementById("toggleBuffer").checked = Boolean(state.settings.show_buffer);
    document.getElementById("toggleLinks").checked = Boolean(state.settings.show_links);
    document.getElementById("toggleLive").checked = Boolean(state.settings.live);
    document.getElementById("liveInterval").value = String(state.settings.live_interval_sec);

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
      const m = L.circleMarker([b.lat, b.lon], {
        radius: 5,
        color: "rgba(219,181,42,0.95)",
        fillColor: "rgba(219,181,42,0.35)",
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
          { color: "rgba(255,255,255,0.22)", weight: 1 }
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
        fetchJson(`/station/${encodeURIComponent(id)}/nearby_bike${qs({ ...joinParams, limit: 50 })}`, { signal: ctrl.signal }),
        fetchJson(`/station/${encodeURIComponent(id)}/factors`, { signal: ctrl.signal }),
        fetchJson(`/station/${encodeURIComponent(id)}/similar${qs(simParams)}`, { signal: ctrl.signal }),
      ]);

      state.lastTimeseries = ts;

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
      setNearbyList(nearby);
      setFactors(factors);
      setSimilarStations(similar, (sid) => {
        selectStationById(sid, { focus: true });
        refreshSelectedStation({ reason: "similar" });
      });

      renderNearbyOnMap(station, nearby);
      setBufferCircle(station);

      setStatusText(`Loaded ${id} · ${ts.granularity} · ${nearby.length} bikes`);
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
  state.stations = await fetchJson("/stations");
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
    });
    marker.on("click", () => {
      selectStationById(s.id, { focus: true });
      refreshSelectedStation({ reason: "click" });
    });
    marker.addTo(metroLayer);
    state.metroMarkerById.set(s.id, marker);
  }

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
