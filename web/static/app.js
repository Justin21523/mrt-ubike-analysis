async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  return await res.json();
}

const CLUSTER_COLORS = [
  "#2a6fdb",
  "#db2a6f",
  "#2adb6f",
  "#dbb52a",
  "#8a2adb",
  "#2adbb5",
  "#db6f2a",
];

function clusterColor(cluster) {
  if (cluster == null) return "#2a6fdb";
  const idx = Number(cluster);
  if (!Number.isFinite(idx)) return "#2a6fdb";
  return CLUSTER_COLORS[Math.abs(idx) % CLUSTER_COLORS.length];
}

function seriesLabel(series) {
  if (!series) return "No data";
  const proxy = series.is_proxy ? " (proxy)" : "";
  return `${series.metric}${proxy}`;
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
    hint.textContent =
      "No factors available. In real data mode, run: python scripts/build_features.py";
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

  for (const c of payload.correlations?.slice(0, 8) ?? []) {
    const li = document.createElement("li");
    li.textContent = `${c.feature} · corr=${Number(c.correlation).toFixed(3)} · n=${c.n}`;
    list.appendChild(li);
  }
}

async function main() {
  const stations = await fetchJson("/stations");

  const map = L.map("map").setView([25.0375, 121.5637], 12);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(map);

  const metroChart = buildChart(document.getElementById("metroChart"), "metro");
  const bikeChart = buildChart(document.getElementById("bikeChart"), "bike");

  const byId = new Map(stations.map((s) => [s.id, s]));
  const markerById = new Map();

  const stationSelect = document.getElementById("stationSelect");
  stationSelect.innerHTML = `<option value="">Select...</option>`;
  for (const s of stations) {
    const opt = document.createElement("option");
    opt.value = s.id;
    const district = s.district ? ` · ${s.district}` : "";
    opt.textContent = `${s.name}${district}`;
    stationSelect.appendChild(opt);
  }

  async function loadStationById(id) {
    const s = byId.get(id);
    if (!s) return;
    stationSelect.value = s.id;

    document.getElementById("stationName").textContent = s.name;
    const metaParts = [s.id, s.city, s.district, s.cluster == null ? null : `cluster ${s.cluster}`].filter(Boolean);
    document.getElementById("stationMeta").textContent = metaParts.join(" · ");

    map.setView([s.lat, s.lon], Math.max(map.getZoom(), 13));

    const ts = await fetchJson(`/station/${encodeURIComponent(s.id)}/timeseries`);
    const metroSeries = ts.series.find((x) => x.metric.startsWith("metro"));
    const bikeSeries = ts.series.find((x) => x.metric.startsWith("bike"));
    const metro = metroSeries?.points ?? [];
    const bike = bikeSeries?.points ?? [];

    document.getElementById("metroChartTitle").textContent = seriesLabel(metroSeries);
    document.getElementById("bikeChartTitle").textContent = seriesLabel(bikeSeries);
    metroChart.data.datasets[0].label = seriesLabel(metroSeries);
    bikeChart.data.datasets[0].label = seriesLabel(bikeSeries);

    setChartData(metroChart, metro);
    setChartData(bikeChart, bike);

    const nearby = await fetchJson(`/station/${encodeURIComponent(s.id)}/nearby_bike`);
    setNearbyList(nearby);

    const factors = await fetchJson(`/station/${encodeURIComponent(s.id)}/factors`);
    setFactors(factors);

    const similar = await fetchJson(`/station/${encodeURIComponent(s.id)}/similar`);
    setSimilarStations(similar, loadStationById);
  }

  stationSelect.addEventListener("change", () => {
    const id = stationSelect.value;
    if (id) loadStationById(id);
  });

  for (const s of stations) {
    const marker = L.circleMarker([s.lat, s.lon], {
      radius: 6,
      color: clusterColor(s.cluster),
      fillColor: clusterColor(s.cluster),
      fillOpacity: 0.9,
      weight: 2,
    });

    marker.on("click", () => loadStationById(s.id));

    marker.addTo(map);
    markerById.set(s.id, marker);
  }

  const overview = await fetchJson("/analytics/overview");
  setOverview(overview);
}

main().catch((err) => {
  console.error(err);
  alert(`Failed to load app: ${err.message}`);
});
