async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  return await res.json();
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

function setSimilarStations(items) {
  const list = document.getElementById("similarList");
  list.innerHTML = "";
  for (const s of items ?? []) {
    const li = document.createElement("li");
    const name = s.name ?? s.id;
    const cluster = s.cluster == null ? "" : ` · cluster ${s.cluster}`;
    li.textContent = `${name} · d=${s.distance.toFixed(3)}${cluster}`;
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

  for (const s of stations) {
    const marker = L.circleMarker([s.lat, s.lon], {
      radius: 6,
      color: "#2a6fdb",
      fillColor: "#2a6fdb",
      fillOpacity: 0.9,
      weight: 2,
    });

    marker.on("click", async () => {
      document.getElementById("stationName").textContent = s.name;
      document.getElementById("stationMeta").textContent = `${s.id} · ${s.city ?? ""}`;

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
      setSimilarStations(similar);
    });

    marker.addTo(map);
  }
}

main().catch((err) => {
  console.error(err);
  alert(`Failed to load app: ${err.message}`);
});
