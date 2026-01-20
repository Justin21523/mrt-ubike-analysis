async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  return await res.json();
}

function setStatusText(text) {
  const el = document.getElementById("statusText");
  if (el) el.textContent = text;
}

function fmtAge(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s)) return "—";
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  if (s < 86400) return `${(s / 3600).toFixed(1)}h`;
  return `${(s / 86400).toFixed(1)}d`;
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

function clamp(n, lo, hi) {
  const x = Number(n);
  if (!Number.isFinite(x)) return lo;
  return Math.max(lo, Math.min(hi, x));
}

function renderList(root, title, items, valueLabel) {
  root.innerHTML = "";
  const h = document.createElement("div");
  h.className = "hint";
  h.textContent = `${title} · Top ${items.length}`;
  root.appendChild(h);

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
    name.textContent = it.name || it.station_id || "—";
    const meta = document.createElement("div");
    meta.className = "hint mono";
    meta.style.margin = "2px 0 0 0";
    meta.textContent = `${valueLabel}: ${it.value_txt ?? it.value ?? "—"}`;
    left.appendChild(name);
    left.appendChild(meta);

    const a = document.createElement("a");
    a.className = "btn btn-primary";
    a.href = `/explorer#station_id=${encodeURIComponent(String(it.station_id))}`;
    a.textContent = "Open";

    li.appendChild(left);
    li.appendChild(a);
    list.appendChild(li);
  }
  root.appendChild(list);
}

async function main() {
  setStatusText("Loading…");
  const [status, meta] = await Promise.all([fetchJson("/status"), fetchJson("/meta")]);
  setModePill(Boolean(status?.demo_mode));
  setWeatherPill(meta);

  const modeEl = document.getElementById("insightsMode");
  const topKEl = document.getElementById("insightsTopK");
  const listEl = document.getElementById("insightsList");
  const btn = document.getElementById("btnRefreshInsights");
  if (!modeEl || !topKEl || !listEl || !btn) return;

  const refresh = async () => {
    const mode = String(modeEl.value || "hotspots");
    const topK = clamp(topKEl.value, 1, 50);
    const city = status?.tdx?.bike_cities?.[0] ?? "Taipei";
    listEl.innerHTML = `<div class="hint">Loading…</div>`;

    if (mode === "rainy_risk") {
      const risk = await fetchJson(`/insights/rain_risk_now?city=${encodeURIComponent(city)}&top_k=${topK}`);
      const items =
        (risk?.items ?? []).map((it) => ({
          station_id: it.station_id,
          name: it.name || it.station_id,
          value: it.mean_available_bikes,
          value_txt: Number.isFinite(Number(it.mean_available_bikes)) ? String(Math.round(Number(it.mean_available_bikes))) : "—",
        })) ?? [];
      renderList(listEl, risk?.is_rainy_now ? "Rainy-risk (now)" : "Rainy-risk (not raining now)", items, "mean_available");
      return;
    }

    // For hotspots/coldspots we lean on backend ordering (it knows the latest heat ts)
    if (mode === "hotspots" || mode === "coldspots") {
      const payload = await fetchJson(`/insights/hotspots?metric=available&agg=sum&top_k=${topK}`);
      const items = (mode === "hotspots" ? payload?.hot : payload?.cold) ?? [];
      renderList(listEl, mode === "hotspots" ? "Hotspots" : "Coldspots", items, "available");
      return;
    }

    // shortage / pressure: use hotspots endpoint with different metric when available in backend
    if (mode === "shortage") {
      const payload = await fetchJson(`/insights/hotspots?metric=available&agg=sum&top_k=${topK}`);
      const items = payload?.cold ?? [];
      renderList(listEl, "Shortage (low availability)", items, "available");
      return;
    }
    if (mode === "pressure") {
      const payload = await fetchJson(`/insights/hotspots?metric=rent_proxy&agg=sum&top_k=${topK}`);
      const items = payload?.hot ?? [];
      renderList(listEl, "Pressure (high rent_proxy)", items, "rent_proxy");
      return;
    }
  };

  btn.addEventListener("click", () => refresh().catch((e) => (listEl.innerHTML = `<div class="hint">Error: ${e.message}</div>`)));
  modeEl.addEventListener("change", () => refresh().catch(() => {}));
  topKEl.addEventListener("change", () => refresh().catch(() => {}));

  await refresh();
  setStatusText("Ready");
}

main().catch((e) => {
  console.error(e);
  setStatusText(`Error: ${e.message}`);
});

