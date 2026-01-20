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

function setStatusText(text) {
  const el = document.getElementById("statusText");
  if (el) el.textContent = text;
}

function kpiCard({ label, value, tone, meta }) {
  const el = document.createElement("div");
  el.className = `briefing-kpi ${tone || ""}`;
  el.innerHTML = `
    <div class="briefing-kpi-label">${label}</div>
    <div class="briefing-kpi-value">${value}</div>
    <div class="briefing-kpi-meta mono">${meta || ""}</div>
  `;
  return el;
}

function linkToExplorer(stationId) {
  const a = document.createElement("a");
  a.className = "btn btn-primary";
  a.href = `/explorer#station_id=${encodeURIComponent(String(stationId))}`;
  a.textContent = "Open in Explorer";
  return a;
}

async function main() {
  setStatusText("Loading…");
  const [status, meta] = await Promise.all([fetchJson("/status"), fetchJson("/meta")]);
  setModePill(Boolean(status?.demo_mode));
  setWeatherPill(meta);

  const h = status?.health ?? {};
  const kpis = [];
  kpis.push(
    kpiCard({
      label: "Collector",
      value: h.collector_running ? "running" : "stopped",
      tone: h.collector_running ? "ok" : "warn",
      meta: h.collector_last_ok_utc ? `last ok ${h.collector_last_ok_utc}` : "",
    })
  );
  kpis.push(
    kpiCard({
      label: "Bronze freshness",
      value: fmtAge(h.bronze_bike_availability_age_s),
      tone: h.bronze_bike_availability_age_s != null && Number(h.bronze_bike_availability_age_s) > 3600 ? "warn" : "ok",
      meta: h.bronze_bike_availability_last_utc || "",
    })
  );
  kpis.push(
    kpiCard({
      label: "Silver freshness",
      value: fmtAge(
        Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity))
      ),
      tone:
        h.silver_metro_bike_links_age_s == null && h.silver_bike_timeseries_age_s == null
          ? "warn"
          : Math.max(Number(h.silver_metro_bike_links_age_s ?? 0), Number(h.silver_bike_timeseries_age_s ?? 0)) > 86400
            ? "warn"
            : "ok",
      meta: "links/bike_timeseries",
    })
  );
  if (Number(h.metro_tdx_404_count || 0) > 0) {
    kpis.push(
      kpiCard({
        label: "Metro TDX 404",
        value: String(h.metro_tdx_404_count),
        tone: "bad",
        meta: h.metro_tdx_404_last_utc ? `last ${h.metro_tdx_404_last_utc}` : "",
      })
    );
  }
  const kpisEl = document.getElementById("homeKpis");
  if (kpisEl) {
    kpisEl.innerHTML = "";
    for (const el of kpis) kpisEl.appendChild(el);
  }

  const homeConclusion = document.getElementById("homeConclusion");
  if (homeConclusion) {
    const w = meta?.meta?.external?.weather_collector ?? null;
    const pieces = [];
    pieces.push(status?.demo_mode ? "Demo mode" : "Real mode");
    pieces.push(`Bronze ${fmtAge(h.bronze_bike_availability_age_s)}`);
    pieces.push(
      `Silver ${fmtAge(Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity)))}`
    );
    if (w) pieces.push(`Weather ${w.stale ? "stale" : "ok"} (${fmtAge(w.heartbeat_age_s)})${w.is_rainy_now ? " · raining now" : ""}`);
    if (Number(h.metro_tdx_404_count || 0)) pieces.push(`Metro 404 ×${h.metro_tdx_404_count}`);
    homeConclusion.textContent = pieces.join(" · ");
  }

  // Story cards
  const city = status?.tdx?.bike_cities?.[0] ?? "Taipei";
  const [usage, risk] = await Promise.all([
    fetchJson(`/insights/weather_usage?city=${encodeURIComponent(city)}&hours=24`).catch(() => null),
    fetchJson(`/insights/rain_risk_now?city=${encodeURIComponent(city)}&top_k=5`).catch(() => null),
  ]);

  const rainUsage = document.getElementById("homeRainUsage");
  if (rainUsage) {
    if (usage?.precip_total_mm != null) {
      rainUsage.textContent =
        `${usage.city || city} · ` +
        `${Number(usage.precip_total_mm) > 0 ? `rain ${Number(usage.precip_total_mm).toFixed(1)}mm` : "no rain"}` +
        `${usage.rainy_hours != null ? ` · rainy hours ${usage.rainy_hours}` : ""}` +
        ` · rent_proxy ${Math.round(Number(usage.rent_proxy_total ?? 0))}` +
        ` · return_proxy ${Math.round(Number(usage.return_proxy_total ?? 0))}`;
    } else {
      rainUsage.textContent = "Weather usage insight not available yet.";
    }
  }

  const rainRisk = document.getElementById("homeRainRisk");
  if (rainRisk) {
    rainRisk.innerHTML = "";
    if (risk?.is_rainy_now && Array.isArray(risk.items) && risk.items.length) {
      const hint = document.createElement("div");
      hint.className = "hint";
      hint.textContent = "Raining now · Top stations by low nearby availability";
      rainRisk.appendChild(hint);
      const row = document.createElement("div");
      row.className = "row row-actions";
      for (const it of risk.items.slice(0, 5)) {
        const b = document.createElement("a");
        b.className = "btn";
        b.href = `/explorer#station_id=${encodeURIComponent(String(it.station_id))}`;
        const v = Number(it.mean_available_bikes);
        b.textContent = `${it.name || it.station_id} · ${Number.isFinite(v) ? Math.round(v) : "—"}`;
        row.appendChild(b);
      }
      rainRisk.appendChild(row);
    } else {
      const msg = document.createElement("div");
      msg.className = "hint";
      msg.textContent = (meta?.meta?.external?.weather_collector?.is_rainy_now ?? false)
        ? "Raining now, but rain-risk list is empty."
        : "Not raining now.";
      rainRisk.appendChild(msg);
    }
  }

  // Callouts (reuse /status alerts)
  const callouts = document.getElementById("homeCallouts");
  if (callouts) {
    callouts.innerHTML = "";
    for (const a of (status?.alerts ?? []).slice(0, 4)) {
      const div = document.createElement("div");
      div.className = `briefing-callout ${(a.level || "info").toLowerCase()}`;
      div.innerHTML = `<div class="briefing-callout-title">${a.title || "Note"}</div><div class="briefing-callout-body">${a.message || ""}</div>`;
      callouts.appendChild(div);
    }
  }

  setStatusText("Ready");
}

main().catch((e) => {
  console.error(e);
  setStatusText(`Error: ${e.message}`);
});

