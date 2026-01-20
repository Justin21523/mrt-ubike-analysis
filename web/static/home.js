const MBA = window.MBA;

function oneSentence(status, meta) {
  const h = status?.health ?? {};
  const w = meta?.meta?.external?.weather_collector ?? null;
  const parts = [];
  parts.push(status?.demo_mode ? "Demo mode" : "Real mode");
  parts.push(`Bronze ${MBA.fmtAge(h.bronze_bike_availability_age_s)}`);
  parts.push(
    `Silver ${MBA.fmtAge(Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity)))}`
  );
  if (w) parts.push(`Weather ${w.stale ? "stale" : "ok"} (${MBA.fmtAge(w.heartbeat_age_s)})${w.is_rainy_now ? " · raining now" : ""}`);
  if (Number(h.metro_tdx_404_count || 0)) parts.push(`Metro 404 ×${h.metro_tdx_404_count}`);
  return parts.join(" · ");
}

function buildCredibilityCard(status, meta) {
  const w = meta?.meta?.external?.weather_collector ?? null;
  const resolved = meta?.meta ?? {};
  const badge = {
    tone: status?.demo_mode ? "warn" : "ok",
    text: status?.demo_mode ? "demo" : "real",
  };
  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Data Credibility",
    title: "Sources · freshness · traceability",
    badge,
    right: `<span class="mono">build ${MBA.shortId(resolved.silver_build_id || meta?.silver_build_meta?.build_id)}</span>`,
    actions: [
      {
        label: "Download JSON",
        onClick: () => MBA.downloadJson(`metrobikeatlas-meta-${new Date().toISOString()}.json`, { status, meta }),
      },
      {
        label: "Copy summary",
        onClick: async () => {
          await MBA.copyText(oneSentence(status, meta));
          MBA.setStatusText("Copied");
        },
      },
      { type: "link", label: "Open Ops", href: "/ops", primary: true },
    ],
  });
  const lines = [];
  lines.push(`<div class="hint">Storage: <span class="mono">${resolved.fallback_source || "—"}</span></div>`);
  lines.push(
    `<div class="hint">Silver build: <span class="mono">${MBA.shortId(resolved.silver_build_id)}</span> · hash <span class="mono">${MBA.shortId(resolved.inputs_hash, 10)}</span></div>`
  );
  if (w) {
    lines.push(
      `<div class="hint">Weather: <span class="mono">${w.stale ? "stale" : "ok"}</span> · age <span class="mono">${MBA.fmtAge(w.heartbeat_age_s)}</span>${w.is_rainy_now ? " · raining now" : ""}</div>`
    );
  }
  body.innerHTML = lines.join("");
  return card;
}

function buildStoryParagraphCard() {
  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Story Structure",
    title: "Problem → Evidence → Implication → Action",
    badge: { tone: "muted", text: "guide" },
  });
  body.innerHTML = `
    <div><span class="mono">Problem</span> · 哪些 MRT 站附近共享單車供需失衡？</div>
    <div><span class="mono">Evidence</span> · 用 heat snapshot + 時間序列展示尖峰。</div>
    <div><span class="mono">Implication</span> · 影響轉乘體驗與營運調度成本。</div>
    <div><span class="mono">Action</span> · 選 Top N 問題站，提出 2–4 個策略選項。</div>
  `;
  return card;
}

function buildKpisCard(status) {
  const h = status?.health ?? {};
  const { card, body } = MBA.createCard({
    tone: "support",
    kicker: "Key Indicators",
    title: "Operational KPIs",
    badge: { tone: "ok", text: "live" },
    actions: [
      { type: "link", label: "Go to Ops", href: "/ops", primary: true },
      { type: "link", label: "Open Explorer", href: "/explorer" },
    ],
  });
  body.innerHTML = `
    <div class="health-cards">
      <div class="health-card">
        <div class="health-title">Collector</div>
        <div class="health-value">${h.collector_running ? "running" : "stopped"}</div>
        <div class="health-meta mono">${h.collector_last_ok_utc || ""}</div>
      </div>
      <div class="health-card">
        <div class="health-title">Bronze freshness</div>
        <div class="health-value">${MBA.fmtAge(h.bronze_bike_availability_age_s)}</div>
        <div class="health-meta mono">${h.bronze_bike_availability_last_utc || ""}</div>
      </div>
      <div class="health-card">
        <div class="health-title">Silver freshness</div>
        <div class="health-value">${MBA.fmtAge(
          Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity))
        )}</div>
        <div class="health-meta mono">links/bike_timeseries</div>
      </div>
    </div>
  `;
  return card;
}

function buildTodayCard(status, meta) {
  const conclusion = oneSentence(status, meta);
  const { card, body } = MBA.createCard({
    tone: "primary",
    kicker: "Executive Summary",
    title: "Today (one sentence)",
    badge: { tone: "ok", text: "brief" },
    actions: [
      { type: "link", label: "Open Explorer", href: "/explorer", primary: true },
      {
        label: "Copy",
        onClick: async () => {
          await MBA.copyText(conclusion);
          MBA.setStatusText("Copied");
        },
      },
    ],
  });
  body.textContent = conclusion;
  return card;
}

function buildStoryProblemCard({ status, meta, usage }) {
  const city = usage?.city || status?.tdx?.bike_cities?.[0] || "Taipei";
  const rainy = Number(usage?.precip_total_mm ?? 0) > 0;
  const p = Number(usage?.precip_total_mm);
  const msg =
    usage && usage.precip_total_mm != null
      ? `${city} · ${rainy ? `rain ${p.toFixed(1)}mm` : "no rain"} · rent_proxy ${Math.round(Number(usage.rent_proxy_total ?? 0))} · return_proxy ${Math.round(
          Number(usage.return_proxy_total ?? 0)
        )}`
      : "Weather usage insight not available yet.";

  const heatParams = rainy ? { show_bike_heat: 1, heat_metric: "rent_proxy", heat_agg: "sum" } : { show_bike_heat: 1, heat_metric: "available", heat_agg: "sum" };
  const { card, body } = MBA.createCard({
    tone: "support",
    kicker: "Story Card · Evidence",
    title: "Rain × Usage (24h)",
    badge: { tone: rainy ? "warn" : "ok", text: rainy ? "rain" : "clear" },
    actions: [
      { type: "link", label: "Open Explorer (heat)", href: MBA.explorerHref(heatParams), primary: true },
      {
        label: "Download JSON",
        onClick: () => MBA.downloadJson(`metrobikeatlas-rain-usage-${new Date().toISOString()}.json`, { status, meta, usage }),
      },
    ],
  });
  body.textContent = msg;
  return card;
}

function buildStoryRiskCard({ status, meta, risk }) {
  const w = meta?.meta?.external?.weather_collector ?? null;
  const rainingNow = Boolean(w?.is_rainy_now);
  const items = Array.isArray(risk?.items) ? risk.items.slice(0, 5) : [];
  const { card, body } = MBA.createCard({
    tone: "support",
    kicker: "Story Card · Actionable list",
    title: "Rain-risk stations (now)",
    badge: { tone: rainingNow ? "warn" : "muted", text: rainingNow ? "raining" : "not raining" },
    actions: [
      { type: "link", label: "Open Insights", href: "/insights", primary: true },
      {
        label: "Download JSON",
        onClick: () => MBA.downloadJson(`metrobikeatlas-rain-risk-${new Date().toISOString()}.json`, { status, meta, risk }),
      },
    ],
  });

  if (!rainingNow) {
    body.innerHTML = `<div class="hint">Not raining now, so this list is informational.</div>`;
    return card;
  }
  if (!items.length) {
    body.innerHTML = `<div class="hint">Raining now, but list is empty.</div>`;
    return card;
  }
  const list = document.createElement("ul");
  list.className = "list";
  for (const it of items) {
    const li = document.createElement("li");
    const v = Number(it.mean_available_bikes);
    const href = MBA.explorerHref({ station_id: it.station_id, show_bike_heat: 1, heat_metric: "available", heat_agg: "sum" });
    li.innerHTML = `<div style="display:flex; justify-content:space-between; gap:10px; align-items:center;">
      <div style="min-width:0;">
        <div style="font-weight:800; font-size:12px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${it.name || it.station_id}</div>
        <div class="hint mono" style="margin:2px 0 0 0;">mean_available: ${Number.isFinite(v) ? Math.round(v) : "—"}</div>
      </div>
      <a class="btn btn-primary" href="${href}">Open</a>
    </div>`;
    list.appendChild(li);
  }
  body.appendChild(list);
  return card;
}

function buildNextActionsCard(status, meta) {
  const { card, body } = MBA.createCard({
    tone: "primary",
    kicker: "Next",
    title: "What to do next",
    badge: { tone: "ok", text: "action" },
    actions: [
      { type: "link", label: "Start/Build (Ops)", href: "/ops", primary: true },
      { type: "link", label: "Explore map", href: MBA.explorerHref({ show_bike_heat: 1, heat_metric: "available", heat_agg: "sum" }) },
      { type: "link", label: "Read methods", href: "/about" },
    ],
  });
  const h = status?.health ?? {};
  const items = [];
  if (!h.collector_running) items.push("Collector is stopped → start collector.");
  const silverAge = Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity));
  if (!Number.isFinite(silverAge)) items.push("Silver missing → run Build Silver.");
  if (Number(h.metro_tdx_404_count || 0)) items.push("Metro 404 → use external metro_stations.csv fallback (see Ops).");
  if (!items.length) items.push("Data looks healthy → pick a station and tell the story with evidence + options.");
  body.innerHTML = `<ol style="margin:0; padding-left:18px;">${items.map((x) => `<li>${x}</li>`).join("")}</ol>`;
  return card;
}

function buildMethodsCard() {
  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Methods",
    title: "Definitions & limitations",
    badge: { tone: "muted", text: "read" },
    actions: [{ type: "link", label: "Open About", href: "/about", primary: true }],
  });
  body.innerHTML =
    `<div class="hint">This project uses proxy signals derived from bike snapshots and city-level weather. Always validate before policy decisions.</div>`;
  return card;
}

async function main() {
  MBA.setStatusText("Loading…");
  const root = document.getElementById("homeCards");
  if (!root) return;

  const [status, meta] = await Promise.all([MBA.fetchJson("/status"), MBA.fetchJson("/meta")]);
  MBA.setModePill(Boolean(status?.demo_mode));
  MBA.setWeatherPill(meta);

  const city = status?.tdx?.bike_cities?.[0] ?? "Taipei";
  const [usage, risk] = await Promise.all([
    MBA.fetchJson(`/insights/weather_usage?city=${encodeURIComponent(city)}&hours=24`).catch(() => null),
    MBA.fetchJson(`/insights/rain_risk_now?city=${encodeURIComponent(city)}&top_k=5`).catch(() => null),
  ]);

  root.innerHTML = "";
  root.appendChild(buildCredibilityCard(status, meta));
  root.appendChild(buildKpisCard(status));
  root.appendChild(buildTodayCard(status, meta));
  root.appendChild(buildStoryParagraphCard());
  root.appendChild(buildStoryProblemCard({ status, meta, usage }));
  root.appendChild(buildStoryRiskCard({ status, meta, risk }));
  root.appendChild(buildNextActionsCard(status, meta));
  root.appendChild(buildMethodsCard());

  MBA.setStatusText("Ready");
}

main().catch((e) => {
  console.error(e);
  MBA.setStatusText(`Error: ${e.message}`);
});
