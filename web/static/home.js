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

function buildPolicyOptionCard({ title, kicker = "Policy option", badge, impact, beneficiaries, risk, needs, actions, payload }) {
  const { card, body } = MBA.createCard({
    tone: "support",
    kicker,
    title,
    badge: badge || { tone: "ok", text: "policy" },
    actions: [
      ...(actions || []),
      {
        label: "Copy",
        onClick: async () => {
          const lines = [];
          lines.push(title);
          if (impact) lines.push(`Impact: ${impact}`);
          if (beneficiaries) lines.push(`Beneficiaries: ${beneficiaries}`);
          if (risk) lines.push(`Risk: ${risk}`);
          if (needs) lines.push(`Needs: ${needs}`);
          await MBA.copyText(lines.join("\n"));
          MBA.setStatusText("Copied");
        },
      },
      {
        label: "Download JSON",
        onClick: () => MBA.downloadJson(`metrobikeatlas-policy-${new Date().toISOString()}.json`, payload || {}),
      },
    ],
  });

  body.innerHTML = `
    <div><span class="mono">Impact</span> · ${impact || "—"}</div>
    <div><span class="mono">Beneficiaries</span> · ${beneficiaries || "—"}</div>
    <div><span class="mono">Risk</span> · ${risk || "—"}</div>
    <div><span class="mono">Needs</span> · ${needs || "—"}</div>
  `;
  return card;
}

function buildPolicyOptionsSection({ status, meta, hotspotsAvail, hotspotsRent, rainRisk }) {
  const h = status?.health ?? {};
  const w = meta?.meta?.external?.weather_collector ?? null;
  const demo = Boolean(status?.demo_mode);
  const cards = [];

  // 1) Data source / pipeline reliability
  if (!demo && Number(h.metro_tdx_404_count || 0) > 0) {
    cards.push(
      buildPolicyOptionCard({
        title: "Fix data source: Metro stations unavailable (TDX 404)",
        badge: { tone: "bad", text: "critical" },
        impact: "Restores station layer/links so insights remain trustworthy.",
        beneficiaries: "Decision makers, operators, analysts",
        risk: "Using fallback may differ from official station definitions.",
        needs: "Provide external metro stations CSV or adjust TDX config/path.",
        actions: [
          { type: "link", label: "Open Ops (fix)", href: "/ops", primary: true },
          { type: "link", label: "Open About", href: "/about" },
        ],
        payload: { kind: "metro_404", status, meta },
      })
    );
  }

  // 2) Shortage: low availability (use coldspots from available)
  const shortage = (hotspotsAvail?.cold ?? []).slice(0, 3);
  if (shortage.length) {
    const top = shortage[0];
    cards.push(
      buildPolicyOptionCard({
        title: "Increase supply at shortage hotspots (low availability)",
        badge: { tone: "warn", text: "shortage" },
        impact: "Reduce 'no-bike' incidents and improve last-mile transfer reliability.",
        beneficiaries: "Commuters, transfer passengers",
        risk: "If shortage is only peak-hour, fixed expansion may be underutilized off-peak.",
        needs: "1–2 weeks of data to verify peak patterns; confirm with operator logs.",
        actions: [
          {
            type: "link",
            label: "Open Explorer (focus)",
            href: MBA.explorerHref({
              station_id: top.station_id,
              show_bike_heat: 1,
              heat_metric: "available",
              heat_agg: "sum",
            }),
            primary: true,
          },
          { type: "link", label: "Open Insights", href: "/insights" },
        ],
        payload: { kind: "shortage", top, list: shortage, hotspotsAvail },
      })
    );
  }

  // 3) Pressure: high rent_proxy (use hotspots from rent_proxy)
  const pressure = (hotspotsRent?.hot ?? []).slice(0, 3);
  if (pressure.length) {
    const top = pressure[0];
    cards.push(
      buildPolicyOptionCard({
        title: "Rebalancing strategy for high demand pressure (rent_proxy)",
        badge: { tone: "warn", text: "pressure" },
        impact: "Mitigates peak-hour demand spikes by targeted rebalancing and guidance.",
        beneficiaries: "High-frequency users, operators",
        risk: "Proxy demand may over/under-estimate true demand in certain areas.",
        needs: "Validate proxy against ridership/operational data; define time-of-day strategy.",
        actions: [
          {
            type: "link",
            label: "Open Explorer (rent heat)",
            href: MBA.explorerHref({
              station_id: top.station_id,
              show_bike_heat: 1,
              heat_metric: "rent_proxy",
              heat_agg: "sum",
            }),
            primary: true,
          },
          { type: "link", label: "Open Ops", href: "/ops" },
        ],
        payload: { kind: "pressure", top, list: pressure, hotspotsRent },
      })
    );
  }

  // 4) Rain contingency
  const rainingNow = Boolean(w?.is_rainy_now);
  const riskItems = (rainRisk?.items ?? []).slice(0, 3);
  if (rainingNow && riskItems.length) {
    const top = riskItems[0];
    cards.push(
      buildPolicyOptionCard({
        title: "Rain-day contingency: pre-position supply near risk stations",
        badge: { tone: "warn", text: "rain" },
        impact: "Improves service resilience during rain; reduces sudden shortages.",
        beneficiaries: "Commuters, vulnerable users during bad weather",
        risk: "Weather is city-level estimate; microclimates may differ by district.",
        needs: "Define rain threshold and lead time; consider communication/signage strategy.",
        actions: [
          {
            type: "link",
            label: "Open Explorer (risk station)",
            href: MBA.explorerHref({
              station_id: top.station_id,
              show_bike_heat: 1,
              heat_metric: "available",
              heat_agg: "sum",
            }),
            primary: true,
          },
          { type: "link", label: "Open Insights", href: "/insights" },
        ],
        payload: { kind: "rain_contingency", top, list: riskItems, rainRisk },
      })
    );
  }

  return cards.slice(0, 4);
}

async function main() {
  MBA.setStatusText("Loading…");
  const root = document.getElementById("homeCards");
  if (!root) return;

  const [status, meta] = await Promise.all([MBA.fetchJson("/status"), MBA.fetchJson("/meta")]);
  MBA.setModePill(Boolean(status?.demo_mode));
  MBA.setWeatherPill(meta);

  const city = status?.tdx?.bike_cities?.[0] ?? "Taipei";
  const [usage, risk, hotspotsAvail, hotspotsRent] = await Promise.all([
    MBA.fetchJson(`/insights/weather_usage?city=${encodeURIComponent(city)}&hours=24`).catch(() => null),
    MBA.fetchJson(`/insights/rain_risk_now?city=${encodeURIComponent(city)}&top_k=5`).catch(() => null),
    MBA.fetchJson(`/insights/hotspots?metric=available&agg=sum&top_k=5`).catch(() => null),
    MBA.fetchJson(`/insights/hotspots?metric=rent_proxy&agg=sum&top_k=5`).catch(() => null),
  ]);

  root.innerHTML = "";
  root.appendChild(buildCredibilityCard(status, meta));
  root.appendChild(buildKpisCard(status));
  root.appendChild(buildTodayCard(status, meta));
  root.appendChild(buildStoryParagraphCard());
  root.appendChild(buildStoryProblemCard({ status, meta, usage }));
  root.appendChild(buildStoryRiskCard({ status, meta, risk }));
  // Policy options (2–4 cards)
  for (const c of buildPolicyOptionsSection({ status, meta, hotspotsAvail, hotspotsRent, rainRisk: risk })) {
    root.appendChild(c);
  }
  root.appendChild(buildNextActionsCard(status, meta));
  root.appendChild(buildMethodsCard());

  MBA.setStatusText("Ready");
}

main().catch((e) => {
  console.error(e);
  MBA.setStatusText(`Error: ${e.message}`);
});
