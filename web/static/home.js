const MBA = window.MBA;

function loadDrafts() {
  try {
    const raw = localStorage.getItem("metrobikeatlas.policy_drafts.v1");
    if (!raw) return {};
    const obj = JSON.parse(raw);
    return obj && typeof obj === "object" ? obj : {};
  } catch {
    return {};
  }
}

function saveDrafts(drafts) {
  try {
    localStorage.setItem("metrobikeatlas.policy_drafts.v1", JSON.stringify(drafts || {}));
  } catch {
    // ignore
  }
}

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

  const heatParams = rainy
    ? { show_bike_heat: 1, heat_metric: "rent_proxy", heat_agg: "sum", guided: 1, guided_kind: "pressure", guided_title: "Rain × Usage evidence" }
    : { show_bike_heat: 1, heat_metric: "available", heat_agg: "sum", guided: 1, guided_kind: "shortage", guided_title: "Rain × Usage evidence" };
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
    const href = MBA.explorerHref({
      station_id: it.station_id,
      show_bike_heat: 1,
      heat_metric: "available",
      heat_agg: "sum",
      guided: 1,
      guided_kind: "rain_contingency",
      guided_title: "Rain-risk station (now)",
    });
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

function buildPolicyOptionCard({
  kind,
  title,
  kicker = "Policy option",
  badge,
  impact,
  beneficiaries,
  risk,
  needs,
  actions,
  payload,
  drafts,
  readOnly = false,
}) {
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

  const draftKey = String(kind || title || "policy");
  const d = drafts?.[draftKey] ?? { notes: "", owner: "", due_date: "" };
  const box = document.createElement("div");
  box.style.marginTop = "10px";
  box.innerHTML = `
    <div class="hint">Draft fields (saved into snapshot JSON)</div>
    <div class="grid2">
      <div class="field">
        <label class="label">Owner</label>
        <input class="input" data-k="owner" placeholder="e.g. Operations team" />
      </div>
      <div class="field">
        <label class="label">Due date</label>
        <input class="input" data-k="due_date" placeholder="YYYY-MM-DD" />
      </div>
    </div>
    <div class="field" style="margin-top:10px;">
      <label class="label">Notes</label>
      <textarea class="input" data-k="notes" rows="3" placeholder="Add discussion notes / assumptions / next questions…"></textarea>
    </div>
  `;
  const ownerEl = box.querySelector('[data-k="owner"]');
  const dueEl = box.querySelector('[data-k="due_date"]');
  const notesEl = box.querySelector('[data-k="notes"]');
  ownerEl.value = String(d.owner || "");
  dueEl.value = String(d.due_date || "");
  notesEl.value = String(d.notes || "");
  const onChange = () => {
    drafts[draftKey] = {
      owner: String(ownerEl.value || "").trim(),
      due_date: String(dueEl.value || "").trim(),
      notes: String(notesEl.value || "").trim(),
    };
    saveDrafts(drafts);
  };
  if (!readOnly) {
    ownerEl.addEventListener("change", onChange);
    dueEl.addEventListener("change", onChange);
    notesEl.addEventListener("change", onChange);
  } else {
    ownerEl.disabled = true;
    dueEl.disabled = true;
    notesEl.disabled = true;
  }
  body.appendChild(box);
  return card;
}

function buildPolicyOptionsSection({ status, meta, hotspotsAvail, hotspotsRent, rainRisk, drafts }) {
  const h = status?.health ?? {};
  const w = meta?.meta?.external?.weather_collector ?? null;
  const demo = Boolean(status?.demo_mode);
  const cards = [];

  // 1) Data source / pipeline reliability
  if (!demo && Number(h.metro_tdx_404_count || 0) > 0) {
    cards.push(
      buildPolicyOptionCard({
        kind: "metro_404",
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
        drafts,
      })
    );
  }

  // 2) Shortage: low availability (use coldspots from available)
  const shortage = (hotspotsAvail?.cold ?? []).slice(0, 3);
  if (shortage.length) {
    const top = shortage[0];
    cards.push(
      buildPolicyOptionCard({
        kind: "shortage",
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
              guided: 1,
              guided_kind: "shortage",
              guided_title: "Increase supply at shortage hotspots",
            }),
            primary: true,
          },
          { type: "link", label: "Open Insights", href: "/insights" },
        ],
        payload: { kind: "shortage", top, list: shortage, hotspotsAvail },
        drafts,
      })
    );
  }

  // 3) Pressure: high rent_proxy (use hotspots from rent_proxy)
  const pressure = (hotspotsRent?.hot ?? []).slice(0, 3);
  if (pressure.length) {
    const top = pressure[0];
    cards.push(
      buildPolicyOptionCard({
        kind: "pressure",
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
              guided: 1,
              guided_kind: "pressure",
              guided_title: "Rebalancing strategy for high demand pressure",
            }),
            primary: true,
          },
          { type: "link", label: "Open Ops", href: "/ops" },
        ],
        payload: { kind: "pressure", top, list: pressure, hotspotsRent },
        drafts,
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
        kind: "rain_contingency",
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
              guided: 1,
              guided_kind: "rain_contingency",
              guided_title: "Rain-day contingency (risk station)",
            }),
            primary: true,
          },
          { type: "link", label: "Open Insights", href: "/insights" },
        ],
        payload: { kind: "rain_contingency", top, list: riskItems, rainRisk },
        drafts,
      })
    );
  }

  return cards.slice(0, 4);
}

function buildSnapshotsCard({ getLivePayload, onApplySnapshot }) {
  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Replay",
    title: "Snapshots (traceable & reproducible)",
    badge: { tone: "muted", text: "localhost" },
    actions: [
      {
        label: "Refresh list",
        onClick: async () => {
          await refreshList();
        },
      },
      {
        label: "Save snapshot",
        primary: true,
        onClick: async () => {
          await saveSnapshot();
        },
      },
      {
        label: "Copy full brief",
        onClick: async () => {
          await copyFullBrief();
        },
      },
    ],
  });

  body.innerHTML = `
    <div class="row row-actions" style="margin-top:0;">
      <select id="snapshotSelect" class="select" style="flex:1; min-width: 260px;"></select>
      <button class="btn" id="btnLoadSnapshot">Load</button>
      <button class="btn" id="btnBackToLive">Live</button>
    </div>
    <div class="field" style="margin-top:10px;">
      <label class="label">Brief notes (optional)</label>
      <textarea id="briefNotes" class="input" rows="3" placeholder="Notes saved into snapshot JSON (for Notion/Docs)…"></textarea>
    </div>
    <div class="hint mono" id="snapshotHint"></div>
  `;

  const selectEl = body.querySelector("#snapshotSelect");
  const hintEl = body.querySelector("#snapshotHint");
  const notesEl = body.querySelector("#briefNotes");

  let snapshotList = [];

  const refreshList = async () => {
    try {
      const out = await MBA.adminFetchJson("/briefing/snapshots?limit=30");
      snapshotList = Array.isArray(out) ? out : [];
      selectEl.innerHTML = "";
      const opt0 = document.createElement("option");
      opt0.value = "";
      opt0.textContent = "Select snapshot…";
      selectEl.appendChild(opt0);
      for (const s of snapshotList) {
        const opt = document.createElement("option");
        opt.value = s.id;
        const when = s.created_at_utc ? String(s.created_at_utc) : "";
        const station = s.snapshot?.station_id ? ` · ${s.snapshot.station_id}` : "";
        opt.textContent = `${String(s.id).slice(0, 8)} · ${when}${station}`;
        selectEl.appendChild(opt);
      }
      hintEl.textContent = snapshotList.length ? `Found ${snapshotList.length} snapshots.` : "No snapshots yet.";
      MBA.setStatusText("Snapshots updated");
    } catch (e) {
      hintEl.textContent = `Snapshots unavailable: ${e.message}`;
      MBA.setStatusText("Snapshots unavailable");
    }
  };

  const saveSnapshot = async () => {
    const live = getLivePayload();
    if (!live) return;
    const notes = String(notesEl.value || "").trim() || null;
    const bodyIn = { ...live, notes };
    try {
      MBA.setStatusText("Saving snapshot…");
      const out = await MBA.adminPostJson("/briefing/snapshots", bodyIn);
      hintEl.textContent = `Saved snapshot ${out?.id || "—"}`;
      await refreshList();
    } catch (e) {
      hintEl.textContent = `Save failed: ${e.message}`;
      MBA.setStatusText("Save failed");
    }
  };

  const copyFullBrief = async () => {
    const live = getLivePayload();
    if (!live) return;
    const lines = [];
    const now = new Date().toISOString();
    lines.push(`# MetroBikeAtlas Brief`);
    lines.push(`- generated_at: ${now}`);
    if (live.artifacts?.silver_build_id) lines.push(`- silver_build_id: ${live.artifacts.silver_build_id}`);
    if (live.artifacts?.inputs_hash) lines.push(`- inputs_hash: ${live.artifacts.inputs_hash}`);
    lines.push("");
    lines.push(`## One sentence`);
    lines.push(String(live.artifacts?.one_sentence || "—"));
    lines.push("");
    lines.push(`## KPIs`);
    for (const k of live.kpis || []) {
      lines.push(`- ${k.label}: ${k.value}${k.meta ? ` (${k.meta})` : ""}`);
    }
    lines.push("");
    lines.push(`## Policy options`);
    for (const c of live.policy_cards || []) {
      lines.push(`### ${c.title || "Policy option"}`);
      lines.push(`- Impact: ${c.impact || "—"}`);
      lines.push(`- Beneficiaries: ${c.beneficiaries || "—"}`);
      lines.push(`- Risk: ${c.risk || "—"}`);
      lines.push(`- Needs: ${c.needs || "—"}`);
      if (c.draft_owner || c.draft_due_date) lines.push(`- Owner/Due: ${c.draft_owner || "—"} / ${c.draft_due_date || "—"}`);
      if (c.draft_notes) lines.push(`- Notes: ${c.draft_notes}`);
      lines.push("");
    }
    const notes = String(notesEl.value || "").trim();
    if (notes) {
      lines.push(`## Notes`);
      lines.push(notes);
      lines.push("");
    }
    await MBA.copyText(lines.join("\n"));
    MBA.setStatusText("Copied full brief");
  };

  body.querySelector("#btnLoadSnapshot").addEventListener("click", () => {
    const id = String(selectEl.value || "");
    if (!id) return;
    const snap = snapshotList.find((s) => s.id === id);
    if (!snap) return;
    onApplySnapshot(snap);
  });
  body.querySelector("#btnBackToLive").addEventListener("click", () => onApplySnapshot(null));

  refreshList().catch(() => {});

  return card;
}

function buildSnapshotBannerCard(snapOut, { onBackToLive, onCopyFullBrief }) {
  const { card, body } = MBA.createCard({
    tone: "primary",
    kicker: "Replay Mode",
    title: `Viewing snapshot ${String(snapOut?.id || "").slice(0, 8)}`,
    badge: { tone: "ok", text: "snapshot" },
    right: `<span class="mono">${snapOut?.created_at_utc || ""}</span>`,
    actions: [
      { label: "Back to Live", primary: true, onClick: onBackToLive },
      {
        label: "Download JSON",
        onClick: () => MBA.downloadJson(`metrobikeatlas-snapshot-${snapOut.id}.json`, snapOut),
      },
      { label: "Copy full brief", onClick: onCopyFullBrief },
    ],
  });
  const note = snapOut?.snapshot?.notes ? `<div class="hint">Notes included.</div>` : `<div class="hint">No notes.</div>`;
  body.innerHTML = `<div class="hint">This page is rendering saved content (reproducible brief).</div>${note}`;
  return card;
}

function buildSnapshotKpisCard(snapshot) {
  const { card, body } = MBA.createCard({
    tone: "support",
    kicker: "KPIs",
    title: "Saved KPIs",
    badge: { tone: "ok", text: String((snapshot?.kpis ?? []).length) },
  });
  const kpis = snapshot?.kpis ?? [];
  if (!kpis.length) {
    body.innerHTML = `<div class="hint">No KPIs saved.</div>`;
    return card;
  }
  body.innerHTML = `<ul style="margin:0; padding-left:18px;">${kpis
    .map((k) => `<li><span class="mono">${k.label}</span>: ${k.value}${k.meta ? ` <span class="hint mono">(${k.meta})</span>` : ""}</li>`)
    .join("")}</ul>`;
  return card;
}

function buildSnapshotPolicyCards(snapshot) {
  const out = [];
  const cards = snapshot?.policy_cards ?? [];
  for (const c of cards) {
    const acts = [];
    if (c.explorer_params && c.explorer_params.station_id) {
      acts.push({ type: "link", label: "Open Explorer", href: MBA.explorerHref(c.explorer_params), primary: true });
    } else {
      acts.push({ type: "link", label: "Open Explorer", href: "/explorer", primary: true });
    }
    acts.push({ type: "link", label: "Open Ops", href: "/ops" });
    out.push(
      buildPolicyOptionCard({
        kind: c.kind,
        title: c.title,
        badge: { tone: "ok", text: c.kind || "policy" },
        impact: c.impact,
        beneficiaries: c.beneficiaries,
        risk: c.risk,
        needs: c.needs,
        actions: acts,
        drafts: {
          [String(c.kind || c.title)]: { owner: c.draft_owner, due_date: c.draft_due_date, notes: c.draft_notes },
        },
        readOnly: true,
        payload: c,
      })
    );
  }
  return out;
}

function buildSnapshotNotesCard(snapshot) {
  if (!snapshot?.notes) return null;
  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Notes",
    title: "Brief notes",
    badge: { tone: "muted", text: "notes" },
  });
  body.textContent = String(snapshot.notes);
  return card;
}

async function main() {
  MBA.setStatusText("Loading…");
  const root = document.getElementById("homeCards");
  if (!root) return;

  const drafts = loadDrafts();
  let snapshotMode = null; // BriefingSnapshotOut or null

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

  const buildLiveSnapshotPayload = () => {
    const h = status?.health ?? {};
    const resolved = meta?.meta ?? {};
    const kpis = [
      { label: "Collector", value: h.collector_running ? "running" : "stopped", meta: h.collector_last_ok_utc || "", tone: h.collector_running ? "ok" : "warn" },
      { label: "Bronze freshness", value: MBA.fmtAge(h.bronze_bike_availability_age_s), meta: h.bronze_bike_availability_last_utc || "", tone: "ok" },
      {
        label: "Silver freshness",
        value: MBA.fmtAge(Math.min(Number(h.silver_metro_bike_links_age_s ?? Infinity), Number(h.silver_bike_timeseries_age_s ?? Infinity))),
        meta: "links/bike_timeseries",
        tone: "ok",
      },
    ];
    if (Number(h.metro_tdx_404_count || 0)) {
      kpis.push({ label: "Metro TDX 404", value: String(h.metro_tdx_404_count), meta: h.metro_tdx_404_last_utc || "", tone: "bad" });
    }

    const policy = [];
    const policyCards = buildPolicyOptionsSection({ status, meta, hotspotsAvail, hotspotsRent, rainRisk: risk, drafts });
    // Extract from DOM-less generation: use the same intent as the generator above
    if (!demo && Number(h.metro_tdx_404_count || 0) > 0) {
      const d = drafts["metro_404"] || {};
      policy.push({
        kind: "metro_404",
        title: "Fix data source: Metro stations unavailable (TDX 404)",
        impact: "Restores station layer/links so insights remain trustworthy.",
        beneficiaries: "Decision makers, operators, analysts",
        risk: "Using fallback may differ from official station definitions.",
        needs: "Provide external metro stations CSV or adjust TDX config/path.",
        draft_owner: d.owner || "",
        draft_due_date: d.due_date || "",
        draft_notes: d.notes || "",
        explorer_params: null,
      });
    }
    if ((hotspotsAvail?.cold ?? []).length) {
      const top = (hotspotsAvail.cold ?? [])[0];
      const d = drafts["shortage"] || {};
      policy.push({
        kind: "shortage",
        title: "Increase supply at shortage hotspots (low availability)",
        impact: "Reduce 'no-bike' incidents and improve last-mile transfer reliability.",
        beneficiaries: "Commuters, transfer passengers",
        risk: "If shortage is only peak-hour, fixed expansion may be underutilized off-peak.",
        needs: "1–2 weeks of data to verify peak patterns; confirm with operator logs.",
        station_id: top?.station_id || null,
        explorer_params: { station_id: top?.station_id || null, show_bike_heat: 1, heat_metric: "available", heat_agg: "sum" },
        draft_owner: d.owner || "",
        draft_due_date: d.due_date || "",
        draft_notes: d.notes || "",
      });
    }
    if ((hotspotsRent?.hot ?? []).length) {
      const top = (hotspotsRent.hot ?? [])[0];
      const d = drafts["pressure"] || {};
      policy.push({
        kind: "pressure",
        title: "Rebalancing strategy for high demand pressure (rent_proxy)",
        impact: "Mitigates peak-hour demand spikes by targeted rebalancing and guidance.",
        beneficiaries: "High-frequency users, operators",
        risk: "Proxy demand may over/under-estimate true demand in certain areas.",
        needs: "Validate proxy against ridership/operational data; define time-of-day strategy.",
        station_id: top?.station_id || null,
        explorer_params: { station_id: top?.station_id || null, show_bike_heat: 1, heat_metric: "rent_proxy", heat_agg: "sum" },
        draft_owner: d.owner || "",
        draft_due_date: d.due_date || "",
        draft_notes: d.notes || "",
      });
    }
    const w = meta?.meta?.external?.weather_collector ?? null;
    if (w?.is_rainy_now && (risk?.items ?? []).length) {
      const top = (risk.items ?? [])[0];
      const d = drafts["rain_contingency"] || {};
      policy.push({
        kind: "rain_contingency",
        title: "Rain-day contingency: pre-position supply near risk stations",
        impact: "Improves service resilience during rain; reduces sudden shortages.",
        beneficiaries: "Commuters, vulnerable users during bad weather",
        risk: "Weather is city-level estimate; microclimates may differ by district.",
        needs: "Define rain threshold and lead time; consider communication/signage strategy.",
        station_id: top?.station_id || null,
        explorer_params: { station_id: top?.station_id || null, show_bike_heat: 1, heat_metric: "available", heat_agg: "sum" },
        draft_owner: d.owner || "",
        draft_due_date: d.due_date || "",
        draft_notes: d.notes || "",
      });
    }

    const one = oneSentence(status, meta);
    const usageSummary =
      usage && usage.precip_total_mm != null
        ? `${usage.city || city} · ${Number(usage.precip_total_mm) > 0 ? `rain ${Number(usage.precip_total_mm).toFixed(1)}mm` : "no rain"} · rent_proxy ${Math.round(
            Number(usage.rent_proxy_total ?? 0)
          )} · return_proxy ${Math.round(Number(usage.return_proxy_total ?? 0))}`
        : null;

    return {
      station_id: policy.find((p) => p.station_id)?.station_id || null,
      story_step: "home",
      kpis,
      settings: { city },
      artifacts: {
        silver_build_id: resolved.silver_build_id || null,
        inputs_hash: resolved.inputs_hash || null,
        fallback_source: resolved.fallback_source || null,
        one_sentence: one,
        story_rain_usage: usageSummary,
        story_rain_risk: risk?.items ?? null,
        generated_at_utc: new Date().toISOString(),
      },
      policy_cards: policy.slice(0, 4),
      notes: null,
    };
  };

  const render = async () => {
    root.innerHTML = "";

    const snapshotsCard = buildSnapshotsCard({
      getLivePayload: () => buildLiveSnapshotPayload(),
      onApplySnapshot: (snapOut) => {
        snapshotMode = snapOut;
        render().catch(() => {});
      },
    });
    root.appendChild(snapshotsCard);

    if (snapshotMode) {
      const snap = snapshotMode.snapshot;
      root.appendChild(
        buildSnapshotBannerCard(snapshotMode, {
          onBackToLive: () => {
            snapshotMode = null;
            render().catch(() => {});
          },
          onCopyFullBrief: async () => {
            const live = snap;
            const lines = [];
            lines.push(`# MetroBikeAtlas Brief (snapshot)`);
            lines.push(`- snapshot_id: ${snapshotMode.id}`);
            lines.push(`- created_at_utc: ${snapshotMode.created_at_utc}`);
            if (live?.artifacts?.silver_build_id) lines.push(`- silver_build_id: ${live.artifacts.silver_build_id}`);
            if (live?.artifacts?.inputs_hash) lines.push(`- inputs_hash: ${live.artifacts.inputs_hash}`);
            lines.push("");
            lines.push(`## One sentence`);
            lines.push(String(live.artifacts?.one_sentence || "—"));
            lines.push("");
            lines.push(`## KPIs`);
            for (const k of live.kpis || []) lines.push(`- ${k.label}: ${k.value}${k.meta ? ` (${k.meta})` : ""}`);
            lines.push("");
            lines.push(`## Policy options`);
            for (const c of live.policy_cards || []) {
              lines.push(`### ${c.title || "Policy option"}`);
              lines.push(`- Impact: ${c.impact || "—"}`);
              lines.push(`- Beneficiaries: ${c.beneficiaries || "—"}`);
              lines.push(`- Risk: ${c.risk || "—"}`);
              lines.push(`- Needs: ${c.needs || "—"}`);
              if (c.draft_owner || c.draft_due_date) lines.push(`- Owner/Due: ${c.draft_owner || "—"} / ${c.draft_due_date || "—"}`);
              if (c.draft_notes) lines.push(`- Notes: ${c.draft_notes}`);
              lines.push("");
            }
            if (live.notes) {
              lines.push(`## Notes`);
              lines.push(String(live.notes));
              lines.push("");
            }
            await MBA.copyText(lines.join("\n"));
            MBA.setStatusText("Copied full brief");
          },
        })
      );
      root.appendChild(buildSnapshotKpisCard(snap));
      for (const c of buildSnapshotPolicyCards(snap)) root.appendChild(c);
      const notesCard = buildSnapshotNotesCard(snap);
      if (notesCard) root.appendChild(notesCard);
      root.appendChild(buildMethodsCard());
      MBA.setStatusText("Ready (snapshot)");
      return;
    }

    root.appendChild(buildCredibilityCard(status, meta));
    root.appendChild(buildKpisCard(status));
    root.appendChild(buildTodayCard(status, meta));
    root.appendChild(buildStoryParagraphCard());
    root.appendChild(buildStoryProblemCard({ status, meta, usage }));
    root.appendChild(buildStoryRiskCard({ status, meta, risk }));
    for (const c of buildPolicyOptionsSection({ status, meta, hotspotsAvail, hotspotsRent, rainRisk: risk, drafts })) {
      root.appendChild(c);
    }
    root.appendChild(buildNextActionsCard(status, meta));
    root.appendChild(buildMethodsCard());
    MBA.setStatusText("Ready");
  };

  await render();

}

main().catch((e) => {
  console.error(e);
  MBA.setStatusText(`Error: ${e.message}`);
});
