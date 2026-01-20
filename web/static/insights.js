const MBA = window.MBA;

function storyParagraphCard() {
  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Story Structure",
    title: "Problem → Evidence → Implication → Action",
    badge: { tone: "muted", text: "guide" },
  });
  body.innerHTML = `
    <div><span class="mono">Problem</span> · 哪些站點缺車/滿柱風險最高？</div>
    <div><span class="mono">Evidence</span> · 用排序清單（Top K）當作證據附件。</div>
    <div><span class="mono">Implication</span> · 影響轉乘與調度，需定義介入策略。</div>
    <div><span class="mono">Action</span> · 一鍵開 Explorer，帶入 heat 參數與站點。</div>
  `;
  return card;
}

function credibilityCard(status, meta) {
  const resolved = meta?.meta ?? {};
  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Data Credibility",
    title: "Sources · freshness · traceability",
    badge: { tone: status?.demo_mode ? "warn" : "ok", text: status?.demo_mode ? "demo" : "real" },
    right: `<span class="mono">build ${MBA.shortId(resolved.silver_build_id)}</span>`,
    actions: [
      { type: "link", label: "Open Ops", href: "/ops", primary: true },
      {
        label: "Download JSON",
        onClick: () => MBA.downloadJson(`metrobikeatlas-insights-meta-${new Date().toISOString()}.json`, { status, meta }),
      },
    ],
  });
  body.innerHTML = `<div class="hint">Silver: <span class="mono">${MBA.shortId(resolved.silver_build_id)}</span> · hash <span class="mono">${MBA.shortId(
    resolved.inputs_hash,
    10
  )}</span> · source <span class="mono">${resolved.fallback_source || "—"}</span></div>`;
  return card;
}

function controlsCard({ state, onRefresh }) {
  const { card, body } = MBA.createCard({
    tone: "support",
    kicker: "Controls",
    title: "Top problem stations",
    badge: { tone: "ok", text: "list" },
    actions: [
      { label: "Refresh list", primary: true, onClick: onRefresh },
      { type: "link", label: "Open Explorer", href: "/explorer" },
    ],
  });

  body.innerHTML = `
    <div class="grid2">
      <div class="field">
        <label class="label" for="insightsMode">Mode</label>
        <select id="insightsMode" class="select">
          <option value="shortage">shortage (low availability)</option>
          <option value="pressure">pressure (high rent_proxy)</option>
          <option value="hotspots">hotspots (available)</option>
          <option value="coldspots">coldspots (available)</option>
          <option value="rainy_risk">rainy-risk (now)</option>
        </select>
      </div>
      <div class="field">
        <label class="label" for="insightsTopK">Top K</label>
        <input id="insightsTopK" class="input input-number" type="number" min="1" max="50" step="1" value="10" />
      </div>
    </div>

    <div class="grid2" style="margin-top:10px;">
      <div class="field">
        <label class="label" for="scenarioWeather">Scenario · Weather</label>
        <select id="scenarioWeather" class="select">
          <option value="auto">auto (use now)</option>
          <option value="rainy">rainy</option>
          <option value="clear">clear</option>
        </select>
      </div>
      <div class="field">
        <label class="label" for="scenarioCalendar">Scenario · Calendar</label>
        <select id="scenarioCalendar" class="select">
          <option value="auto">auto</option>
          <option value="weekday">weekday</option>
          <option value="holiday">holiday</option>
        </select>
      </div>
    </div>
  `;

  const modeEl = body.querySelector("#insightsMode");
  const topKEl = body.querySelector("#insightsTopK");
  const weatherEl = body.querySelector("#scenarioWeather");
  const calEl = body.querySelector("#scenarioCalendar");

  modeEl.value = state.mode;
  topKEl.value = String(state.topK);
  weatherEl.value = state.scenarioWeather;
  calEl.value = state.scenarioCalendar;

  modeEl.addEventListener("change", () => {
    state.mode = modeEl.value;
    onRefresh();
  });
  topKEl.addEventListener("change", () => {
    state.topK = Math.max(1, Math.min(50, Number(topKEl.value) || 10));
    onRefresh();
  });
  weatherEl.addEventListener("change", () => {
    state.scenarioWeather = weatherEl.value;
    onRefresh();
  });
  calEl.addEventListener("change", () => {
    state.scenarioCalendar = calEl.value;
    onRefresh();
  });

  return card;
}

function whyCard({ state, meta }) {
  const w = meta?.meta?.external?.weather_collector ?? null;
  const nowRainy = Boolean(w?.is_rainy_now);
  const weatherMode =
    state.scenarioWeather === "auto" ? (nowRainy ? "rainy" : "clear") : state.scenarioWeather;
  const calMode = state.scenarioCalendar;

  const { card, body } = MBA.createCard({
    tone: "meta",
    kicker: "Why",
    title: "1-sentence explanation",
    badge: { tone: weatherMode === "rainy" ? "warn" : "muted", text: weatherMode },
  });

  const modeTxt =
    state.mode === "shortage"
      ? "nearby bike availability is low"
      : state.mode === "pressure"
        ? "rent_proxy is high (demand pressure)"
        : state.mode === "rainy_risk"
          ? "it is raining now, and these stations are more likely to run out"
          : state.mode === "hotspots"
            ? "these stations have higher values"
            : "these stations have lower values";
  const calTxt =
    calMode === "holiday" ? "Assume holiday (different pattern)." : calMode === "weekday" ? "Assume weekday." : "Calendar: auto.";
  const wTxt = weatherMode === "rainy" ? "Assume rainy scenario." : weatherMode === "clear" ? "Assume clear scenario." : "";

  body.textContent = `We highlight Top ${state.topK} because ${modeTxt}. ${wTxt} ${calTxt}`.trim();
  return card;
}

function listCard({ title, badgeText, badgeTone, items, valueLabel, explorerDefaults, rawPayload }) {
  const { card, body } = MBA.createCard({
    tone: "support",
    kicker: "Evidence",
    title,
    badge: { tone: badgeTone, text: badgeText },
    actions: [
      {
        label: "Download JSON",
        onClick: () => MBA.downloadJson(`metrobikeatlas-insights-${new Date().toISOString()}.json`, rawPayload),
      },
      { type: "link", label: "Open Explorer (heat)", href: MBA.explorerHref(explorerDefaults), primary: true },
    ],
  });

  if (!items.length) {
    body.innerHTML = `<div class="hint">No items.</div>`;
    return card;
  }
  const ul = document.createElement("ul");
  ul.className = "list";
  for (const it of items) {
    const li = document.createElement("li");
    li.style.display = "flex";
    li.style.justifyContent = "space-between";
    li.style.gap = "10px";
    li.style.alignItems = "center";

    const left = document.createElement("div");
    left.style.minWidth = "0";
    const name = document.createElement("div");
    name.style.fontWeight = "800";
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

    const href = MBA.explorerHref({ ...explorerDefaults, station_id: it.station_id });
    const a = document.createElement("a");
    a.className = "btn btn-primary";
    a.href = href;
    a.textContent = "Open";

    li.appendChild(left);
    li.appendChild(a);
    ul.appendChild(li);
  }
  body.appendChild(ul);
  return card;
}

async function main() {
  MBA.setStatusText("Loading…");
  const root = document.getElementById("insightsCards");
  if (!root) return;

  const state = {
    mode: "shortage",
    topK: 10,
    scenarioWeather: "auto",
    scenarioCalendar: "auto",
  };

  const [status, meta] = await Promise.all([MBA.fetchJson("/status"), MBA.fetchJson("/meta")]);
  MBA.setModePill(Boolean(status?.demo_mode));
  MBA.setWeatherPill(meta);

  const refresh = async () => {
    MBA.setStatusText("Loading list…");
    root.innerHTML = "";
    root.appendChild(credibilityCard(status, meta));
    root.appendChild(storyParagraphCard());
    root.appendChild(
      controlsCard({
        state,
        onRefresh: () => refresh().catch((e) => MBA.setStatusText(`Error: ${e.message}`)),
      })
    );
    root.appendChild(whyCard({ state, meta }));

    const city = status?.tdx?.bike_cities?.[0] ?? "Taipei";
    const topK = state.topK;

    let payload = null;
    let title = "";
    let badgeText = "";
    let badgeTone = "ok";
    let valueLabel = "";
    let explorerDefaults = {};
    let items = [];

    if (state.mode === "rainy_risk") {
      payload = await MBA.fetchJson(`/insights/rain_risk_now?city=${encodeURIComponent(city)}&top_k=${topK}`);
      items = (payload?.items ?? []).map((it) => ({
        station_id: it.station_id,
        name: it.name || it.station_id,
        value: it.mean_available_bikes,
        value_txt: Number.isFinite(Number(it.mean_available_bikes)) ? String(Math.round(Number(it.mean_available_bikes))) : "—",
      }));
      title = "Rainy-risk stations (now)";
      badgeText = payload?.is_rainy_now ? "raining" : "not raining";
      badgeTone = payload?.is_rainy_now ? "warn" : "muted";
      valueLabel = "mean_available";
      explorerDefaults = { show_bike_heat: 1, heat_metric: "available", heat_agg: "sum" };
    } else if (state.mode === "pressure") {
      payload = await MBA.fetchJson(`/insights/hotspots?metric=rent_proxy&agg=sum&top_k=${topK}`);
      items = payload?.hot ?? [];
      title = "Pressure (high rent_proxy)";
      badgeText = "rent_proxy";
      badgeTone = "warn";
      valueLabel = "rent_proxy";
      explorerDefaults = { show_bike_heat: 1, heat_metric: "rent_proxy", heat_agg: "sum" };
    } else if (state.mode === "hotspots") {
      payload = await MBA.fetchJson(`/insights/hotspots?metric=available&agg=sum&top_k=${topK}`);
      items = payload?.hot ?? [];
      title = "Hotspots (high available)";
      badgeText = "available";
      badgeTone = "ok";
      valueLabel = "available";
      explorerDefaults = { show_bike_heat: 1, heat_metric: "available", heat_agg: "sum" };
    } else if (state.mode === "coldspots" || state.mode === "shortage") {
      payload = await MBA.fetchJson(`/insights/hotspots?metric=available&agg=sum&top_k=${topK}`);
      items = payload?.cold ?? [];
      title = state.mode === "shortage" ? "Shortage (low availability)" : "Coldspots (low available)";
      badgeText = "available";
      badgeTone = state.mode === "shortage" ? "warn" : "muted";
      valueLabel = "available";
      explorerDefaults = { show_bike_heat: 1, heat_metric: "available", heat_agg: "sum" };
    }

    root.appendChild(
      listCard({
        title,
        badgeText,
        badgeTone,
        items: items.slice(0, topK),
        valueLabel,
        explorerDefaults,
        rawPayload: { status, meta, mode: state.mode, payload },
      })
    );

    const jump = MBA.createCard({
      tone: "primary",
      kicker: "Jump",
      title: "Open Explorer with parameters",
      badge: { tone: "ok", text: "action" },
      actions: [
        { type: "link", label: "Open Explorer (heat)", href: MBA.explorerHref(explorerDefaults), primary: true },
        { type: "link", label: "Open Ops", href: "/ops" },
      ],
    });
    jump.body.innerHTML = `<div class="hint">This link pre-configures heat metric and lets you open any station with one click.</div>`;
    root.appendChild(jump.card);

    MBA.setStatusText("Ready");
  };

  await refresh();
}

main().catch((e) => {
  console.error(e);
  MBA.setStatusText(`Error: ${e.message}`);
});

