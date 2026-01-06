# 05 — Phase 5：Bronze 資料擷取腳本（TDX Raw JSON → data/bronze）

## 1) 本階段目標

到 Phase 4 為止，我們已經有：

- `TDXClient`：負責 token、重試、timeout（可靠 HTTP）
- `TDXMetroClient` / `TDXBikeClient`：負責「解析/標準化」成 dataclass（偏向 Silver）

Phase 5 的重點是 **Bronze（原始層）**：把 TDX API 回傳的資料「原封不動」寫到本地資料湖 `data/bronze/`，並附上最小但關鍵的 metadata（抓取時間、request 參數）。

你會學到：

1. 為什麼 Bronze 要存 raw JSON（可追溯、可重跑、可稽核）？
2. 為什麼 Bronze 檔案一定要有 `retrieved_at`（UTC）與 request metadata？
3. 共享單車 availability 為什麼要用「定期抓快照」來做 time series？
4. 長時間收集要注意哪些坑：rate limit、抖動（jitter）、失敗退避（backoff）、停止條件

---

## 2) 我改了哪些檔案？為什麼要改這些？

- 修改（加上逐行英文註解）：
  - `scripts/extract_metro_stations.py`
  - `scripts/extract_bike_stations.py`
  - `scripts/collect_bike_availability.py`
  - `scripts/collect_bike_availability_loop.py`
- 新增（超詳細中文教學）：
  - `docs/05_phase5_bronze_ingestion_scripts.md`（本文件）

為什麼這四支 script 是 Bronze 的核心：

- `extract_*_stations.py`：抓「變動較少」的站點資料（station metadata）
- `collect_*availability*.py`：抓「變動頻繁」的可用量快照（realtime-ish），用快照累積出 time series

---

## 3) 核心概念講解（術語中英對照）

### 3.1 Bronze 的資料長相（wrapper schema）

Bronze 檔案不是直接把 payload dump 出來而已，而是包一層 wrapper：

- `retrieved_at`：抓取時間（UTC ISO format）
- `request`：你呼叫了什麼 path/params（可追溯）
- `payload`：原始 API 回傳 JSON（不做解析、不改欄位）

這層 wrapper 由 `write_bronze_json(...)` 寫出來（在 `src/metrobikeatlas/ingestion/bronze.py`）。

為什麼要包 wrapper（而不是只存 payload）：

- 你之後看到一個檔案，才知道它「什麼時候抓的」「怎麼抓的」
- 你可以用同一個 request metadata 重跑（reproducible）

### 3.2 Partition（分區）與檔名規則

檔案會被寫到類似下面的路徑：

- 捷運站點：`data/bronze/tdx/metro/stations/city=Taipei/20260106T120000Z.json`
- 單車站點：`data/bronze/tdx/bike/stations/city=Taipei/20260106T120000Z.json`
- 單車可用量：`data/bronze/tdx/bike/availability/city=Taipei/20260106T120000Z.json`

這種設計（source/domain/dataset/city=...）的好處：

- 你可以用路徑直接做資料切分（partition pruning）
- 你可以很直覺地知道一個檔案屬於哪個資料集

### 3.3 為什麼 availability 要「定期快照」？

很多共享單車資料源提供的是「現在的可用量」，不是「歷史時序」。

所以你要自己建立 time series：

1. 每隔 N 分鐘抓一次 snapshot
2. 落地到 Bronze（保留原始 JSON）
3. 在 Silver 階段把多個 snapshot 串起來，變成 `bike_timeseries.csv`

---

## 4) 程式碼分區塊貼上（Bronze scripts）

### 4.1 `extract_metro_stations.py`：抓捷運站點 metadata

```py
with TDXClient(
    base_url=config.tdx.base_url,
    token_url=config.tdx.token_url,
    credentials=creds,
) as tdx:
    for city in config.tdx.metro.cities:
        path = config.tdx.metro.stations_path_template.format(city=city)
        params = {"$format": "JSON"}
        retrieved_at = datetime.now(timezone.utc)
        payload = tdx.get_json(path, params=params)

        out = write_bronze_json(
            bronze_dir,
            source="tdx",
            domain="metro",
            dataset="stations",
            city=city,
            retrieved_at=retrieved_at,
            request={"path": path, "params": params},
            payload=payload,
        )
```

你要注意的重點：

- Bronze 只存 raw payload，不在這裡 parse
- `retrieved_at` 用 UTC，方便跨機器對齊
- `request` 會把 path/params 也存起來

### 4.2 `collect_bike_availability_loop.py`：長時間收集可用量快照

這支 script 做了三件很 production 的事：

1. **Stop conditions**：用 `--duration-seconds` / `--max-iterations` 控制停止
2. **Backoff**：當「全部城市都抓不到」時，用指數退避避免狂打 API
3. **Jitter**：用隨機抖動把呼叫分散，降低 burst / rate limit

```py
if ok == 0:
    logger.warning("No snapshots collected; backing off %ss.", current_backoff_s)
    time.sleep(current_backoff_s)
    current_backoff_s = min(current_backoff_s * 2, int(args.max_backoff_seconds))
    continue
```

---

## 5) 常見錯誤與排查

### 5.1 缺少 TDX 憑證

症狀：

- `ValueError: Missing env vars: TDX_CLIENT_ID and TDX_CLIENT_SECRET`

排查：

1. 複製 `.env.example` → `.env`
2. 填入你的 `TDX_CLIENT_ID` / `TDX_CLIENT_SECRET`
3. 重新跑 script（`load_config()` 會在有安裝 python-dotenv 時自動載入 `.env`）

### 5.2 429 rate limit / intermittent 5xx

排查方向：

- 把 polling interval 拉長（例如 300s → 600s）
- 開啟 jitter（例如 `--jitter-seconds 10`）
- 觀察 backoff log，確認失敗時沒有進入 tight loop

### 5.3 你以為有 time series，但其實只有一筆快照

如果你只跑一次 `collect_bike_availability.py`：

- 你只會得到「某個時間點」的 snapshot

要做時序：

- 用 loop script 跑一段時間（例如 1 小時）或用 cron 每 5 分鐘跑一次 one-shot

---

## 6) 本階段驗收方式（中文 + English commands）

### 6.1 Unit tests (no network)

```bash
pytest -q
```

### 6.2 Lint (optional)

```bash
ruff check scripts/extract_metro_stations.py scripts/extract_bike_stations.py \
  scripts/collect_bike_availability.py scripts/collect_bike_availability_loop.py
```

### 6.3 Manual run (requires TDX credentials + network)

```bash
python scripts/extract_metro_stations.py
python scripts/extract_bike_stations.py
python scripts/collect_bike_availability.py
python scripts/collect_bike_availability_loop.py --interval-seconds 300 --duration-seconds 3600 --jitter-seconds 5
```

預期結果：

- `data/bronze/tdx/...` 底下產生 JSON 檔（依 city 分區）
- Console/log 看到 `Wrote ...` 或 `logger.info("Wrote %s", out)`

