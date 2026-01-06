# 06 — Phase 6：建立 Silver（Bronze JSON → Silver CSV）

## 1) 本階段目標

Phase 5 我們已經能把 TDX 的 raw JSON 落地到 `data/bronze/`；但是：

- API/Web 不想直接吃「每個城市欄位都不一樣」的 raw JSON
- 分析/整併也需要穩定的欄位與型別

所以 Phase 6 的目標是：把 Bronze 轉成 **Silver（標準化 CSV）**，讓後續流程可以重用。

你會得到 4 份 Silver 核心表：

1. `metro_stations.csv`（捷運站點維度表）
2. `bike_stations.csv`（單車站點維度表）
3. `bike_timeseries.csv`（單車可用量時序 + 借還 proxy）
4. `metro_bike_links.csv`（捷運站 ↔ 附近單車站的關聯表）

---

## 2) 我改了哪些檔案？為什麼要改這些？

- 修改（加上逐行英文註解）：
  - `src/metrobikeatlas/ingestion/bronze.py`
  - `scripts/build_silver.py`
- 新增（超詳細中文教學）：
  - `docs/06_phase6_build_silver.md`（本文件）

為什麼要同時改這兩個檔案：

- `bronze.py` 定義了 Bronze wrapper 的「最小 schema」（retrieved_at / request / payload）
- `build_silver.py` 是把 Bronze 轉成 Silver 的「主入口」，初學者最常在這裡卡住資料流

---

## 3) 核心概念講解（術語中英對照）

### 3.1 Dimension / Fact 的思維（維度表 / 事實表）

Silver 層我們希望有一個非常基本但穩定的資料模型：

- **Dimension table（維度表）**：站點這種「相對穩定」的資料  
  - `metro_stations.csv`
  - `bike_stations.csv`
- **Fact table（事實表）**：隨時間變動的觀測值  
  - `bike_timeseries.csv`（每站每時間點的可用量）
- **Link table（關聯表）**：用來做 join 的橋接資料  
  - `metro_bike_links.csv`

這樣做的好處：

- API 要畫圖時，只要：
  - 先用 links 找出某捷運站附近有哪些 bike 站
  - 再聚合 bike_timeseries
- 你在做 feature engineering 時，也能清楚知道要 join 哪些表

### 3.2 為什麼 stations 只取「每城市最新一份 Bronze」？

站點 metadata 通常不是每分鐘變動：

- 你可能跑過多次 `extract_*_stations.py`
- Bronze 會累積很多 station snapshot

Silver 在 MVP 採取策略：

- **每個城市只取最新一個 station snapshot**

原因：

- 避免重複站點（同一站被寫很多次）
- 讓 Silver 維度表保持「一站一列」的乾淨形狀

### 3.3 為什麼 availability 讀多個 Bronze 檔？

availability 是時變的：

- 你要靠「多次快照」才有 time series

所以 `build_silver.py` 會讀很多檔：

- `data/bronze/tdx/bike/availability/city=.../*.json`

並且有一個 cap：

- `--max-availability-files`（避免資料太多導致記憶體爆掉）

### 3.4 借還 proxy（rent/return proxy）是什麼？

如果你只有「可用量」而沒有真正的「借車/還車事件」，你仍然可以做一個可用 proxy：

- 可用量下降 → 可能有人借車
- 可用量上升 → 可能有人還車

這不是完美的真值，但在：

- 沒有 trip dataset
- 或沒有 metro ridership dataset

的 MVP 階段，是一個合理替代指標。

---

## 4) 程式碼分區塊貼上

### 4.1 Bronze wrapper：`write_bronze_json(...)`

```py
wrapper = {
    "retrieved_at": retrieved_at.astimezone(timezone.utc).isoformat(),
    "request": dict(request) if request else None,
    "payload": payload,
}
```

重點：

- Silver 不靠「猜」，而是直接讀 `payload`
- `request` 讓你 debug 時可以回推：到底打了哪個 endpoint、帶了哪些 params

### 4.2 `build_silver.py`：stations（最新一份）→ CSV

```py
for city in config.tdx.metro.cities:
    city_dir = bronze_dir / "tdx" / "metro" / "stations" / f"city={city}"
    bronze = read_bronze_json(_latest_file(city_dir))
    payload = bronze["payload"]
    for item in payload:
        metro_rows.append(asdict(TDXMetroClient.parse_station(item, city=city)))
```

### 4.3 `build_silver.py`：availability（多份檔案）→ time series

```py
files = sorted(city_dir.glob("*.json"))[-args.max_availability_files :]
for f in files:
    bronze = read_bronze_json(f)
    payload = bronze["payload"]
    for item in payload:
        record = TDXBikeClient.parse_availability(item)
        availability_rows.append({**asdict(record), "city": city})
```

---

## 5) 常見錯誤與排查

### 5.1 `FileNotFoundError: No Bronze files found ...`

代表：

- 你還沒跑 Phase 5 的 Bronze 擷取 scripts

處理：

- 先跑（需要 TDX creds + network）：
  - `python scripts/extract_metro_stations.py`
  - `python scripts/extract_bike_stations.py`
  - `python scripts/collect_bike_availability_loop.py --duration-seconds 3600`

### 5.2 `No bike availability data found; skipping bike_timeseries.csv`

代表：

- Bronze 沒有 availability 快照（你可能只抓了 stations）

處理：

- 跑一次 `collect_bike_availability.py`（只會有一筆時間點）
- 或跑 loop script 收集一段時間（比較像 time series）

### 5.3 Silver CSV 有重複站點

可能原因：

- 上游 station payload 本身就有重複
- 或你把多個 snapshot 合併了（理論上我們現在只取最新一份）

處理：

- 先檢查 Bronze payload 的 station_id 是否有重複
- 再決定要不要在 Silver 增加 `drop_duplicates`（但要先定義 dedup key）

---

## 6) 本階段驗收方式（中文 + English commands）

### 6.1 Unit tests

```bash
pytest -q
```

### 6.2 Lint (optional)

```bash
ruff check src/metrobikeatlas/ingestion/bronze.py scripts/build_silver.py
```

### 6.3 Manual run (requires Bronze files already exist)

```bash
python scripts/build_silver.py --bronze-dir data/bronze --silver-dir data/silver
```

預期結果：

- `data/silver/` 底下出現：
  - `metro_stations.csv`
  - `bike_stations.csv`
  - `bike_timeseries.csv`（如果有 availability 快照）
  - `metro_bike_links.csv`

