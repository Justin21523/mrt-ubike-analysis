# 04 — Phase 4：TDX 資料集 Client（Metro/Bike：Stations + Availability）

## 1) 本階段目標

Phase 3 我們把 `TDXClient`（認證 + HTTP reliability）完成了；Phase 4 我們要把它「套用到具體資料集」：

- 捷運/軌道：**站點 metadata**（stations）與（可選的）**運量/進出站時序**（ridership timeseries）
- 共享單車：**站點 metadata**（stations）與 **可用量快照**（availability snapshot）

這個 Phase 的目標是讓你能理解並回答：

1. 為什麼還需要 `TDXMetroClient` / `TDXBikeClient`？不能直接用 `TDXClient.get_json()` 就好嗎？
2. 「Bronze 原始 JSON」與「Silver 標準化 dataclass」的責任邊界在哪裡？
3. 為什麼要把解析（parse/normalize）集中在 client 模組，而不是散落在每個 script？
4. 為什麼 availability 的 `use_cache` 預設是 `False`？
5. 欄位不一致（StationUID / StationID / UID；PositionLat / Lat）要怎麼做 production-minded 的容錯？

---

## 2) 我改了哪些檔案？為什麼要這樣拆？

- 修改（加上逐行英文註解）：
  - `src/metrobikeatlas/ingestion/tdx_metro_client.py`
  - `src/metrobikeatlas/ingestion/tdx_bike_client.py`
- 新增（超詳細中文教學）：
  - `docs/04_phase4_tdx_dataset_clients.md`（本文件）

為什麼要拆成兩個 dataset client：

- Metro 與 Bike 的 endpoint、欄位、更新頻率都不同
- 但它們共享同一個「可靠 HTTP + token」底層（Phase 3 的 `TDXClient`）

你可以把架構想成：

```
TDXClient (auth + retry + timeout)
  ├─ TDXMetroClient (which endpoints + parse station + optional ridership)
  └─ TDXBikeClient  (which endpoints + parse station + parse availability)
```

---

## 3) 核心概念講解（術語中英對照）

### 3.1 Bronze vs Silver：到底「解析」應該在哪裡做？

- **Bronze（原始層）**：保存「API 回來的原封不動 JSON」，加上抓取時間與 request metadata（可追溯）。
- **Silver（標準化層）**：把 JSON 轉成穩定欄位與型別，讓 join/resample/analytics 都能重用。

本 repo 的策略是：

- Bronze：由 scripts 抓 raw JSON → 寫檔（`data/bronze/...`）
- Silver：讀 Bronze JSON → 用 `TDX*Client.parse_*` 做標準化 → 輸出 CSV

為什麼 `parse_*` 放在 `TDXMetroClient`/`TDXBikeClient`？

- 解析規則與「資料源欄位差異」高度相關（TDX 的不一致是常態）
- 集中一處可以讓你：
  - 修一次就全站受益（所有 scripts/pipe 都一致）
  - 減少 copy-paste parsing code

### 3.2 Cache（快取）在這裡的角色

這裡的 `JsonFileCache` 是「MVP 等級」的輔助：

- 目的：減少重複 API call（更快、更省配額）
- 不是：取代 Bronze data lake（Bronze 才是可追溯的正式落地）

所以你會看到：

- stations：通常可 cache（更新不會那麼頻繁）
- availability：預設不 cache（realtime-ish，cache 反而會讓你拿到舊資料）

### 3.3 Production-minded parsing：容錯但不吞錯

TDX 不同城市/系統的欄位可能不一致，因此我們採用「多 key fallback」：

- `StationUID` / `StationId` / `StationID` / `UID`
- `StationPosition.PositionLat` / `Lat` / `latitude`

但有些欄位是「缺了就沒意義」：

- station id、lat/lon、availability timestamp

這些我們選擇 **fail fast**（直接 `ValueError`），因為：

- 你寧願早點發現資料破掉，也不要默默產生錯誤的 Silver 表

---

## 4) 程式碼分區塊貼上

### 4.1 `TDXMetroClient.list_stations()`：呼叫 endpoint + optional cache + parse

```py
def list_stations(self, city: str, *, use_cache: bool = True) -> list[MetroStation]:
    path = self._metro.stations_path_template.format(city=city)
    params = {"$format": "JSON"}

    cache_key = None
    if self._cache is not None and use_cache:
        cache_key = self._cache.make_key(
            "tdx:metro:stations",
            {"base_url": self._settings.base_url, "path": path, "params": params},
        )
        cached = self._cache.get(cache_key)
        if cached is not None:
            return [self.parse_station(item, city=city) for item in cached]

    data = self._tdx.get_json(path, params=params)
    if self._cache is not None and use_cache and cache_key is not None:
        self._cache.set(cache_key, data)

    return [self.parse_station(item, city=city) for item in data]
```

你要注意的重點：

- `TDXClient` 只負責「可靠拿到 JSON」
- `TDXMetroClient` 才負責「把 JSON 變成 MetroStation」

### 4.2 `TDXMetroClient.fetch_ridership_timeseries()`：ridership 是 optional dataset

```py
if not self._metro.ridership_path_template:
    logger.warning("No metro ridership path configured; returning empty ridership series.")
    return []
```

為什麼要回 `[]`：

- 很多城市根本沒有 station-level ridership（或格式差異很大）
- MVP 要讓系統能跑：API/Web 會用 proxy 或 demo data 先補位

### 4.3 `TDXBikeClient.fetch_availability_snapshot()`：為什麼 `use_cache=False`？

```py
def fetch_availability_snapshot(self, city: str, *, use_cache: bool = False) -> list[BikeAvailability]:
    path = self._bike.availability_path_template.format(city=city)
    params = {"$format": "JSON"}
    ...
```

因為 availability 是 realtime-ish：

- 預設就應該拿「最新」資料
- cache 會讓你以為資料沒變（其實只是 cache 沒過期）

### 4.4 `parse_station()`：為什麼要做多 key fallback？

```py
station_id = (
    item.get("StationUID")
    or item.get("StationId")
    or item.get("StationID")
    or item.get("UID")
)
```

這是 production 的常態：

- 你不能假設上游資料永遠一致
- 你要把「常見變體」集中處理

### 4.5 `parse_availability()`：時間解析與坑

```py
ts_raw = item.get("UpdateTime") or item.get("SrcUpdateTime") or item.get("UpdateTimestamp")
ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
```

你要注意：

- `Z` 代表 UTC（ISO-8601 的寫法）
- Python `datetime.fromisoformat` 需要 `+00:00` 才能解析成 timezone-aware datetime

---

## 5) 常見錯誤與排查

### 5.1 `ValueError: Missing station id in record`

代表：

- 上游 payload 其中一筆沒有 id（可能是格式變了、或你打到不同資料集）

排查：

- 把那筆 `item` 印出來（或在 debug 時存到 Bronze）
- 更新 `parse_station` 的 fallback key list（只在你確認新欄位名合理時）

### 5.2 `TypeError/ValueError` on lat/lon parsing

代表：

- `StationPosition` 缺失或欄位名不同，導致 `float(None)` 或 `float("...")` 失敗

排查：

- 檢查 `pos` 裡實際有哪些 key（不同城市/系統差異很大）
- 規劃「必要欄位」：lat/lon 缺失通常應該 fail fast

### 5.3 Availability timestamp parse error

代表：

- `UpdateTime` 的格式不是 ISO-8601 或包含奇怪字串

排查：

- 先看原始字串
- 如果格式不是 ISO-8601，考慮加一個更健壯的 parser（但要小心不要引入不必要依賴）

---

## 6) 本階段驗收方式（中文 + English commands）

### 6.1 Unit tests

```bash
pytest -q
```

Expected:

- all tests pass (exit code 0)

### 6.2 Lint (optional)

```bash
ruff check src/metrobikeatlas/ingestion/tdx_metro_client.py src/metrobikeatlas/ingestion/tdx_bike_client.py
```

Expected:

- `All checks passed!`

