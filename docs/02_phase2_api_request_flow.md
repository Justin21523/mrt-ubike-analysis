# 02 — Phase 2：API 請求流程（Request Flow：Route → Service → Repository → JSON）

## 1) 本階段目標

在 Phase 1 我們把「專案怎麼跑起來（bootstrap）」打通了；Phase 2 我們要把「瀏覽器點一下 → 後端回資料 → 前端畫出來」這條資料流講清楚。

你會學到：

1. **FastAPI 的路由層（routes）在做什麼**：把 HTTP request 變成 Python 函式呼叫，並把結果轉成 JSON 回給前端。
2. **Dependency Injection（依賴注入，DI）入門**：為什麼我們不用全域變數，而是用 `Depends(get_service)` 拿到 `StationService`。
3. **Service layer（服務層）存在的理由**：為什麼 route handler 不直接呼叫 repository，而是「route → service → repository」。
4. **API contract（API 契約）**：為什麼我們要用 Pydantic `response_model` + `model_validate`，讓前端（DOM）拿到穩定欄位。
5. **錯誤處理策略**：`KeyError` → 404、`ValueError` → 400、以及 FastAPI 自動的 422。

> 這個 Phase 是「把系統的骨架資料流」講清楚。你理解這段後，後面做 ingestion / preprocessing / features / analytics，都會知道資料應該放在哪一層、怎麼接上 API、怎麼讓前端用得穩。

---

## 2) 我改了哪些檔案？為什麼要這樣拆？

本 Phase 只改「API 請求流程」的核心兩個檔案，原因是：我們要先把 request flow 的責任切清楚，避免把商業邏輯塞在 route handler 裡，導致以後功能越長越難測。

- 修改（加上逐行英文註解）：
  - `src/metrobikeatlas/api/routes.py`
  - `src/metrobikeatlas/api/service.py`

為什麼這樣拆：

- `routes.py`：只處理「HTTP 相關」的事情（路由、query params、狀態碼、回應模型）
- `service.py`：只處理「應用層」的事情（demo/real mode 切換、把 repo 結果包成 UI 需要的形狀）
- `repository`：只處理「資料取得」與「資料聚合」的事情（從 demo/本地表取資料、做時序/空間 join）

> 你可以把這三層想成：Controller（routes）→ Use-case（service）→ Data access（repository）。

---

## 3) 核心概念講解（術語中英對照）

### 3.1 Route handler（路由處理器）

- **Route handler**：FastAPI 用 `@router.get(...)` 綁定一個 URL，當瀏覽器呼叫這個 URL，就會執行對應的 Python 函式。
- 你可以把它想成「HTTP → Python 的翻譯層」。

重點：

- handler 應該保持 **thin（薄）**：不要放太多商業邏輯，否則很難測、很難重用。

### 3.2 Dependency Injection（DI，依賴注入）

- **DI**：不是在函式裡自己 new 物件，而是讓框架（FastAPI）在每次 request 時「幫你把需要的物件準備好」。
- 本 repo 的做法是：
  1) 在 app factory 把 `StationService` 放到 `app.state`  
  2) 在 routes 用 `Depends(get_service)` 拿到同一個 service

好處：

- 不需要全域變數（更可測、更可維護）
- 你能清楚追蹤資料流：request → route → service → repo

### 3.3 API contract（API 契約）與 Pydantic response_model

前端（瀏覽器）通常會：

1. `fetch("/station/xxx/timeseries?...")`
2. `response.json()`
3. 把 JSON 填到 DOM（例如 Chart.js 畫圖、Leaflet 畫 marker）

如果後端今天回傳：

- 欄位名稱改了
- 型別改了（string 變 number）
- 某些欄位偶爾缺失

那前端就很容易：

- 直接噴錯（例如 `undefined`）
- 圖畫不出來（Chart.js 資料不符合預期）
- 地圖 marker 壞掉（lat/lon 欄位不一致）

所以我們要用 **response model（回應模型）** 來鎖定「我承諾給前端的 JSON 長什麼樣」。

在 FastAPI 你會看到兩層保護：

1. `@router.get(..., response_model=...)`：宣告 endpoint 的回應 schema
2. `SomeOut.model_validate(payload)`：在回傳前再做一次驗證（防守式設計），避免 repo 回傳怪資料把前端炸掉

### 3.4 Query parameters（查詢參數）作為「runtime overrides」

這個 MVP 的一個重要設計：**距離門檻（300/500m）、時間聚合（15min/hour/day）、權重等都放 config**，但 UI 仍然可以用 query params 暫時覆蓋，方便探索。

例如：

- 空間 join：
  - `join_method=buffer` + `radius_m=500`
  - `join_method=nearest` + `nearest_k=10`
- 時間對齊：
  - `granularity=hour`
  - `window_days=7`

為什麼 query params 要設成 `Optional[...] = None`？

- **None** 表示「使用 config 的預設值」
- 使用者只有在需要探索時才提供參數

這樣可以兼顧：

- Production-minded defaults（可重現、可控）
- Exploration flexibility（不重啟 server 也能調參）

### 3.5 錯誤處理：404 / 400 / 422 的差別

你會在 routes 看到：

- `KeyError` → 404：例如站點 ID 不存在
- `ValueError` → 400：例如使用者給了不合理的參數（半徑負值、K=0 等）
- 422：FastAPI 在「進入 handler 之前」就做型別驗證，如果不符合就直接回 422（例如 `nearest_k=abc`）

這個分層很重要，因為前端可以用不同方式提示使用者：

- 404：顯示「站點不存在」
- 400：顯示「參數錯誤，請調整設定」
- 422：通常是「表單/URL 拼錯」或「前端沒有正確送出型別」

### 3.6 `app.state` 的生命週期與 thread-safety（初學者必懂）

我們把 `StationService` 放在 `app.state`：

- 好處：只建一次 service，request 來了就拿同一個 instance（效能好、責任清楚）
- 風險：這代表 service 會被多個 request 共用（並行），所以 service **不要保存會被 request 改動的狀態**

目前的設計是安全的，因為：

- `StationService` 只保存 `config` 與 `repo`
- repository 在 MVP 是「讀取/計算結果」，不會把 request-specific state 存在 instance 上

如果你之後要加 cache，請注意：

- 不要把「跟 request 相關」的暫存寫進共享物件（容易 race condition）
- 優先用「純函式 + 明確輸入輸出」或「thread-safe cache」策略

---

## 4) 程式碼分區塊貼上

> 注意：以下程式碼是從目前 repo 的最新版擷取。你可以直接打開檔案對照英文逐行註解。

### 4.1 `src/metrobikeatlas/api/routes.py`：DI + 回應模型 + 錯誤映射

```py
from __future__ import annotations

# We use `Literal` to restrict certain query parameters to a small, documented set of values.
# We use `Optional[...]` for parameters that can be omitted so the server can fall back to config defaults.
from typing import Literal, Optional

# FastAPI primitives:
# - `APIRouter` groups endpoints (routes) so the app factory can include them cleanly.
# - `Depends` performs dependency injection (DI) per request (no global variables needed).
# - `HTTPException` converts Python errors into proper HTTP status codes + JSON error payloads.
# - `Request` gives access to `app.state` where we store our service object.
from fastapi import APIRouter, Depends, HTTPException, Request

# Pydantic response models:
# These models define the JSON schema returned to the browser and validate payload shape at runtime.
# This matters for maintainability: the frontend (DOM + fetch) can rely on stable fields and types.
from metrobikeatlas.api.schemas import (
    AnalyticsOverviewOut,
    AppConfigOut,
    BikeStationOut,
    NearbyBikeOut,
    SimilarStationOut,
    StationFactorsOut,
    StationOut,
    StationTimeSeriesOut,
)

# `StationService` is our thin application layer that hides whether we are in demo mode or real-data mode.
# Dataflow: HTTP request -> route handler -> StationService -> repository -> dict payload -> Pydantic model -> JSON response.
from metrobikeatlas.api.service import StationService


# A router is like a "mini app": it holds endpoints that can be attached to a FastAPI application.
router = APIRouter()


# Dependency provider: FastAPI will call this function per request when a handler declares `Depends(get_service)`.
# We keep the service on `app.state` so it is constructed once in the app factory (not per request).
def get_service(request: Request) -> StationService:
    # `app.state` is a generic container, so mypy cannot know the attribute exists; hence the `type: ignore`.
    # Pitfall: if `StationService` is not attached in `create_app`, this will raise `AttributeError` at runtime.
    return request.app.state.station_service  # type: ignore[attr-defined]
```

### 4.2 `/config`：把後端預設值交給前端初始化控制面板

```py
@router.get("/config", response_model=AppConfigOut)
def get_config(service: StationService = Depends(get_service)) -> AppConfigOut:
    # Read a snapshot of the typed config so the frontend can render defaults consistently with the backend.
    cfg = service.config
    # Return a Pydantic model instance; FastAPI will serialize it to JSON for the browser.
    return AppConfigOut(
        # Basic app metadata helps the UI show the correct title and which mode it is running in.
        app_name=cfg.app.name,
        demo_mode=cfg.app.demo_mode,
        # Temporal config controls how time series are aligned for visualization (15min/hour/day, timezone).
        temporal={
            "timezone": cfg.temporal.timezone,
            "granularity": cfg.temporal.granularity,
        },
        # Spatial config controls how bike stations are associated with metro stations (buffer radius vs nearest K).
        spatial={
            "join_method": cfg.spatial.join_method,
            "radius_m": cfg.spatial.radius_m,
            "nearest_k": cfg.spatial.nearest_k,
        },
        # Analytics config controls similarity and clustering (used by "similar stations" in the UI).
        analytics={
            "similarity": {
                "top_k": cfg.analytics.similarity.top_k,
                "metric": cfg.analytics.similarity.metric,
                "standardize": cfg.analytics.similarity.standardize,
            },
            "clustering": {
                "k": cfg.analytics.clustering.k,
                "standardize": cfg.analytics.clustering.standardize,
            },
        },
        # Map defaults let the UI start at a sensible location without hard-coding values in JS.
        web_map={
            "center_lat": cfg.web.map.center_lat,
            "center_lon": cfg.web.map.center_lon,
            "zoom": cfg.web.map.zoom,
        },
    )
```

### 4.3 `/stations`：地圖 marker 的資料來源

```py
@router.get("/stations", response_model=list[StationOut])
def list_stations(service: StationService = Depends(get_service)) -> list[StationOut]:
    # Fetch metro stations (optionally enriched with district/cluster if Gold tables are present).
    stations = service.list_stations()
    # Map internal dict keys to the public API field names expected by the frontend.
    return [
        StationOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            city=s.get("city"),
            system=s.get("system"),
            district=s.get("district"),
            cluster=s.get("cluster"),
        )
        for s in stations
    ]
```

### 4.4 `/station/{id}/timeseries`：前端兩張曲線的主要資料

```py
@router.get("/station/{station_id}/timeseries", response_model=StationTimeSeriesOut)
def station_timeseries(
    # Path parameter: the unique metro station id (used as a stable key across tables).
    station_id: str,
    # Query params: spatial join controls for which bike stations get aggregated.
    join_method: Optional[Literal["buffer", "nearest"]] = None,
    radius_m: Optional[float] = None,
    nearest_k: Optional[int] = None,
    # Query params: temporal alignment controls for bucketing timestamps.
    granularity: Optional[Literal["15min", "hour", "day"]] = None,
    timezone: Optional[str] = None,
    window_days: Optional[int] = None,
    # Query param: choose whether to prefer real ridership or a bike-derived proxy in the response.
    metro_series: Literal["auto", "ridership", "proxy"] = "auto",
    # Dependency injection: FastAPI calls `get_service` and passes the result here.
    service: StationService = Depends(get_service),
) -> StationTimeSeriesOut:
    try:
        # Delegate to the service layer so route code stays "thin" and focused on HTTP concerns.
        payload = service.station_timeseries(
            station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            granularity=granularity,
            timezone=timezone,
            window_days=window_days,
            metro_series=metro_series,
        )
    except KeyError:
        # A missing station id maps to 404 so the frontend can show a "not found" message.
        raise HTTPException(status_code=404, detail="Station not found")
    except ValueError as e:
        # Bad user input (e.g., unsupported granularity) maps to 400 for a clear client-side error.
        # Note: FastAPI may also return 422 for validation errors before this handler runs.
        raise HTTPException(status_code=400, detail=str(e))
    # Validate the payload shape against the Pydantic model (defensive programming for API stability).
    return StationTimeSeriesOut.model_validate(payload)
```

### 4.5 `src/metrobikeatlas/api/service.py`：route 與 repository 的中介層

```py
from __future__ import annotations

# `Any` is used for our internal "dict payload" boundary between repository and service.
# In a later phase we may tighten these to TypedDicts/Pydantic models, but `Any` keeps MVP flexible.
from typing import Any

# DemoRepository provides deterministic in-memory/sample data so the UI always has something to render.
from metrobikeatlas.demo.repository import DemoRepository
# `AppConfig` is the single source of truth for runtime settings (demo mode, spatial/temporal defaults, etc.).
from metrobikeatlas.config.models import AppConfig
# LocalRepository reads our local data lake tables (Silver/Gold) from disk in "real data" mode.
from metrobikeatlas.repository.local import LocalRepository


# `StationService` is a thin application layer between HTTP routes and repositories.
# Why have a service at all (instead of calling the repository directly from routes)?
# - Keeps route handlers focused on HTTP concerns (status codes, query params, response models).
# - Centralizes "mode switching" (demo vs real data) without global variables.
# - Provides a stable API for the frontend: route -> service -> repo -> dict -> Pydantic -> JSON -> DOM.
class StationService:
    # We inject config so this object is easy to construct in tests and does not read globals/env at import time.
    def __init__(self, config: AppConfig) -> None:
        # Store the typed config so routes (and `/config`) can expose defaults to the UI.
        self._config = config
        # Choose the repository implementation once at startup.
        # Pitfall: this service instance is shared across requests (stored on `app.state`), so it must be
        # effectively stateless or thread-safe; using a read-only repository object satisfies that.
        self._repo = DemoRepository(config) if config.app.demo_mode else LocalRepository(config)

    @property
    def config(self) -> AppConfig:
        # Expose config as a read-only property so callers cannot mutate settings accidentally.
        return self._config

    def list_stations(self) -> list[dict[str, Any]]:
        # Return metro station metadata for the map marker layer in the frontend.
        return self._repo.list_metro_stations()

    def list_bike_stations(self) -> list[dict[str, Any]]:
        # Return bike station metadata for overlays (nearby bikes, debug layers, etc.).
        return self._repo.list_bike_stations()

    def station_timeseries(
        self,
        station_id: str,
        *,
        join_method: str | None = None,
        radius_m: float | None = None,
        nearest_k: int | None = None,
        granularity: str | None = None,
        timezone: str | None = None,
        window_days: int | None = None,
        metro_series: str = "auto",
    ) -> dict[str, Any]:
        # Delegate to the repository which implements the actual retrieval + aggregation logic.
        # We keep the service signature close to the HTTP layer so runtime overrides stay explicit.
        return self._repo.station_timeseries(
            # `station_id` is the stable key that links station metadata, bike links, and time series.
            station_id,
            # Spatial join params control which bike stations are considered "near" this metro station.
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            # Temporal params control how raw timestamps are bucketed for charting.
            granularity=granularity,
            timezone=timezone,
            window_days=window_days,
            # `metro_series` lets the API choose real ridership when available, otherwise fall back to a proxy.
            metro_series=metro_series,
        )
```

---

## 5) 逐段解釋（初學者版本）

### 5.1 為什麼要有 `get_service()`？

你可能會想：為什麼不直接在每個 handler 裡寫 `StationService(load_config())`？

原因：

1. **避免每個 request 都重建 service**（慢、也可能造成資源浪費）
2. **避免全域變數**（測試不好寫、也會讓依賴關係變隱藏）
3. **讓資料流很清楚**：route 只管「拿到 service 然後叫它」

FastAPI 的 `Depends(get_service)` 會在每次 request：

- 先呼叫 `get_service(request)`
- 把回傳的 service 注入到 handler 參數 `service=...`

> 這就是 DI 的入門形態：你不用自己建立 service，也不用全域變數，框架會幫你把依賴準備好。

### 5.2 `/config` 為什麼很重要？

你會在 Web UI 看到很多控制項（例如 join_method、radius、granularity）。

如果前端把這些預設值硬寫死在 JS：

- 後端改 config，前端忘記改，就會不一致
- 你會很難做到「同一份設定 → 可重現結果」

所以我們把 config 的預設值透過 `/config` 給前端：

- 前端啟動時 fetch `/config`
- 把回傳的 defaults 填進 UI 控制面板

這樣你就能做到：

- config 是 single source of truth
- UI 只是「顯示與覆蓋」設定，不是「自己定義」設定

### 5.3 為什麼 `/timeseries` 要接受一堆 query params？

因為我們要同時滿足兩件事：

1. **可重現**：沒給參數就用 config 預設值（每次結果一致）
2. **可探索**：給參數就臨時覆蓋（不用改檔案、不用重啟 server）

而且我們在 handler 裡保持 thin：

- handler 不做 aggregation
- handler 只把參數轉交給 `service.station_timeseries(...)`

### 5.4 `StationTimeSeriesOut.model_validate(payload)` 的意義

這行是 MVP 裡非常「production mindset」的一步：

- repository 回傳的是 dict（彈性大，但容易出錯）
- 我們在回傳給前端前，用 Pydantic 驗證一次

好處：

- 你可以更早發現「資料表欄位缺失」「型別不對」等問題
- 前端比較不會收到「半對半錯」的 payload（最難 debug）

### 5.5 `StationService` 為什麼要存在？

`StationService` 的核心價值是「把 route 從資料細節解耦」：

- route 不需要知道 DemoRepository / LocalRepository 的差異
- route 不需要知道資料在哪裡（檔案、記憶體、CSV…）
- 你之後新增資料來源（例如真正的 MRT 運量表）只要改 repository，不用把 routes 全改一遍

---

## 6) 常見錯誤與排查

### 6.1 422 Unprocessable Entity（常見）

症狀：

- 你打 `/station/xxx/timeseries?nearest_k=abc` 會回 422

原因：

- `nearest_k` 宣告是 `Optional[int]`，FastAPI 在進 handler 前就會做型別驗證

排查：

- 看回傳 JSON 會指出哪個欄位無法 parse
- 確認前端送的是數字，不是字串

### 6.2 404 Station not found

症狀：

- 你點了不存在的 station id

原因：

- repository 查不到該 id，丟 `KeyError`，routes 把它轉成 404

排查：

- 先呼叫 `/stations` 看有哪些合法 `id`

### 6.3 400 Bad Request（參數不合理）

症狀：

- 半徑負值、K=0、或某些 join/granularity 組合不支援

原因：

- repository/service 主動丟 `ValueError`

排查：

- 看 response 的 `detail` 字串（我們會把 `ValueError` message 回傳）
- 如果是 UI 控制項造成，檢查前端是否有做最小值限制

---

## 7) 本階段驗收方式（中文 + 英文命令）

### 7.1 Run tests

```bash
pytest -q
```

預期結果：

- 所有測試通過（exit code 0）

### 7.2 Run the API locally (demo mode)

```bash
python scripts/run_api.py
```

預期結果：

- Console 沒有 traceback
- 開啟 `http://127.0.0.1:8000/` 看到地圖與控制面板
- 開啟 `http://127.0.0.1:8000/docs` 看到 OpenAPI 文件

### 7.3 Smoke test the endpoints (optional)

```bash
curl -s http://127.0.0.1:8000/config | head
curl -s http://127.0.0.1:8000/stations | head
```

預期結果：

- `/config` 回傳包含 `demo_mode` 與 `spatial/temporal` defaults
- `/stations` 回傳一個 station list（demo mode 也會有資料）
