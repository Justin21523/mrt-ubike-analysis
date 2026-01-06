# 01 — Phase 1：啟動流程（Bootstrap：FastAPI + Web UI）

## 1) 本階段目標

在這個 Phase，你會學到「一個完整可執行的 Web 專案」最基本的啟動流程：

1. 程式如何讀取設定（config）
2. 如何建立 FastAPI app（包含 routes 與 service）
3. 如何同時提供：
   - API（給前端 fetch 資料）
   - 靜態網頁（HTML/CSS/JS）
4. 為什麼要用「app factory（工廠函式）」避免全域變數，讓程式更好測試與維護

你完成這個 Phase 之後，後面的所有模組（資料管線、分析、控制面板）都會變得更好理解，因為你知道資料怎麼從後端流到瀏覽器 DOM。

## 2) 我改了哪些檔案？為什麼要這樣拆？

本 Phase 專注在「啟動流程」這條線，所以只改最關鍵的兩個入口檔案，並新增教學文件：

- 修改（加上逐行英文註解）：  
  - `scripts/run_api.py`：本地啟動入口（run server）
  - `src/metrobikeatlas/api/app.py`：建立 FastAPI app 的工廠（create_app）
- 新增（超詳細中文教學）：  
  - `docs/00_overview.md`：專案總覽（你現在正在讀的系列文件的索引）
  - `docs/01_phase1_bootstrap.md`：本文件（逐段講清楚 bootstrap）

為什麼要這樣拆：

- 初學者最常卡住的不是「資料科學」本身，而是「整個系統到底怎麼跑起來」。
- 我們先把「入口」講清楚，你後面改任何功能都會更有信心。

## 3) 核心概念講解（術語中英對照）

### 3.1 ASGI / Uvicorn（你在跑的其實是什麼）

- **ASGI（Asynchronous Server Gateway Interface）**：Python web app 的一種標準介面（類似規格），FastAPI 是 ASGI app。
- **Uvicorn**：一個 ASGI server，用來把 FastAPI app 跑起來，對外提供 HTTP。

> 簡單理解：FastAPI 是「你的網站程式」，Uvicorn 是「開門做生意的伺服器」。

### 3.2 App factory（工廠函式）

- **App factory**：用一個函式（例如 `create_app(config)`）回傳一個新的 app instance。

為什麼重要：

- 避免 import 模組時就產生 side effects（例如：直接啟動 server、讀檔、連線）
- 讓測試更容易：你可以在測試裡建立 app，但不用真的開 server

### 3.3 Dependency injection（依賴注入）的「入門版」

這個 repo 用一個很簡單、初學者友善的方式做 DI：

- 把 `StationService` 放在 `app.state` 上
- 在 route handler 用 `Depends(get_service)` 拿到它

好處：

- 沒有全域變數
- 你的資料流變得非常清楚：路由 → service → repository（demo/local）

## 4) 程式碼分區塊貼上

> 注意：下面貼的是「完整檔案」，因為本 repo 要求你可以直接對照每一行註解。

### 4.1 `scripts/run_api.py`

```py
# Use postponed evaluation of annotations so type hints don't require importing types at runtime.
# This helps avoid import cycles and keeps startup lightweight (especially important in CLI scripts).
from __future__ import annotations

# We import `uvicorn` to run our FastAPI application as an ASGI server during local development.
# In production you would typically run Uvicorn via a process manager (e.g., systemd, Docker, k8s).
import uvicorn

# We use a factory function so the FastAPI app can be created with a typed config (no global state).
from metrobikeatlas.api.app import create_app

# We load config at runtime so settings can be changed via `config/default.json` or environment variables
# without modifying code (production-minded configuration management).
from metrobikeatlas.config.loader import load_config



# This `main()` function is the script's single entrypoint, which keeps all side effects
# (config IO, app creation, server startup) in one place and makes the module import-safe.
def main() -> None:
    # Read the typed application config (timezone, join radius, demo mode, etc.).
    config = load_config()

    # Build the FastAPI app with routes + dependency wiring based on the config.
    app = create_app(config)

    # Start a local dev server on localhost; the static web UI is served by the same app.
    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    # This guard prevents accidental side effects when the module is imported by tests or other scripts.
    main()
```

### 4.2 `src/metrobikeatlas/api/app.py`

```py
# Use postponed evaluation of annotations so type hints stay as strings at runtime.
# This reduces import-time coupling (helpful for larger apps with many modules).
from __future__ import annotations

# We use `Path` to work with filesystem paths in a cross-platform way (no manual string joins).
from pathlib import Path

# `FastAPI` is the Python web framework that exposes our data as HTTP endpoints for the web UI.
from fastapi import FastAPI

# `FileResponse` efficiently streams a file from disk (used for serving `web/index.html`).
from fastapi.responses import FileResponse

# `StaticFiles` serves assets (JS/CSS) so the browser (DOM) can load the dashboard bundle.
from fastapi.staticfiles import StaticFiles

# API routes are defined in a separate module to keep the app factory small and testable.
from metrobikeatlas.api.routes import router

# `StationService` is our application service layer: it reads data (demo or local Silver/Gold)
# and shapes it into payloads that the frontend consumes.
from metrobikeatlas.api.service import StationService

# `AppConfig` is the typed config model so we can avoid globals and magic strings.
from metrobikeatlas.config.models import AppConfig

# Central logging configuration keeps operational debugging consistent across scripts and the API.
from metrobikeatlas.utils.logging import configure_logging


# This app factory builds the FastAPI application from a typed config.
# Keeping app construction in a function (instead of module-level globals) improves testability and reuse.
def create_app(config: AppConfig) -> FastAPI:
    # Configure Python logging early so every subsequent log line follows the same format/level.
    # Pitfall: `logging.basicConfig(...)` is a no-op if handlers already exist (common in notebooks/tests),
    # so treat this as best-effort for local/dev.
    configure_logging(config.logging)

    # Create the FastAPI application instance; the title shows up in the OpenAPI docs.
    app = FastAPI(title=config.app.name)

    # Store the service on `app.state` so route handlers can access it without global variables.
    # This is a simple dependency-injection pattern that keeps the dataflow explicit.
    app.state.station_service = StationService(config)

    # Register all API endpoints (e.g., `/stations`, `/station/{id}/timeseries`, `/config`).
    app.include_router(router)

    # The web UI lives under `web/` (configurable). It is a plain HTML page + static assets.
    static_dir = config.web.static_dir

    # We serve JS/CSS from `/static/*` (browser will request these files to render the dashboard DOM).
    assets_dir = static_dir / "static"

    # The root route (`/`) returns `index.html` so the user can open the dashboard in a browser.
    index_html = static_dir / "index.html"

    # Only mount `/static` if the directory exists so the API can still run in minimal environments.
    if assets_dir.exists():
        # `StaticFiles` does basic file serving; for high-traffic production you would typically
        # put a CDN / reverse proxy in front (and add caching headers).
        app.mount("/static", StaticFiles(directory=assets_dir), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        # Serve the HTML entrypoint; the browser then loads `/static/app.js` and renders the UI.
        # Pitfall: if `index.html` is missing, this will 500; keep the repo layout consistent.
        return FileResponse(index_html)

    # Return the fully constructed app so callers (scripts/tests) can decide how to run it.
    return app


# This helper resolves a stable repo root path for scripts that need to locate files reliably.
def resolve_repo_root() -> Path:
    # Resolve the repository root from this file location.
    # This is useful for scripts that need paths relative to the project without relying on CWD.
    return Path(__file__).resolve().parents[3]
```

## 5) 逐段解釋（做什麼 / 為什麼 / 還可以怎麼做）

### 5.1 `scripts/run_api.py` 的重點

這個檔案做的事非常「單純」，但它是整個專案的啟動入口。

#### (1) 讀設定：`config = load_config()`

做什麼：

- 把 `config/default.json` 讀進來，轉成 `AppConfig`（typed dataclass）

為什麼這樣設計：

- 初學者常見壞習慣：把參數硬寫在程式裡（例如 join 半徑、時區、demo/real），一改就要改程式碼。
- 這個專案要求 production mindset，所以參數必須集中在 config，並且可以被環境變數覆寫（例如部署到不同環境）。

還可以怎麼做：

- 後續 Phase 會教你把「UI 的調參數」與 config 的預設值對齊，避免前後端不一致。

#### (2) 建 app：`app = create_app(config)`

做什麼：

- 呼叫 app factory，建立 FastAPI instance

為什麼：

- 你可以在測試裡 import `create_app()`，不用啟動 uvicorn
- 你可以依不同設定建立不同 app（demo/real）

#### (3) 跑 server：`uvicorn.run(...)`

做什麼：

- 把 FastAPI app 交給 Uvicorn 跑，並開在 `127.0.0.1:8000`

常見坑：

- 你如果用 `0.0.0.0`，代表對外網卡都開放（在某些環境可能有安全風險）
- 如果 port 被占用，會報錯（你需要換 port）

### 5.2 `src/metrobikeatlas/api/app.py` 的重點

#### (1) `configure_logging(config.logging)`

做什麼：

- 設定 log level（INFO/DEBUG）與格式，讓你查資料流問題時有一致輸出

為什麼：

- 做資料工程時，最常見的 bug 是「資料沒抓到 / join 出來是空的 / 時區錯」  
  沒有好 log 你會很痛苦。

#### (2) `app.state.station_service = StationService(config)`

做什麼：

- 讓 route handler 可以用 `request.app.state.station_service` 取得 service

為什麼：

- 避免全域變數（global state）
- 讓依賴關係更清楚：Routes 不直接碰 pandas/檔案，而是呼叫 service

常見坑（進階但要先知道）：

- `StationService` 內部可能會載入 DataFrame（Silver/Gold）。如果資料很大，你要注意記憶體。
- Uvicorn 若開多個 worker，每個 worker 都會各自建立一份 service（這是正常但要知道）。

#### (3) `app.mount("/static", StaticFiles(...))` + `index()`

做什麼：

- 把 `web/static/*` 掛到 `/static/*`
- 把 `/` 回傳 `web/index.html`

為什麼：

- 這樣瀏覽器打開 `/` 就能看到 UI，不需要額外前端 server

常見坑：

- `web/index.html` 路徑錯或檔案不存在 → 500
- 你改了 `web/` 結構卻忘了同步 config → 404

## 6) 常見錯誤與排查（中文）

1) `ModuleNotFoundError: No module named 'metrobikeatlas'`

- 你可能沒有安裝依賴或沒有用正確的 venv
- 解法：重新建立 venv 並 `pip install -r requirements-dev.txt`

2) 打開 `http://127.0.0.1:8000/` 顯示 404 或下載檔案

- 檢查 `web/index.html` 是否存在
- 檢查 `config/default.json` 的 `web.static_dir` 是否指向正確資料夾

3) UI 顯示，但呼叫 API 失敗（右下角 status 出現 Error）

- 打開瀏覽器 DevTools → Network
- 看 `GET /stations`、`GET /station/...` 是否回 200
- 如果是 500，回到終端機看 Python error trace

## 7) 本階段驗收方式（中文 + 英文命令）

中文驗收：

- 你可以啟動 server
- 你可以在瀏覽器看到 dashboard
- 你按 `?` 能看到快捷鍵清單
- 你用控制面板改參數，右下角 status 會更新並重新載入資料

English commands:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
python scripts/run_api.py
```

Expected:

- `pytest -q` passes
- `python scripts/run_api.py` starts Uvicorn without stack traces
- Opening `http://127.0.0.1:8000/` shows the UI and no console errors in the browser
