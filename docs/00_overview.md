# 00 — 專案總覽（MetroBikeAtlas）

## 1) 本階段目標

這份文件的目標是讓你在「還沒開始改任何程式碼」之前，就先建立對整個專案的**全局理解**：

- 這個專案在解決什麼問題（捷運 × 共享單車 × 城市因子分析）
- 資料從哪裡來、怎麼進來、怎麼被整理成 API / Web UI 能用的形狀
- 你在 repo 裡應該從哪裡開始看、要怎麼跑起來、哪些檔案是關鍵入口

> 重要：這個 repo 以 Python（FastAPI + 資料管線）為主，Web 端是純靜態頁面（HTML/CSS/JS）由 FastAPI 直接提供。

## 2) 我改了哪些檔案？為什麼要這樣拆？

本階段是「總覽」文件，因此只新增教學文件本身：

- 新增：`docs/00_overview.md`

原因：

- 你需要先知道整個系統的**資料流（data flow）**：從外部資料 → 本地資料層 → API → 瀏覽器 DOM，才不會在後面每個模組都像在「拼拼圖」。
- 後續的教學文件會用 `docs/01_phase1_bootstrap.md`、`docs/02_phase2_...` 的方式逐步深入；`00_overview` 是全系列的「索引」。

## 3) 核心概念講解（術語中英對照）

下面先把你一定會反覆看到的概念講清楚：

### 3.1 資料分層（Bronze / Silver / Gold）

- **Bronze（原始層）**：存放「原封不動」的 API 回應（通常是 JSON），加上請求參數與抓取時間，重點是可追溯（traceability）。
- **Silver（標準化層）**：把資料整理成一致的欄位、型別、時區，適合做 join / resample。
- **Gold（分析/特徵層）**：更偏向報表與分析的產物，例如 station-level features、correlation、clusters。

### 3.2 Demo mode vs Real data mode

- **Demo mode**：不需要 TDX 憑證，後端用「可重現的假資料」回應 API，確保 UI 能正常運作。
- **Real data mode**：讀取 `data/silver/` 與 `data/gold/`，需要你先跑管線把資料生出來。

### 3.3 FastAPI、API、Web UI、DOM（資料怎麼到畫面上）

- **FastAPI**：Python web framework，用來提供 HTTP API。
- **API**：例如 `/stations`、`/station/{id}/timeseries`，Web UI 會用 `fetch()` 去呼叫它們。
- **Web UI（HTML/CSS/JS）**：瀏覽器端的程式，負責畫地圖（Leaflet）、畫圖表（Chart.js）、處理控制面板（DOM event）。
- **DOM（Document Object Model）**：瀏覽器中表示 UI 結構的物件樹，你看到的按鈕、選單、表格都是 DOM 節點。

## 4) 程式碼分區塊貼上（你應該從哪裡開始看）

### 4.1 專案入口（建議你第一個看的檔案）

執行 `python scripts/run_api.py` 會建立 FastAPI app 並啟動伺服器：

```py
# scripts/run_api.py (excerpt)
from metrobikeatlas.api.app import create_app
from metrobikeatlas.config.loader import load_config

config = load_config()
app = create_app(config)
uvicorn.run(app, host="127.0.0.1", port=8000)
```

### 4.2 API 與 Web UI 的交會點

FastAPI app 會把 `web/` 當作靜態資源提供給瀏覽器：

```py
# src/metrobikeatlas/api/app.py (excerpt)
assets_dir = static_dir / "static"
app.mount("/static", StaticFiles(directory=assets_dir), name="static")

@app.get("/")
def index() -> FileResponse:
    return FileResponse(index_html)
```

你在瀏覽器打開 `/`，會拿到 `index.html`，然後 `index.html` 會載入 `/static/app.js`，接著 JS 會呼叫 API。

## 5) 逐段解釋（做什麼 / 為什麼 / 還可以怎麼做）

### 5.1 為什麼要用「入口 script」而不是直接在模組裡啟動？

做什麼：

- `scripts/run_api.py` 是「可執行入口」，它只負責把 config + app 組起來，然後交給 uvicorn。

為什麼：

- 你可以在測試（pytest）或其他程式裡 import `create_app()`，而不會因為 import 就「自動開伺服器」。
- 這是初學者最容易踩雷的點：把 side effects 寫在 import 時間，會讓程式難測試、也難部署。

還可以怎麼做：

- 後續你也可以新增 `scripts/run_pipeline_mvp.py` 的教學文件，解釋資料管線怎麼一鍵跑完。

### 5.2 為什麼要用 FastAPI 直接 serving 靜態頁面？

做什麼：

- 目前 UI 是單一頁面 dashboard，FastAPI 同時提供 API 和 `index.html`。

為什麼：

- MVP 階段部署簡單：一個 Python 服務就能跑起來。
- 你可以先把重心放在資料與分析，不需要先做複雜前端工程。

還可以怎麼做：

- 如果未來 UI 變成大型 SPA（React/Vue），常見做法是 UI build 後由 CDN 或 Nginx 提供，但 API 仍由 FastAPI 提供。

## 6) 常見錯誤與排查（初學者最常卡住的地方）

1) 進入頁面一片空白

- 檢查 API 是否啟動：終端機有沒有看到 uvicorn 在跑。
- 檢查 `/static/app.js` 是否能載入：瀏覽器 DevTools → Network 看看是否 404。

2) UI 有，但沒有資料

- Demo mode：應該一定有資料（假資料）。
- Real data mode：你需要先建立 `data/silver/`，否則 API 會找不到檔案。

3) 控制面板調參數沒反應

- UI 其實是重新呼叫 API（帶 query params），所以你要看 DevTools → Network 的 URL 是否有帶上你調的參數。

## 7) 本階段驗收方式（中文 + 英文命令）

中文說明：

- 你只要能把 API 跑起來，並且瀏覽器能開啟 UI，就代表「總覽階段」完成。

English commands:

1) Install deps and run:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python scripts/run_api.py
```

Expected:

- Terminal shows Uvicorn running on `http://127.0.0.1:8000/`
- Opening that URL shows the dashboard UI (map + panels)

