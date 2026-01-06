# 07 — Phase 7：一鍵跑完整 MVP 管線（run_pipeline_mvp.py）

## 1) 本階段目標

到 Phase 6，我們已經把每個「小零件」做出來了：

- 抓 Bronze（stations / availability）
- 建 Silver（stations / timeseries / links）

但初學者最常遇到的問題是：

> 「我到底要照什麼順序跑？每次都要記一堆命令嗎？」

Phase 7 的目標就是把整個 MVP 管線封裝成 **一支可重現的入口**：

- `scripts/run_pipeline_mvp.py`

它會依順序幫你跑：

1. TDX → Bronze（可選）
2. Bronze → Silver（必跑）
3. Silver validation（必跑）
4. (Optional) 匯入外部捷運運量 CSV（metro_timeseries.csv）
5. Silver → Gold features（可選）
6. Gold analytics（可選）

---

## 2) 我改了哪些檔案？

- 修改（加上逐行英文註解）：
  - `scripts/run_pipeline_mvp.py`
- 新增（超詳細中文教學）：
  - `docs/07_phase7_run_pipeline_mvp.md`（本文件）

---

## 3) 核心概念講解（術語中英對照）

### 3.1 Pipeline runner 的設計哲學：用 subprocess 跑小腳本

你可能會想：「為什麼不把全部邏輯寫在一個 Python 檔案裡？」

我們採取的做法是：

- 每個步驟都有自己的 script（單一責任、容易測試/替換）
- `run_pipeline_mvp.py` 只負責「串接順序 + 參數傳遞 + fail-fast」

好處：

- 你可以單獨 debug 某一步（例如只跑 build_silver）
- runner 只是一個 orchestrator，不會變成巨型怪物檔

### 3.2 用 env 讓所有 subprocess 共用同一份 config

你會看到 runner 做這件事：

- 如果你有傳 `--config path/to.json`
- runner 會設定 `METROBIKEATLAS_CONFIG_PATH`
- 之後所有子腳本就會讀到同一份 config

這樣可以避免：

- 你以為你在用 A config，其實某個子腳本還在用 default config

### 3.3 為什麼 runner 會先 validate Silver？

因為 Silver 是後面所有 feature/analytics/API 的共同基礎。

如果 Silver schema 壞了：

- 你後面做再多分析也沒有意義

所以我們用 `validate_silver_dir(..., strict=True)` 做 fail-fast。

### 3.4 Metro curve fallback：有運量就用運量，沒有就用 proxy

在 runner 裡你會看到 target metric 的選擇邏輯：

- 如果 `data/silver/metro_timeseries.csv` 存在 → 用 `metro_ridership`
- 否則 → 用 `metro_flow_proxy_from_bike_rent`

這個設計符合 MVP 需求：

- 沒有官方運量時，系統仍可跑（用 proxy）
- 一旦你匯入運量 CSV，就能自動切換成「真值」

---

## 4) 常用命令（你可以直接複製貼上）

### 4.1 最小可跑（只建 Silver；前提是你已經有 Bronze）

```bash
python scripts/run_pipeline_mvp.py --skip-extract-stations --skip-features --skip-analytics
```

### 4.2 從頭跑（需要 TDX creds + network）

```bash
python scripts/run_pipeline_mvp.py --collect-duration-seconds 3600 --collect-interval-seconds 300
```

你會得到：

- Bronze stations（metro/bike）
- Bronze availability（收集 1 小時）
- Silver（stations/timeseries/links）
- Gold features + analytics（如果沒有 skip）

### 4.3 匯入外部捷運運量（用 CSV 覆蓋 proxy）

```bash
python scripts/run_pipeline_mvp.py \
  --skip-extract-stations \
  --import-metro-csv path/to/metro.csv \
  --import-station-id-col station_id \
  --import-ts-col ts \
  --import-value-col value \
  --import-align \
  --import-granularity hour \
  --import-agg sum
```

---

## 5) 常見錯誤與排查

### 5.1 缺少 TDX 憑證

症狀：

- runner 一開始就噴：`Missing TDX credentials...`

排查：

- 設定 `TDX_CLIENT_ID` / `TDX_CLIENT_SECRET`（建議放 `.env`，不要 commit）

### 5.2 `subprocess.CalledProcessError`

代表：

- 某個子腳本失敗（例如 build_silver 找不到 Bronze）

排查順序：

1. 看 log：「Running: ...」最後跑到哪個指令
2. 直接單獨跑那支 script（會更容易定位）

### 5.3 Silver validation 失敗

代表：

- Silver 表 schema 或內容不符合預期（例如缺欄位、型別錯）

排查：

- 先看 `data/silver/` 產出的 CSV
- 回頭確認 Bronze payload 是否符合 parse 規則（Phase 4）

---

## 6) 本階段驗收方式（中文 + English commands）

```bash
pytest -q
ruff check scripts/run_pipeline_mvp.py
```

（有網路與 TDX creds 時）你也可以做一次完整跑：

```bash
python scripts/run_pipeline_mvp.py --collect-duration-seconds 600 --collect-interval-seconds 300 --skip-features --skip-analytics
```

