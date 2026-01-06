# 03 — Phase 3：TDX 基礎客戶端（OAuth Token + Reliable HTTP）

## 1) 本階段目標

前兩個 Phase 我們已經把：

- 專案怎麼啟動（FastAPI app factory）
- UI 怎麼呼叫 API（Route → Service → Repository）

都打通了。

Phase 3 開始我們要把「真實世界資料」的入口做好：**TDX API**。

這個 Phase 的目標是讓你能回答下面這些「一定要搞懂」的問題：

1. 為什麼 TDX 需要先打 Token API（OAuth2 client credentials）？
2. 為什麼我們要把 token cache 在 client instance，而不是每次 request 都重新取 token？
3. 為什麼我們要用 `requests.Session()` + `Retry`？這跟 production mindset 有什麼關係？
4. 為什麼要明確設定 timeout、重試、backoff（退避）？如果不做會怎樣？
5. 什麼是「可觀測性」的一部分：錯誤訊息要有足夠 context、但不能洩漏 secrets。

> 重點：這個 Phase 不做「抓哪些資料」的 domain 邏輯（那是下一個 Phase）。  
> 我們先把「所有 TDX 呼叫都會用到的底層能力」做好：認證、HTTP、錯誤分類、重試策略。

---

## 2) 我改了哪些檔案？為什麼只改這個？

- 修改（加上逐行英文註解）：`src/metrobikeatlas/ingestion/tdx_base.py`
- 新增（超詳細中文教學）：`docs/03_phase3_tdx_base_client.md`（本文件）

為什麼 Phase 3 只改 `tdx_base.py`：

- `tdx_base.py` 是 **所有 TDX 資料源 client 的共用底層**，包含 token、session、retries。
- 如果底層做得不穩，後面你寫 `TDXMetroClient` / `TDXBikeClient` 都會一直出現：
  - 401 token 過期
  - 429 rate limit
  - 5xx transient failure
  - request 卡住（沒 timeout）
  - 一堆散落的 try/except（難維護）

先把底層「標準化」，後面新增資料集就只是「換 endpoint path + parse JSON」。

---

## 3) 核心概念講解（術語中英對照）

### 3.1 OAuth2 Client Credentials（客戶端憑證流程）

- **OAuth2**：一種授權/認證的標準機制。
- **Client Credentials flow**：常見的 server-to-server 認證方式。
  - 你用 `client_id` + `client_secret` 去 token endpoint 換一個短效的 `access_token`。
  - 之後你呼叫真正資料 API 時，用 `Authorization: Bearer <token>` 帶上這個 token。

在本 repo，憑證來自環境變數：

- `TDX_CLIENT_ID`
- `TDX_CLIENT_SECRET`

為什麼要用 env vars（而不是寫死在 code）：

- secrets 不能進 git（安全）
- production 部署可以用不同環境的 secrets（dev/staging/prod）

### 3.2 Bearer Token（持有人令牌）

- **Bearer token**：一種「拿到 token 的人就能用」的憑證（所以很敏感）。
- 常見 header：
  - `Authorization: Bearer <access_token>`

因此：

- error message 不應該把 token 印出來（避免 log 洩漏）

### 3.3 requests.Session（連線重用）

為什麼不用 `requests.get(...)` 每次打一次：

- `Session` 會重用 TCP 連線（keep-alive）
- 效能更好、也比較不會對 API 造成額外負擔

### 3.4 Retry + Backoff（重試與退避）

真實世界 API 一定會遇到 transient failures：

- **429 Too Many Requests**：被 rate limit
- **500/502/503/504**：上游服務暫時不穩

如果你不做 retry/backoff：

- pipeline 會常常「跑到一半就爆掉」
- 你會很難做長時間收集（例如每 5 分鐘抓一次 bike availability）

我們用 urllib3 的 `Retry`，並掛在 `HTTPAdapter` 上：

- 讓「重試策略」集中在 client 底層
- 上層 code（metro/bike client）不需要每個地方自己寫 retry loop

### 3.5 Timeout（超時）是 production 必備

沒有 timeout 的 network call 是很危險的：

- 可能因為網路卡住而永遠掛住
- 你的 cron job / pipeline 會堆積、無法回復

所以 `TDXClient` 在所有 request 都用同一個 `timeout_s`。

### 3.6 Error taxonomy（錯誤分類）

在本 repo 我們把錯誤分成兩類（這是很重要的 production 習慣）：

- `TDXAuthError`：token 流程失敗（憑證、token endpoint、token response 格式）
- `TDXRequestError`：呼叫資料 API 失敗（4xx/5xx，或 retry 後仍失敗）

這樣做的好處：

- 你在 CLI script 可以針對 auth error 給更清楚的提示（請設定 env vars）
- 你在 pipeline 可以針對 request error 做告警或更高層 retry

---

## 4) 程式碼分區塊貼上

> 注意：下面的 code block 是從 `src/metrobikeatlas/ingestion/tdx_base.py` 擷取（包含逐行英文註解）。

### 4.1 Imports：為什麼要這些依賴？

```py
from __future__ import annotations

# `dataclass` reduces boilerplate for small "data holder" types like credentials and access tokens.
from dataclasses import dataclass
# We use timezone-aware `datetime` values (always UTC) so expiry comparisons are unambiguous.
from datetime import datetime, timedelta, timezone
# `json.dumps` is used only for safe, truncated debug output in error messages.
import json
# Typing helpers keep our interfaces explicit while we still operate on JSON dicts in the MVP.
from typing import Any, Mapping, MutableMapping, Optional

# `requests` performs HTTP calls; we wrap it to centralize retries, auth, and error handling.
import requests
# `HTTPAdapter` lets us mount a retry policy onto a `requests.Session` (production-minded robustness).
from requests.adapters import HTTPAdapter
# `Retry` implements backoff for transient failures (rate limits, 5xx), without manual sleep loops.
from urllib3.util.retry import Retry
```

### 4.2 Credentials：為什麼要用 env vars？

```py
@dataclass(frozen=True)
class TDXCredentials:
    client_id: str
    client_secret: str

    @staticmethod
    def from_env() -> "TDXCredentials":
        import os

        client_id = os.getenv("TDX_CLIENT_ID")
        client_secret = os.getenv("TDX_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise ValueError("Missing env vars: TDX_CLIENT_ID and TDX_CLIENT_SECRET")
        return TDXCredentials(client_id=client_id, client_secret=client_secret)
```

### 4.3 Token cache：為什麼要有 `_Token` 與 skew？

```py
@dataclass
class _Token:
    access_token: str
    expires_at: datetime

    def is_expired(self, now: datetime, *, skew_seconds: int = 60) -> bool:
        return now >= (self.expires_at - timedelta(seconds=skew_seconds))
```

### 4.4 Session + Retry：怎麼把重試策略「掛進去」？

```py
self._session = requests.Session()
self._session.headers.update({"User-Agent": user_agent})

retry = Retry(
    total=max_retries,
    connect=max_retries,
    read=max_retries,
    status=max_retries,
    backoff_factor=backoff_factor,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET", "POST"),
    raise_on_status=False,
)
self._session.mount("https://", HTTPAdapter(max_retries=retry))
self._session.mount("http://", HTTPAdapter(max_retries=retry))
```

### 4.5 取 token：token endpoint → `_Token`

```py
def _fetch_token(self) -> _Token:
    payload = {
        "grant_type": "client_credentials",
        "client_id": self._credentials.client_id,
        "client_secret": self._credentials.client_secret,
    }
    resp = self._session.post(self._token_url, data=payload, timeout=self._timeout_s)
    if resp.status_code >= 400:
        raise TDXAuthError(f"TDX token request failed ({resp.status_code}): {resp.text[:500]}")

    data = resp.json()
    access_token = data.get("access_token")
    expires_in = data.get("expires_in")
    if not access_token or not expires_in:
        raise TDXAuthError(f"TDX token response missing fields: {json.dumps(data)[:500]}")

    expires_at = self._now_utc() + timedelta(seconds=int(expires_in))
    return _Token(access_token=access_token, expires_at=expires_at)
```

### 4.6 呼叫資料 API：Authorization header + 401 retry once

```py
def get_json(...):
    url = self._build_url(path)
    req_headers: MutableMapping[str, str] = {
        "Authorization": f"Bearer {self._get_token()}",
        "Accept": "application/json",
    }
    if headers:
        req_headers.update(headers)

    resp = self._session.get(url, params=params, headers=req_headers, timeout=self._timeout_s)
    if resp.status_code == 401:
        self._token = None
        req_headers["Authorization"] = f"Bearer {self._get_token()}"
        resp = self._session.get(url, params=params, headers=req_headers, timeout=self._timeout_s)

    if resp.status_code >= 400:
        raise TDXRequestError(...)
    return resp.json()
```

---

## 5) 逐段解釋（初學者版本）

### 5.1 為什麼 `_fetch_token()` 不能每次 request 都呼叫？

如果你每打一次資料 API 都先打 token endpoint：

- 你會把 token endpoint 當成瓶頸（更容易被 rate limit）
- latency 變長（每次都多一次 HTTP call）
- token endpoint 掛了你就完全不能做任何事

所以我們做 token caching：

- 第一次呼叫時才取 token
- 後面重用直到快過期（skew 提前 refresh）

### 5.2 為什麼要用 skew 提前 refresh？

如果你在 token「剛好過期」才 refresh，你會遇到一種很討厭的問題：

- 你送出 request 時 token 還沒過期
- request 在路上跑了一下
- 到 server 時 token 過期了 → 401

這會造成：

- 偶發、難重現的 401
- 你以為是 credentials 壞了，其實只是 timing

skew 就是在避免這種 edge case。

### 5.3 為什麼 401 要 retry once？

很多 API 的 token 失效是「短暫且可恢復」的，例如：

- token 被撤銷
- token 剛過期

我們做的策略是：

1. 收到 401 → 清掉 `_token`
2. 重新取 token
3. 同一個 request retry 一次

這樣可以避免：

- pipeline 因為偶發 401 直接失敗

但也要注意：

- 只 retry 一次，避免無限迴圈
- 如果仍然失敗，丟 `TDXRequestError` 讓上層決定怎麼處理

### 5.4 為什麼 Retry 要處理 429 與 5xx？

你做「每 5 分鐘抓一次」這種長時間收集，429/5xx 幾乎是必然。

Retry + backoff 的目標不是「掩蓋問題」，而是：

- 對 transient failure 更有韌性（resilient）
- 減少手動重跑成本

同時我們也限制：

- 只對 `GET/POST` 做 retry
- 最大重試次數可配置（`max_retries`）

### 5.5 為什麼 error message 要截斷（`[:500]`）？

production 常見做法：

- log 要有足夠 context（狀態碼、url、params）
- 但不要把整個 response body 印爆（log 太大、也可能包含敏感資訊）

所以我們用 `resp.text[:500]`：

- 保留一小段訊息協助 debug
- 避免 log 爆炸

---

## 6) 常見錯誤與排查

### 6.1 `ValueError: Missing env vars ...`

原因：

- 你沒有設定 `TDX_CLIENT_ID` / `TDX_CLIENT_SECRET`

排查：

- 先把 `.env.example` 複製成 `.env`
- 把你的 TDX 憑證填進去（不要 commit）
- 確認你有載入環境變數（例如用 `python-dotenv` 的 loader 或 shell export）

### 6.2 `TDXAuthError: token request failed (401/403/...)`

原因可能有：

- 憑證錯誤
- token_url 不對
- TDX token service 暫時不穩

排查：

- 確認 config 的 `tdx.token_url` 是否正確
- 確認 env vars 是否正確
- 觀察 error message 的 status code 與 body 截斷

### 6.3 `TDXRequestError: request failed (429)`

原因：

- 你被 rate limit

排查：

- 先降低你的呼叫頻率（例如收集間隔拉長）
- 調整 retry/backoff（但不要無限重試）
- 對長期收集：用 queue/worker 或 schedule 控制流量

### 6.4 Request 卡住很久

如果你看到程式長時間沒有反應：

- 檢查 timeout 是否被改成太大
- 檢查網路環境是否不穩

這也是為什麼我們一定要有 `timeout_s`。

---

## 7) 本階段驗收方式（中文 + 英文命令）

### 7.1 Run unit tests

```bash
pytest -q
```

預期結果：

- 所有測試通過（exit code 0）

### 7.2 Static check (optional but recommended)

```bash
ruff check src/metrobikeatlas/ingestion/tdx_base.py
```

預期結果：

- `All checks passed!`

### 7.3 Manual smoke test (requires valid TDX credentials)

> 如果你有 TDX 憑證且你的環境允許對外連線，你可以跑下一個 Phase 的 script（抓 station metadata）。
> Phase 3 本身只保證底層 client 正確，資料集層的抓取會在下一個 Phase 詳解。

