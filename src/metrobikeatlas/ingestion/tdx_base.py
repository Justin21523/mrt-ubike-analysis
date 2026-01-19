from __future__ import annotations

# `dataclass` reduces boilerplate for small "data holder" types like credentials and access tokens.
from dataclasses import dataclass
# We use timezone-aware `datetime` values (always UTC) so expiry comparisons are unambiguous.
from datetime import datetime, timedelta, timezone
# `json.dumps` is used only for safe, truncated debug output in error messages.
import json
# `logging` is used to report rate limiting and paging behavior without leaking secrets.
import logging
# `random` is used for small jitter in client-side throttling (avoid synchronized bursts).
import random
# `time` provides monotonic clocks and sleeping for client-side throttling.
import time
# Typing helpers keep our interfaces explicit while we still operate on JSON dicts in the MVP.
from typing import Any, Mapping, MutableMapping, Optional

# `requests` performs HTTP calls; we wrap it to centralize retries, auth, and error handling.
import requests
# `HTTPAdapter` lets us mount a retry policy onto a `requests.Session` (production-minded robustness).
from requests.adapters import HTTPAdapter
# `Retry` implements backoff for transient failures (rate limits, 5xx), without manual sleep loops.
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


# Custom exception for OAuth/token failures (credentials, token URL, malformed token response, etc.).
class TDXAuthError(RuntimeError):
    # We subclass `RuntimeError` so CLI scripts can fail fast without forced checked-exception handling.
    pass


# Custom exception for non-auth HTTP failures when calling TDX business endpoints.
class TDXRequestError(RuntimeError):
    # Keeping this separate from `TDXAuthError` lets callers decide whether to retry or alert on auth issues.
    pass


class TDXRateLimitError(TDXRequestError):
    """
    Raised when TDX returns 429 (rate limit).

    `retry_after_s` is best-effort parsed from `Retry-After` header when present.
    """

    def __init__(self, message: str, *, retry_after_s: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


class _RateLimiter:
    """
    Minimal client-side throttle.

    This is complementary to urllib3's Retry/backoff:
    - Retry/backoff handles transient errors (429/5xx) after they happen.
    - This throttle reduces the chance we hit rate limits in the first place (especially during paging).
    """

    def __init__(
        self,
        *,
        min_interval_s: float,
        jitter_s: float,
        now_fn=time.monotonic,
        sleep_fn=time.sleep,
    ) -> None:
        self._min_interval_s = max(float(min_interval_s), 0.0)
        self._jitter_s = max(float(jitter_s), 0.0)
        self._now = now_fn
        self._sleep = sleep_fn
        self._next_allowed_at = 0.0

    def wait(self) -> None:
        if self._min_interval_s <= 0:
            return
        now = float(self._now())
        remaining = self._next_allowed_at - now
        if remaining > 0:
            jitter = random.random() * self._jitter_s if self._jitter_s else 0.0
            self._sleep(remaining + jitter)
            now = float(self._now())
        self._next_allowed_at = now + self._min_interval_s


# `TDXCredentials` holds the client id/secret for the OAuth client-credentials flow.
@dataclass(frozen=True)
class TDXCredentials:
    # `client_id` identifies the application registered with TDX.
    client_id: str
    # `client_secret` is sensitive; it must come from env/.env and must never be committed to git.
    client_secret: str

    @staticmethod
    def from_env() -> "TDXCredentials":
        # Import locally to keep module import lightweight and avoid exporting `os` from this module.
        import os

        # Read credentials from environment variables (production practice: separate config from code).
        client_id = os.getenv("TDX_CLIENT_ID")
        client_secret = os.getenv("TDX_CLIENT_SECRET")
        # Fail fast with a clear message so users immediately know what to configure.
        if not client_id or not client_secret:
            raise ValueError("Missing env vars: TDX_CLIENT_ID and TDX_CLIENT_SECRET")
        # Return an immutable credentials object so callers cannot accidentally mutate secrets in memory.
        return TDXCredentials(client_id=client_id, client_secret=client_secret)


# `_Token` represents a cached OAuth access token and when it expires.
@dataclass
class _Token:
    # Raw bearer token string returned by the token endpoint.
    access_token: str
    # UTC timestamp after which we must refresh the token.
    expires_at: datetime

    def is_expired(self, now: datetime, *, skew_seconds: int = 60) -> bool:
        # We refresh slightly early (skew) to avoid edge cases where a token expires mid-request.
        # This is a common production pattern to reduce intermittent 401s.
        return now >= (self.expires_at - timedelta(seconds=skew_seconds))


# `TDXClient` is a minimal, production-minded HTTP client for TDX APIs.
class TDXClient:
    """
    Minimal production-minded TDX client.

    - Token is stored on the instance (no globals).
    - Retries are handled via a requests adapter for transient failures.
    """

    def __init__(
        self,
        *,
        base_url: str,
        token_url: str,
        credentials: TDXCredentials,
        timeout_s: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        min_request_interval_s: float = 0.0,
        request_jitter_s: float = 0.0,
        user_agent: str = "metrobikeatlas/0.1.0",
    ) -> None:
        # Normalize `base_url` so later path joins are consistent (avoid double slashes).
        self._base_url = base_url.rstrip("/")
        # Token endpoint is usually separate from the base API host.
        self._token_url = token_url
        # Credentials are injected so this client can be constructed in tests without reading env vars.
        self._credentials = credentials
        # A single timeout value keeps behavior predictable across scripts and avoids hanging requests.
        self._timeout_s = timeout_s
        # Token starts empty; it will be fetched lazily on the first request.
        self._token: Optional[_Token] = None

        # Client-side throttle reduces the chance we hit rate limits during paging/bursty runs.
        self._rate_limiter = _RateLimiter(
            min_interval_s=min_request_interval_s,
            jitter_s=request_jitter_s,
        )

        # A `Session` reuses connections (keep-alive) which is both faster and friendlier to the API.
        self._session = requests.Session()
        # A stable User-Agent helps operators identify traffic and debug API-side logs.
        self._session.headers.update({"User-Agent": user_agent})

        # Configure retries for transient failures (rate limiting and 5xx) using urllib3's policy.
        retry = Retry(
            # `total` caps how many retries happen overall for a request.
            total=max_retries,
            # Split out per-phase retry limits to match `total` (connect/read/status are common failure modes).
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            # Backoff grows delays between retries (e.g., 0.5s, 1s, 2s...) to reduce pressure on the server.
            backoff_factor=backoff_factor,
            # Retry only on status codes that are likely transient or rate-limit related.
            status_forcelist=(429, 500, 502, 503, 504),
            # Limit retries to idempotent-ish methods we use here; avoid retrying unsafe methods implicitly.
            allowed_methods=("GET", "POST"),
            # Respect `Retry-After` for 429/503 when present (common for rate limiting).
            respect_retry_after_header=True,
            # Do not raise inside urllib3; we want to surface a single `TDXRequestError` with context.
            raise_on_status=False,
        )
        # Cap backoff so a single request doesn't stall for an unbounded amount of time.
        # Note: older urllib3 versions may not accept `backoff_max` in the constructor.
        try:
            retry.backoff_max = 60  # type: ignore[attr-defined]
        except Exception:
            pass
        # Mount the retry policy for both HTTPS and HTTP in case environments differ.
        self._session.mount("https://", HTTPAdapter(max_retries=retry))
        self._session.mount("http://", HTTPAdapter(max_retries=retry))

    def _now_utc(self) -> datetime:
        # Always use UTC to avoid timezone bugs (DST, local machine settings, etc.).
        return datetime.now(timezone.utc)

    def _fetch_token(self) -> _Token:
        # TDX uses an OAuth2 client-credentials flow, which is a server-to-server authentication method.
        # We send the client id/secret to the token endpoint and receive a short-lived bearer token.
        payload = {
            "grant_type": "client_credentials",
            "client_id": self._credentials.client_id,
            "client_secret": self._credentials.client_secret,
        }
        # Use the shared session so retries/backoff apply and connections are reused.
        resp = self._session.post(self._token_url, data=payload, timeout=self._timeout_s)
        # Convert non-2xx token responses into a typed exception for clearer error handling upstream.
        if resp.status_code >= 400:
            raise TDXAuthError(f"TDX token request failed ({resp.status_code}): {resp.text[:500]}")

        # Parse JSON; if the response is not JSON, `resp.json()` will raise, which is acceptable for MVP.
        data = resp.json()
        # Extract required token fields; TDX typically provides `access_token` and `expires_in` seconds.
        access_token = data.get("access_token")
        expires_in = data.get("expires_in")
        # Fail fast if the response shape is unexpected (prevents returning a half-broken token).
        if not access_token or not expires_in:
            raise TDXAuthError(f"TDX token response missing fields: {json.dumps(data)[:500]}")

        # Convert `expires_in` into an absolute UTC timestamp for stable comparisons.
        expires_at = self._now_utc() + timedelta(seconds=int(expires_in))
        # Return a token object so we can cache it on the client instance.
        return _Token(access_token=access_token, expires_at=expires_at)

    def _get_token(self) -> str:
        # Compute "now" once to keep comparisons consistent (and avoid calling the clock multiple times).
        now = self._now_utc()
        # Refresh token lazily when missing or expired; this keeps startup fast and avoids unused token calls.
        if self._token is None or self._token.is_expired(now):
            self._token = self._fetch_token()
        # Return the bearer token string for Authorization headers.
        return self._token.access_token

    def _build_url(self, path: str) -> str:
        # Support absolute URLs (used for `@odata.nextLink` paging).
        if path.startswith("https://") or path.startswith("http://"):
            return path
        # Ensure callers can pass either "/path" or "path" without creating a double slash.
        path = path.lstrip("/")
        # Join base URL and path in a predictable way.
        return f"{self._base_url}/{path}"

    def get_json(
        self,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        # Throttle before making the request (helps avoid 429s, especially when paging).
        self._rate_limiter.wait()
        # Build the full URL early so we can include it in error messages.
        url = self._build_url(path)
        # Start from required headers: Authorization (bearer token) and Accept (JSON response expected).
        req_headers: MutableMapping[str, str] = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }
        # Merge optional headers so callers can pass TDX-specific knobs without modifying this client.
        if headers:
            req_headers.update(headers)

        # Perform the GET request; retry/backoff is handled by the mounted adapter.
        resp = self._session.get(url, params=params, headers=req_headers, timeout=self._timeout_s)
        # 401 often means an expired/revoked token; we clear token and retry once with a fresh one.
        if resp.status_code == 401:
            # Reset cached token so `_get_token()` will fetch a new one.
            self._token = None
            # Rebuild Authorization header with the new token.
            req_headers["Authorization"] = f"Bearer {self._get_token()}"
            # Retry the exact same request once; if it still fails, we surface an error to the caller.
            resp = self._session.get(url, params=params, headers=req_headers, timeout=self._timeout_s)

        # Treat any 4xx/5xx as an error; callers can handle retries outside if needed.
        if resp.status_code >= 400:
            if resp.status_code == 429:
                retry_after_s: float | None = None
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        retry_after_s = float(ra)
                    except Exception:
                        retry_after_s = None
                raise TDXRateLimitError(
                    f"TDX request failed ({resp.status_code}) url={url} params={params} retry_after={ra} body={resp.text[:500]}",
                    retry_after_s=retry_after_s,
                )
            raise TDXRequestError(
                f"TDX request failed ({resp.status_code}) url={url} params={params} body={resp.text[:500]}"
            )
        # Return parsed JSON; downstream code will map dicts into typed schema objects (Silver layer).
        return resp.json()

    def get_json_all(
        self,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        max_pages: int = 100,
    ) -> list[Any]:
        """
        Fetch all pages for endpoints that return OData-style responses.

        Supported shapes:
        - JSON list: returns as-is.
        - OData dict: {"value": [...], "@odata.nextLink": "..."}: follows nextLink until exhausted.

        This keeps Bronze payloads stable as a list, which simplifies Silver building.
        """

        if max_pages < 1:
            raise ValueError("max_pages must be >= 1")

        out: list[Any] = []
        next_path: Optional[str] = path
        next_params: Optional[Mapping[str, Any]] = params

        page = 0
        while next_path is not None:
            page += 1
            if page > max_pages:
                raise TDXRequestError(f"Exceeded max_pages={max_pages} for path={path}")

            data = self.get_json(next_path, params=next_params, headers=headers)
            next_params = None  # nextLink already includes query params

            if isinstance(data, list):
                out.extend(data)
                break
            if isinstance(data, Mapping):
                value = data.get("value")
                if isinstance(value, list):
                    out.extend(value)
                else:
                    raise TDXRequestError(f"Unexpected TDX response shape for path={path}: {json.dumps(data)[:200]}")

                next_link = data.get("@odata.nextLink") or data.get("odata.nextLink")
                if isinstance(next_link, str) and next_link.strip():
                    next_path = next_link
                    continue
                next_path = None
                break

            raise TDXRequestError(f"Unexpected TDX response type for path={path}: {type(data).__name__}")

        if page > 1:
            logger.info("Fetched %s pages (%s records) for %s", page, len(out), path)
        return out

    def close(self) -> None:
        # Close network resources; important for long-running scripts to avoid open connections.
        self._session.close()

    def __enter__(self) -> "TDXClient":
        # Support `with TDXClient(...) as tdx:` so scripts reliably close sessions.
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Always close the session when leaving the context manager, even on exceptions.
        self.close()
