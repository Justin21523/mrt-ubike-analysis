from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Mapping, MutableMapping, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class TDXAuthError(RuntimeError):
    pass


class TDXRequestError(RuntimeError):
    pass


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


@dataclass
class _Token:
    access_token: str
    expires_at: datetime

    def is_expired(self, now: datetime, *, skew_seconds: int = 60) -> bool:
        return now >= (self.expires_at - timedelta(seconds=skew_seconds))


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
        user_agent: str = "metrobikeatlas/0.1.0",
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token_url = token_url
        self._credentials = credentials
        self._timeout_s = timeout_s
        self._token: Optional[_Token] = None

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

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

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

    def _get_token(self) -> str:
        now = self._now_utc()
        if self._token is None or self._token.is_expired(now):
            self._token = self._fetch_token()
        return self._token.access_token

    def _build_url(self, path: str) -> str:
        path = path.lstrip("/")
        return f"{self._base_url}/{path}"

    def get_json(
        self,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
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
            raise TDXRequestError(
                f"TDX request failed ({resp.status_code}) url={url} params={params} body={resp.text[:500]}"
            )
        return resp.json()

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "TDXClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

