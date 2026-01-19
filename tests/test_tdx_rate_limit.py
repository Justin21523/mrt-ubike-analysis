from __future__ import annotations

from typing import Any

import pytest

from metrobikeatlas.ingestion.tdx_base import TDXClient, TDXCredentials, TDXRequestError, _RateLimiter


def test_rate_limiter_sleeps_to_enforce_min_interval() -> None:
    now = 0.0
    slept: list[float] = []

    def now_fn() -> float:
        return now

    def sleep_fn(seconds: float) -> None:
        slept.append(seconds)

    limiter = _RateLimiter(min_interval_s=1.0, jitter_s=0.0, now_fn=now_fn, sleep_fn=sleep_fn)

    limiter.wait()
    assert slept == []

    limiter.wait()
    assert slept == [1.0]


def test_get_json_all_returns_list_unchanged() -> None:
    client = TDXClient(
        base_url="https://example.com",
        token_url="https://example.com/token",
        credentials=TDXCredentials(client_id="x", client_secret="y"),
    )

    calls: list[tuple[str, Any]] = []

    def fake_get_json(path: str, *, params=None, headers=None):  # type: ignore[no-untyped-def]
        calls.append((path, params))
        return [{"id": 1}, {"id": 2}]

    client.get_json = fake_get_json  # type: ignore[method-assign]
    out = client.get_json_all("Bike/Station/City/Taipei", params={"$format": "JSON"})
    assert out == [{"id": 1}, {"id": 2}]
    assert calls == [("Bike/Station/City/Taipei", {"$format": "JSON"})]


def test_get_json_all_follows_odata_nextlink() -> None:
    client = TDXClient(
        base_url="https://example.com",
        token_url="https://example.com/token",
        credentials=TDXCredentials(client_id="x", client_secret="y"),
    )

    calls: list[str] = []
    responses = [
        {"value": [1, 2], "@odata.nextLink": "https://example.com/next?page=2"},
        {"value": [3]},
    ]

    def fake_get_json(path: str, *, params=None, headers=None):  # type: ignore[no-untyped-def]
        calls.append(path)
        return responses.pop(0)

    client.get_json = fake_get_json  # type: ignore[method-assign]
    out = client.get_json_all("Rail/Metro/Station/City/Taipei", params={"$format": "JSON"}, max_pages=10)
    assert out == [1, 2, 3]
    assert calls == ["Rail/Metro/Station/City/Taipei", "https://example.com/next?page=2"]


def test_get_json_all_enforces_max_pages() -> None:
    client = TDXClient(
        base_url="https://example.com",
        token_url="https://example.com/token",
        credentials=TDXCredentials(client_id="x", client_secret="y"),
    )

    def fake_get_json(path: str, *, params=None, headers=None):  # type: ignore[no-untyped-def]
        return {"value": [1], "@odata.nextLink": "https://example.com/next"}

    client.get_json = fake_get_json  # type: ignore[method-assign]
    with pytest.raises(TDXRequestError):
        client.get_json_all("x", max_pages=2)
