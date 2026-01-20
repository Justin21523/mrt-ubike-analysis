from __future__ import annotations

from fastapi.testclient import TestClient

from metrobikeatlas.api.app import create_app
from metrobikeatlas.config.loader import load_config


def test_status_endpoint_returns_payload(monkeypatch) -> None:
    # Keep this test deterministic even if the developer environment has demo mode disabled.
    monkeypatch.setenv("METROBIKEATLAS_DEMO_MODE", "true")
    config = load_config()
    app = create_app(config)
    client = TestClient(app)

    resp = client.get("/status")
    assert resp.status_code == 200
    payload = resp.json()

    assert "now_utc" in payload
    assert "demo_mode" in payload
    assert "bronze_dir" in payload
    assert "silver_dir" in payload
    assert isinstance(payload.get("silver_tables"), list)
    assert isinstance(payload.get("bronze_datasets"), list)
    assert isinstance(payload.get("alerts"), list)


def test_admin_endpoints_block_non_localhost(monkeypatch) -> None:
    monkeypatch.setenv("METROBIKEATLAS_DEMO_MODE", "true")
    app = create_app(load_config())
    client = TestClient(app)

    # TestClient host is not 127.0.0.1/::1, so admin should be blocked.
    resp = client.post("/admin/collector/start", json={})
    assert resp.status_code == 403

    resp = client.post("/admin/build_silver_async", json={})
    assert resp.status_code == 403

    resp = client.post("/admin/collector/restart_if_stale", json={})
    assert resp.status_code == 403

    resp = client.post("/admin/weather/refresh", json={})
    assert resp.status_code == 403

    resp = client.get("/admin/jobs")
    assert resp.status_code == 403

    resp = client.get("/admin/jobs/doesnotexist/events")
    assert resp.status_code == 403

    resp = client.get("/external/metro_stations/validate")
    assert resp.status_code == 403

    resp = client.get("/external/metro_stations/preview")
    assert resp.status_code == 403

    resp = client.get("/external/metro_stations/download")
    assert resp.status_code == 403

    resp = client.post("/briefing/snapshots", json={})
    assert resp.status_code == 403

    resp = client.get("/briefing/snapshots")
    assert resp.status_code == 403


def test_timeseries_includes_meta(monkeypatch) -> None:
    monkeypatch.setenv("METROBIKEATLAS_DEMO_MODE", "true")
    app = create_app(load_config())
    client = TestClient(app)

    stations = client.get("/stations").json()
    assert stations
    station_id = stations[0]["id"]
    resp = client.get(f"/station/{station_id}/timeseries")
    assert resp.status_code == 200
    payload = resp.json()
    assert "meta" in payload
    assert payload["meta"]["endpoint"] == "timeseries"
    assert payload["meta"]["station_id"] == station_id


def test_heat_endpoints_work_in_demo(monkeypatch) -> None:
    monkeypatch.setenv("METROBIKEATLAS_DEMO_MODE", "true")
    app = create_app(load_config())
    client = TestClient(app)

    idx = client.get("/stations/heat_index?limit=5").json()
    ts = idx["timestamps"][-1]
    rows = client.get(f"/stations/heat_at?ts={ts}&metric=available&agg=sum").json()
    assert isinstance(rows, list)
    if rows:
        row = rows[0]
        assert row["metric"] == "available"
        assert row["agg"] == "sum"
        assert "value" in row


def test_meta_endpoint_works(monkeypatch) -> None:
    monkeypatch.setenv("METROBIKEATLAS_DEMO_MODE", "true")
    app = create_app(load_config())
    client = TestClient(app)
    resp = client.get("/meta")
    assert resp.status_code == 200
    payload = resp.json()
    assert "now_utc" in payload
    assert "demo_mode" in payload
    assert "meta" in payload


def test_replay_endpoint_works(monkeypatch) -> None:
    monkeypatch.setenv("METROBIKEATLAS_DEMO_MODE", "true")
    app = create_app(load_config())
    client = TestClient(app)
    stations = client.get("/stations").json()
    station_id = stations[0]["id"]
    resp = client.get(f"/replay?station_id={station_id}")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["station_id"] == station_id
    assert "meta" in payload
