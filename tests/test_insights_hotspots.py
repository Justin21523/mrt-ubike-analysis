from __future__ import annotations

from fastapi.testclient import TestClient

from metrobikeatlas.api.app import create_app
from metrobikeatlas.config.loader import load_config


def test_hotspots_endpoint_works_in_demo(monkeypatch) -> None:
    monkeypatch.setenv("METROBIKEATLAS_DEMO_MODE", "true")
    app = create_app(load_config())
    client = TestClient(app)

    resp = client.get("/insights/hotspots?metric=available&agg=sum&top_k=3")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["metric"] == "available"
    assert payload["agg"] == "sum"
    assert isinstance(payload["hot"], list)
    assert isinstance(payload["cold"], list)
