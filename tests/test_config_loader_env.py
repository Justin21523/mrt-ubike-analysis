from __future__ import annotations

import json

from metrobikeatlas.config.loader import load_config


def _write_config(tmp_path, *, demo_mode: bool = True) -> str:
    cfg = {
        "app": {"name": "Test", "demo_mode": demo_mode},
        "tdx": {
            "base_url": "https://json.base",
            "token_url": "https://json.token",
            "metro": {"cities": ["Taipei"], "stations_path_template": "x", "ridership_path_template": None},
            "bike": {"cities": ["Taipei"], "stations_path_template": "y", "availability_path_template": "z"},
        },
        "temporal": {"timezone": "Asia/Taipei", "granularity": "hour"},
        "spatial": {"join_method": "buffer", "radius_m": 500, "nearest_k": 3},
        "cache": {"dir": "data/cache", "ttl_seconds": 3600},
        "features": {
            "station_features_path": "data/gold/station_features.csv",
            "station_targets_path": "data/gold/station_targets.csv",
            "timeseries_window_days": 7,
            "admin_boundaries_geojson_path": None,
            "time_patterns": {
                "peak_am_start_hour": 7,
                "peak_am_end_hour": 10,
                "peak_pm_start_hour": 17,
                "peak_pm_end_hour": 20,
            },
            "accessibility": {"bike": {"w_station_count": 1.0, "w_capacity_sum": 0.02, "w_distance_mean_m": -0.005}},
            "poi": None,
            "station_district_map_path": None,
        },
        "analytics": {"similarity": {"top_k": 5, "metric": "euclidean", "standardize": True}, "clustering": {"k": 3, "standardize": True}},
        "logging": {"level": "INFO", "format": "%(message)s"},
        "web": {"static_dir": "web", "map": {"center_lat": 0, "center_lon": 0, "zoom": 1}},
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    return str(path)


def test_env_overrides_tdx_urls(monkeypatch, tmp_path) -> None:
    path = _write_config(tmp_path)
    monkeypatch.setenv("TDX_BASE_URL", "https://env.base")
    monkeypatch.setenv("TDX_TOKEN_URL", "https://env.token")

    cfg = load_config(path, base_dir=tmp_path)
    assert cfg.tdx.base_url == "https://env.base"
    assert cfg.tdx.token_url == "https://env.token"


def test_env_overrides_demo_mode(monkeypatch, tmp_path) -> None:
    path = _write_config(tmp_path, demo_mode=True)
    monkeypatch.setenv("METROBIKEATLAS_DEMO_MODE", "false")

    cfg = load_config(path, base_dir=tmp_path)
    assert cfg.app.demo_mode is False


def test_invalid_demo_mode_does_not_override(monkeypatch, tmp_path) -> None:
    path = _write_config(tmp_path, demo_mode=False)
    monkeypatch.setenv("METROBIKEATLAS_DEMO_MODE", "maybe")

    cfg = load_config(path, base_dir=tmp_path)
    assert cfg.app.demo_mode is False

