from __future__ import annotations

from pathlib import Path

import pandas as pd

from metrobikeatlas.config.models import (
    AnalyticsSettings,
    AppConfig,
    AppSettings,
    CacheSettings,
    ClusteringSettings,
    FeatureSettings,
    LoggingSettings,
    POISettings,
    SimilaritySettings,
    SpatialSettings,
    TDXBikeSettings,
    TDXMetroSettings,
    TDXSettings,
    TemporalSettings,
    WebMapSettings,
    WebSettings,
)
from metrobikeatlas.repository.local import LocalRepository


def _test_config() -> AppConfig:
    return AppConfig(
        app=AppSettings(name="Test", demo_mode=False),
        tdx=TDXSettings(
            base_url="https://example.invalid",
            token_url="https://example.invalid/token",
            metro=TDXMetroSettings(cities=["Taipei"], stations_path_template="", ridership_path_template=None),
            bike=TDXBikeSettings(
                cities=["Taipei"],
                stations_path_template="",
                availability_path_template="",
            ),
        ),
        temporal=TemporalSettings(timezone="UTC", granularity="hour"),
        spatial=SpatialSettings(join_method="buffer", radius_m=500, nearest_k=3),
        cache=CacheSettings(dir=Path("data/cache"), ttl_seconds=0),
        features=FeatureSettings(
            station_features_path=Path("data/gold/station_features.csv"),
            station_targets_path=Path("data/gold/station_targets.csv"),
            timeseries_window_days=7,
            admin_boundaries_geojson_path=Path("data/external/admin_boundaries.geojson"),
            poi=POISettings(
                path=Path("data/external/poi.csv"),
                radii_m=[300, 500],
                categories=["food"],
            ),
            station_district_map_path=Path("data/external/metro_station_district.csv"),
        ),
        analytics=AnalyticsSettings(
            similarity=SimilaritySettings(top_k=5, metric="euclidean", standardize=True),
            clustering=ClusteringSettings(k=3, standardize=True),
        ),
        logging=LoggingSettings(level="INFO", format="%(message)s"),
        web=WebSettings(static_dir=Path("web"), map=WebMapSettings(center_lat=0, center_lon=0, zoom=1)),
    )


def _write_required_silver(silver_dir: Path) -> None:
    pd.DataFrame(
        [
            {"station_id": "M1", "name": "Metro 1", "lat": 25.0, "lon": 121.0, "city": "Taipei", "system": "TRTC"}
        ]
    ).to_csv(silver_dir / "metro_stations.csv", index=False)

    pd.DataFrame(
        [
            {"station_id": "B1", "name": "Bike 1", "lat": 25.0005, "lon": 121.0, "city": "Taipei", "capacity": 40},
            {"station_id": "B2", "name": "Bike 2", "lat": 25.0007, "lon": 121.0003, "city": "Taipei", "capacity": 32},
        ]
    ).to_csv(silver_dir / "bike_stations.csv", index=False)

    pd.DataFrame(
        [
            {"metro_station_id": "M1", "bike_station_id": "B1", "distance_m": 80.0},
            {"metro_station_id": "M1", "bike_station_id": "B2", "distance_m": 120.0},
        ]
    ).to_csv(silver_dir / "metro_bike_links.csv", index=False)

    # Two snapshots per bike station in the same hour
    pd.DataFrame(
        [
            {"station_id": "B1", "ts": "2026-01-01T00:10:00+00:00", "available_bikes": 5},
            {"station_id": "B1", "ts": "2026-01-01T00:20:00+00:00", "available_bikes": 4},
            {"station_id": "B2", "ts": "2026-01-01T00:10:00+00:00", "available_bikes": 10},
            {"station_id": "B2", "ts": "2026-01-01T00:30:00+00:00", "available_bikes": 8},
        ]
    ).to_csv(silver_dir / "bike_timeseries.csv", index=False)


def test_station_timeseries_uses_bike_proxy_when_metro_missing(tmp_path: Path) -> None:
    config = _test_config()
    _write_required_silver(tmp_path)

    repo = LocalRepository(config, silver_dir=tmp_path)
    ts = repo.station_timeseries("M1")

    metro = next(s for s in ts["series"] if s["metric"].startswith("metro"))
    bike = next(s for s in ts["series"] if s["metric"].startswith("bike"))

    assert metro["is_proxy"] is True
    assert metro["metric"] == "metro_flow_proxy_from_bike_rent"
    assert bike["metric"] == "bike_available_bikes_total"

    # Availability uses mean within the hour per station, then sums across stations:
    # B1 mean = (5 + 4) / 2 = 4.5, B2 mean = (10 + 8) / 2 = 9.0, total = 13.5
    assert bike["points"][0]["value"] == 13.5

    # Rent proxy is derived from negative deltas in available bikes:
    # B1: 5 -> 4 => 1 rent, B2: 10 -> 8 => 2 rents, total = 3
    assert metro["points"][0]["value"] == 3.0


def test_station_timeseries_prefers_metro_series_when_available(tmp_path: Path) -> None:
    config = _test_config()
    _write_required_silver(tmp_path)

    pd.DataFrame(
        [
            {"station_id": "M1", "ts": "2026-01-01T00:00:00+00:00", "value": 1234},
        ]
    ).to_csv(tmp_path / "metro_timeseries.csv", index=False)

    repo = LocalRepository(config, silver_dir=tmp_path)
    ts = repo.station_timeseries("M1")

    metro = next(s for s in ts["series"] if s["metric"].startswith("metro"))
    assert metro["is_proxy"] is False
    assert metro["metric"] == "metro_ridership"
    assert metro["points"][0]["value"] == 1234.0
