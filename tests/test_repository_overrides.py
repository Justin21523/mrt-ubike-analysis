from __future__ import annotations

from pathlib import Path

import pandas as pd

from metrobikeatlas.config.models import (
    AccessibilitySettings,
    AnalyticsSettings,
    AppConfig,
    AppSettings,
    BikeAccessibilityWeights,
    CacheSettings,
    ClusteringSettings,
    FeatureSettings,
    FeatureTimePatternSettings,
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
            time_patterns=FeatureTimePatternSettings(
                peak_am_start_hour=7,
                peak_am_end_hour=10,
                peak_pm_start_hour=17,
                peak_pm_end_hour=20,
            ),
            accessibility=AccessibilitySettings(
                bike=BikeAccessibilityWeights(
                    w_station_count=1.0, w_capacity_sum=0.02, w_distance_mean_m=-0.005, bias=0.0
                )
            ),
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
            {"station_id": "B1", "name": "Bike 1", "lat": 0.0, "lon": 0.0, "city": "Taipei", "capacity": 40},
        ]
    ).to_csv(silver_dir / "bike_stations.csv", index=False)

    pd.DataFrame(
        [
            {"metro_station_id": "M1", "bike_station_id": "B1", "distance_m": 999999.0},
        ]
    ).to_csv(silver_dir / "metro_bike_links.csv", index=False)

    pd.DataFrame(
        columns=["station_id", "ts", "available_bikes"],
    ).to_csv(silver_dir / "bike_timeseries.csv", index=False)


def test_nearby_bike_returns_empty_when_no_bikes_match(tmp_path: Path) -> None:
    config = _test_config()
    _write_required_silver(tmp_path)

    repo = LocalRepository(config, silver_dir=tmp_path)
    nearby = repo.nearby_bike("M1", radius_m=50)
    assert nearby == []


def test_station_timeseries_accepts_granularity_override(tmp_path: Path) -> None:
    config = _test_config()
    _write_required_silver(tmp_path)

    repo = LocalRepository(config, silver_dir=tmp_path)
    ts = repo.station_timeseries("M1", granularity="day")
    assert ts["granularity"] == "day"

