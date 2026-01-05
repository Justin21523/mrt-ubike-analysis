from __future__ import annotations

import pandas as pd

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.features.builder import build_station_targets


def test_station_targets_include_metro_ridership_when_provided() -> None:
    config = load_config()

    links = pd.DataFrame(
        [
            {"metro_station_id": "M1", "bike_station_id": "B1", "distance_m": 10.0},
        ]
    )
    bike_ts = pd.DataFrame(
        [
            {"station_id": "B1", "ts": "2026-01-01T00:00:00+00:00", "available_bikes": 5},
            {"station_id": "B1", "ts": "2026-01-01T00:10:00+00:00", "available_bikes": 4},
        ]
    )
    metro_ts = pd.DataFrame(
        [
            {"station_id": "M1", "ts": "2026-01-01T00:00:00+00:00", "value": 100},
            {"station_id": "M1", "ts": "2026-01-01T01:00:00+00:00", "value": 200},
        ]
    )

    targets = build_station_targets(config=config, bike_timeseries=bike_ts, links=links, metro_timeseries=metro_ts)
    metrics = set(targets["metric"].tolist())

    assert "metro_flow_proxy_from_bike_rent" in metrics
    assert "metro_ridership" in metrics

