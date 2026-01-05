from __future__ import annotations

from dataclasses import asdict
from typing import Iterable

import pandas as pd

from metrobikeatlas.config.models import SpatialSettings
from metrobikeatlas.schemas.core import StationBikeLink
from metrobikeatlas.utils.geo import haversine_m


def build_station_bike_links(
    metro_stations: pd.DataFrame,
    bike_stations: pd.DataFrame,
    *,
    metro_id_col: str = "station_id",
    bike_id_col: str = "station_id",
    lat_col: str = "lat",
    lon_col: str = "lon",
    settings: SpatialSettings,
) -> pd.DataFrame:
    """
    Create a metro_station_id -> bike_station_id mapping using either:
    - buffer: all bike stations within `radius_m`
    - nearest: k nearest bike stations

    MVP note: this uses a simple haversine loop (no spatial index). Optimize later if needed.
    """

    links: list[StationBikeLink] = []
    for _, metro in metro_stations.iterrows():
        metro_id = str(metro[metro_id_col])
        metro_lat = float(metro[lat_col])
        metro_lon = float(metro[lon_col])

        distances: list[tuple[str, float]] = []
        for _, bike in bike_stations.iterrows():
            bike_id = str(bike[bike_id_col])
            d = haversine_m(metro_lat, metro_lon, float(bike[lat_col]), float(bike[lon_col]))
            distances.append((bike_id, d))

        if settings.join_method == "buffer":
            selected = [(bid, d) for bid, d in distances if d <= settings.radius_m]
        else:
            selected = sorted(distances, key=lambda x: x[1])[: max(settings.nearest_k, 1)]

        for bike_id, d in selected:
            links.append(
                StationBikeLink(
                    metro_station_id=metro_id, bike_station_id=bike_id, distance_m=float(d)
                )
            )

    return pd.DataFrame([asdict(link) for link in links])


def filter_links_for_station(
    links: pd.DataFrame, *, metro_station_id: str, metro_id_col: str = "metro_station_id"
) -> pd.DataFrame:
    return links[links[metro_id_col] == metro_station_id].copy()

