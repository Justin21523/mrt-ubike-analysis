from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import math
import random
from typing import Any

from metrobikeatlas.config.models import AppConfig
from metrobikeatlas.schemas.core import BikeStation, MetroStation, StationBikeLink, TimeSeriesPoint
from metrobikeatlas.utils.geo import haversine_m


class DemoRepository:
    """
    Deterministic in-memory dataset so the MVP web UI works without credentials.
    """

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._tz = ZoneInfo(config.temporal.timezone)

        self._metro = [
            MetroStation(
                station_id="MRT_TAIPEI_MAIN",
                name="Taipei Main Station",
                lat=25.0478,
                lon=121.5170,
                city="Taipei",
                system="TRTC",
            ),
            MetroStation(
                station_id="MRT_ZHONGXIAO_FUXING",
                name="Zhongxiao Fuxing",
                lat=25.0413,
                lon=121.5445,
                city="Taipei",
                system="TRTC",
            ),
            MetroStation(
                station_id="MRT_CITY_HALL",
                name="Taipei City Hall",
                lat=25.0403,
                lon=121.5672,
                city="Taipei",
                system="TRTC",
            ),
        ]

        self._bike = [
            BikeStation("BIKE_0001", "Bike Station A", 25.0484, 121.5155, "Taipei", "YouBike", 40),
            BikeStation("BIKE_0002", "Bike Station B", 25.0462, 121.5204, "Taipei", "YouBike", 32),
            BikeStation("BIKE_0003", "Bike Station C", 25.0418, 121.5430, "Taipei", "YouBike", 28),
            BikeStation("BIKE_0004", "Bike Station D", 25.0399, 121.5682, "Taipei", "YouBike", 36),
            BikeStation("BIKE_0005", "Bike Station E", 25.0410, 121.5654, "Taipei", "YouBike", 24),
        ]

        self._links = self._build_links()
        self._series = self._build_timeseries()

    def list_metro_stations(self) -> list[dict[str, Any]]:
        return [asdict(s) for s in self._metro]

    def list_bike_stations(self) -> list[dict[str, Any]]:
        return [asdict(s) for s in self._bike]

    def nearby_bike(self, metro_station_id: str) -> list[dict[str, Any]]:
        bike_by_id = {b.station_id: b for b in self._bike}
        out = []
        for link in self._links:
            if link.metro_station_id != metro_station_id:
                continue
            bike = bike_by_id[link.bike_station_id]
            out.append({**asdict(bike), "distance_m": link.distance_m})
        return sorted(out, key=lambda x: x["distance_m"])

    def station_timeseries(self, metro_station_id: str) -> dict[str, Any]:
        if metro_station_id not in self._series:
            raise KeyError(metro_station_id)

        return {
            "station_id": metro_station_id,
            "granularity": self._config.temporal.granularity,
            "timezone": self._config.temporal.timezone,
            "series": [
                {
                    "metric": "metro_ridership_proxy",
                    "points": [asdict(p) for p in self._series[metro_station_id]["metro"]],
                },
                {
                    "metric": "bike_available_bikes",
                    "points": [asdict(p) for p in self._series[metro_station_id]["bike_available"]],
                },
            ],
        }

    def _build_links(self) -> list[StationBikeLink]:
        links: list[StationBikeLink] = []
        for metro in self._metro:
            for bike in self._bike:
                d = haversine_m(metro.lat, metro.lon, bike.lat, bike.lon)
                if d <= self._config.spatial.radius_m:
                    links.append(
                        StationBikeLink(
                            metro_station_id=metro.station_id,
                            bike_station_id=bike.station_id,
                            distance_m=d,
                        )
                    )
        return links

    def _build_timeseries(self) -> dict[str, dict[str, list[TimeSeriesPoint]]]:
        freq_minutes = {"15min": 15, "hour": 60, "day": 1440}[self._config.temporal.granularity]
        end = datetime.now(self._tz).replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(days=7)
        steps = int((end - start).total_seconds() // (freq_minutes * 60))

        output: dict[str, dict[str, list[TimeSeriesPoint]]] = {}
        for metro in self._metro:
            rng = random.Random(metro.station_id)
            metro_points: list[TimeSeriesPoint] = []
            bike_points: list[TimeSeriesPoint] = []

            base_metro = rng.uniform(5000, 15000)
            amp_metro = rng.uniform(1500, 4000)
            base_bike = rng.uniform(30, 120)
            amp_bike = rng.uniform(10, 30)

            for i in range(steps + 1):
                ts = start + timedelta(minutes=freq_minutes * i)
                hour = ts.hour + ts.minute / 60.0
                daily = math.sin(2 * math.pi * (hour / 24.0))
                weekend = 0.85 if ts.weekday() >= 5 else 1.0

                metro_value = max(0.0, (base_metro + amp_metro * daily) * weekend)
                bike_value = max(0.0, base_bike - amp_bike * daily + rng.uniform(-3, 3))

                metro_points.append(TimeSeriesPoint(ts=ts, value=float(metro_value)))
                bike_points.append(TimeSeriesPoint(ts=ts, value=float(bike_value)))

            output[metro.station_id] = {"metro": metro_points, "bike_available": bike_points}
        return output

