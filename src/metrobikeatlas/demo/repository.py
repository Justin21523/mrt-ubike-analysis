from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import math
import random
from typing import Any, Literal, Optional

import pandas as pd

from metrobikeatlas.config.models import AppConfig
from metrobikeatlas.analytics.similarity import find_similar_stations
from metrobikeatlas.schemas.core import BikeStation, MetroStation, StationBikeLink, TimeSeriesPoint
from metrobikeatlas.utils.geo import haversine_m


SpatialJoinMethod = Literal["buffer", "nearest"]
Granularity = Literal["15min", "hour", "day"]
SimilarityMetric = Literal["euclidean", "cosine"]
MetroSeriesMode = Literal["auto", "ridership", "proxy"]


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
        self._station_features = self._build_station_features()

    def list_metro_stations(self) -> list[dict[str, Any]]:
        by_id = {}
        for _, row in self._station_features.iterrows():
            station_id = str(row["station_id"])
            by_id[station_id] = row.to_dict()
        out = []
        for idx, station in enumerate(self._metro):
            payload = asdict(station)
            meta = by_id.get(station.station_id, {})
            payload["district"] = meta.get("district")
            payload["cluster"] = idx
            out.append(payload)
        return out

    def list_bike_stations(self) -> list[dict[str, Any]]:
        return [asdict(s) for s in self._bike]

    def nearby_bike(
        self,
        metro_station_id: str,
        *,
        join_method: Optional[SpatialJoinMethod] = None,
        radius_m: Optional[float] = None,
        nearest_k: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        metro = next((m for m in self._metro if m.station_id == metro_station_id), None)
        if metro is None:
            raise KeyError(metro_station_id)

        method: SpatialJoinMethod = join_method or self._config.spatial.join_method
        radius = self._config.spatial.radius_m if radius_m is None else float(radius_m)
        k = self._config.spatial.nearest_k if nearest_k is None else int(nearest_k)

        rows = []
        for bike in self._bike:
            d = haversine_m(metro.lat, metro.lon, bike.lat, bike.lon)
            rows.append({**asdict(bike), "distance_m": float(d)})

        if method == "buffer":
            rows = [r for r in rows if r["distance_m"] <= float(radius)]
        else:
            rows = sorted(rows, key=lambda x: x["distance_m"])[: max(int(k), 1)]

        rows = sorted(rows, key=lambda x: x["distance_m"])
        if limit is not None:
            rows = rows[: max(int(limit), 0)]
        return rows

    def station_timeseries(
        self,
        metro_station_id: str,
        *,
        join_method: Optional[SpatialJoinMethod] = None,
        radius_m: Optional[float] = None,
        nearest_k: Optional[int] = None,
        granularity: Optional[Granularity] = None,
        timezone: Optional[str] = None,
        window_days: Optional[int] = None,
        metro_series: MetroSeriesMode = "auto",
    ) -> dict[str, Any]:
        metro = next((m for m in self._metro if m.station_id == metro_station_id), None)
        if metro is None:
            raise KeyError(metro_station_id)

        gran: Granularity = granularity or self._config.temporal.granularity
        tz = timezone or self._config.temporal.timezone
        if gran not in ("15min", "hour", "day"):
            raise ValueError(f"Unsupported granularity: {gran}")

        window_days_value = None if window_days is None else max(int(window_days), 1)

        # Derive a scale from how many bike stations are nearby under the current join settings.
        nearby = self.nearby_bike(
            metro_station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            limit=None,
        )
        scale = max(len(nearby), 1)

        freq_minutes = {"15min": 15, "hour": 60, "day": 1440}[gran]
        tzinfo = ZoneInfo(tz)
        end = datetime.now(tzinfo).replace(minute=0, second=0, microsecond=0)
        if gran == "15min":
            end = end.replace(minute=(end.minute // 15) * 15)
        start = end - timedelta(days=window_days_value or 7)
        steps = int((end - start).total_seconds() // (freq_minutes * 60))

        rng = random.Random(metro_station_id)
        base_metro = rng.uniform(5000, 15000)
        amp_metro = rng.uniform(1500, 4000)
        base_bike = rng.uniform(30, 120) * scale
        amp_bike = rng.uniform(10, 30) * scale

        metro_points: list[TimeSeriesPoint] = []
        bike_available: list[TimeSeriesPoint] = []
        for i in range(steps + 1):
            ts = start + timedelta(minutes=freq_minutes * i)
            hour = ts.hour + ts.minute / 60.0
            daily = math.sin(2 * math.pi * (hour / 24.0))
            weekend = 0.85 if ts.weekday() >= 5 else 1.0

            metro_value = max(0.0, (base_metro + amp_metro * daily) * weekend)
            bike_value = max(0.0, base_bike - amp_bike * daily + rng.uniform(-3, 3) * scale)

            metro_points.append(TimeSeriesPoint(ts=ts, value=float(metro_value)))
            bike_available.append(TimeSeriesPoint(ts=ts, value=float(bike_value)))

        bike_rent: list[TimeSeriesPoint] = []
        bike_return: list[TimeSeriesPoint] = []
        prev = None
        for p in bike_available:
            if prev is None:
                prev = p.value
                bike_rent.append(TimeSeriesPoint(ts=p.ts, value=0.0))
                bike_return.append(TimeSeriesPoint(ts=p.ts, value=0.0))
                continue
            delta = p.value - prev
            bike_rent.append(TimeSeriesPoint(ts=p.ts, value=float(max(-delta, 0.0))))
            bike_return.append(TimeSeriesPoint(ts=p.ts, value=float(max(delta, 0.0))))
            prev = p.value

        metro_ridership_points = [asdict(p) for p in metro_points]
        metro_proxy_points = [asdict(p) for p in bike_rent]

        series: list[dict[str, Any]] = []
        if metro_series in ("auto", "ridership"):
            series.append(
                {
                    "metric": "metro_ridership",
                    "points": metro_ridership_points,
                    "source": "demo",
                    "is_proxy": True,
                }
            )
        series.append(
            {
                "metric": "metro_flow_proxy_from_bike_rent",
                "points": metro_proxy_points,
                "source": "demo",
                "is_proxy": True,
            }
        )
        if metro_series == "proxy":
            series = [series[-1]] + series[:-1]

        series.extend(
            [
                {
                    "metric": "bike_available_bikes_total",
                    "points": [asdict(p) for p in bike_available],
                    "source": "demo",
                    "is_proxy": False,
                },
                {
                    "metric": "bike_rent_proxy_total",
                    "points": [asdict(p) for p in bike_rent],
                    "source": "demo",
                    "is_proxy": True,
                },
                {
                    "metric": "bike_return_proxy_total",
                    "points": [asdict(p) for p in bike_return],
                    "source": "demo",
                    "is_proxy": True,
                },
            ]
        )

        return {
            "station_id": metro_station_id,
            "granularity": gran,
            "timezone": tz,
            "series": series,
        }

    def station_factors(self, metro_station_id: str) -> dict[str, Any]:
        df = self._station_features.copy()
        df["station_id"] = df["station_id"].astype(str)
        if metro_station_id not in set(df["station_id"]):
            raise KeyError(metro_station_id)

        row = df[df["station_id"] == metro_station_id].iloc[0].to_dict()
        numeric_cols = [
            c
            for c in df.columns
            if c != "station_id" and pd.api.types.is_numeric_dtype(df[c])
        ]

        factors = []
        for k, v in row.items():
            if k == "station_id":
                continue
            percentile = None
            if k in numeric_cols:
                pct_series = df[k].rank(pct=True, method="average")
                percentile = float(pct_series[df["station_id"] == metro_station_id].iloc[0])
            factors.append({"name": k, "value": v, "percentile": percentile})
        factors = sorted(factors, key=lambda x: x["name"])
        return {"station_id": metro_station_id, "factors": factors, "available": True}

    def similar_stations(
        self,
        metro_station_id: str,
        *,
        top_k: Optional[int] = None,
        metric: Optional[SimilarityMetric] = None,
        standardize: Optional[bool] = None,
    ) -> list[dict[str, Any]]:
        sim = find_similar_stations(
            self._station_features,
            station_id=metro_station_id,
            top_k=self._config.analytics.similarity.top_k if top_k is None else int(top_k),
            metric=self._config.analytics.similarity.metric if metric is None else metric,
            standardize=self._config.analytics.similarity.standardize
            if standardize is None
            else bool(standardize),
        )
        name_map = {m.station_id: m.name for m in self._metro}
        out = []
        for _, r in sim.iterrows():
            sid = str(r["station_id"])
            out.append({"station_id": sid, "name": name_map.get(sid), "distance": float(r["distance"])})
        return out

    def analytics_overview(self, *, top_n: int = 20) -> dict[str, Any]:
        # Demo-only placeholder
        return {"available": False}

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

    def _build_station_features(self) -> pd.DataFrame:
        suffix = f"r{int(self._config.spatial.radius_m)}m"
        if self._config.spatial.join_method != "buffer":
            suffix = f"k{int(self._config.spatial.nearest_k)}"

        bike_by_id = {b.station_id: b for b in self._bike}
        rows = []
        for metro in self._metro:
            linked = [l for l in self._links if l.metro_station_id == metro.station_id]
            capacities = [bike_by_id[l.bike_station_id].capacity or 0 for l in linked]
            distances = [l.distance_m for l in linked]

            rng = random.Random(metro.station_id)
            rows.append(
                {
                    "station_id": metro.station_id,
                    "district": rng.choice(["Datong", "Daan", "Xinyi"]),
                    f"bike_station_count_{suffix}": len({l.bike_station_id for l in linked}),
                    f"bike_capacity_sum_{suffix}": float(sum(capacities)),
                    f"bike_distance_mean_m_{suffix}": float(sum(distances) / max(len(distances), 1)),
                    "poi_count_300m": int(rng.uniform(20, 80)),
                    "poi_count_500m": int(rng.uniform(60, 200)),
                }
            )
        return pd.DataFrame(rows)
