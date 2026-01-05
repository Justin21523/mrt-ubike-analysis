from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Mapping, Optional

from metrobikeatlas.config.models import TDXBikeSettings, TDXSettings
from metrobikeatlas.ingestion.tdx_base import TDXClient
from metrobikeatlas.schemas.core import BikeAvailability, BikeStation
from metrobikeatlas.utils.cache import JsonFileCache


logger = logging.getLogger(__name__)


class TDXBikeClient:
    def __init__(
        self,
        *,
        tdx: TDXClient,
        settings: TDXSettings,
        bike: TDXBikeSettings,
        cache: Optional[JsonFileCache] = None,
    ) -> None:
        self._tdx = tdx
        self._settings = settings
        self._bike = bike
        self._cache = cache

    def list_stations(self, city: str, *, use_cache: bool = True) -> list[BikeStation]:
        path = self._bike.stations_path_template.format(city=city)
        params = {"$format": "JSON"}

        cache_key = None
        if self._cache is not None and use_cache:
            cache_key = self._cache.make_key(
                "tdx:bike:stations",
                {"base_url": self._settings.base_url, "path": path, "params": params},
            )
            cached = self._cache.get(cache_key)
            if cached is not None:
                return [self.parse_station(item, city=city) for item in cached]

        data = self._tdx.get_json(path, params=params)
        if self._cache is not None and use_cache and cache_key is not None:
            self._cache.set(cache_key, data)
        return [self.parse_station(item, city=city) for item in data]

    def fetch_availability_snapshot(self, city: str, *, use_cache: bool = False) -> list[BikeAvailability]:
        """
        Fetch a single availability snapshot (realtime-ish).

        For a time series, run this periodically (cron) and persist to `data/bronze/...`.
        """

        path = self._bike.availability_path_template.format(city=city)
        params = {"$format": "JSON"}

        cache_key = None
        if self._cache is not None and use_cache:
            cache_key = self._cache.make_key(
                "tdx:bike:availability",
                {"base_url": self._settings.base_url, "path": path, "params": params},
            )
            cached = self._cache.get(cache_key)
            if cached is not None:
                return [self.parse_availability(item) for item in cached]

        data = self._tdx.get_json(path, params=params)
        if self._cache is not None and use_cache and cache_key is not None:
            self._cache.set(cache_key, data)
        return [self.parse_availability(item) for item in data]

    @staticmethod
    def parse_station(item: Mapping[str, Any], *, city: str) -> BikeStation:
        station_id = (
            item.get("StationUID")
            or item.get("StationId")
            or item.get("StationID")
            or item.get("UID")
        )
        if not station_id:
            raise ValueError(f"Missing station id in record: {item}")

        name_block = item.get("StationName") or {}
        if isinstance(name_block, dict):
            name = name_block.get("Zh_tw") or name_block.get("En") or str(station_id)
        else:
            name = str(name_block) if name_block else str(station_id)

        pos = item.get("StationPosition") or item.get("Position") or {}
        lat = float(pos.get("PositionLat") or pos.get("Lat") or pos.get("latitude"))
        lon = float(pos.get("PositionLon") or pos.get("Lon") or pos.get("longitude"))

        operator = item.get("OperatorID") or item.get("Operator") or "BIKE"
        capacity = item.get("BikesCapacity") or item.get("Capacity")
        capacity_value = None if capacity is None else int(capacity)

        return BikeStation(
            station_id=str(station_id),
            name=str(name),
            lat=lat,
            lon=lon,
            city=city,
            operator=str(operator),
            capacity=capacity_value,
        )

    @staticmethod
    def parse_availability(item: Mapping[str, Any]) -> BikeAvailability:
        station_id = (
            item.get("StationUID")
            or item.get("StationId")
            or item.get("StationID")
            or item.get("UID")
        )
        if not station_id:
            raise ValueError(f"Missing station id in availability record: {item}")

        ts_raw = item.get("UpdateTime") or item.get("SrcUpdateTime") or item.get("UpdateTimestamp")
        if not ts_raw:
            raise ValueError(f"Missing update time in availability record: {item}")
        ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))

        available_bikes = item.get("AvailableRentBikes") or item.get("AvailableBikes") or 0
        available_docks = item.get("AvailableReturnBikes") or item.get("AvailableDocks")

        return BikeAvailability(
            station_id=str(station_id),
            ts=ts,
            available_bikes=int(available_bikes),
            available_docks=None if available_docks is None else int(available_docks),
            source="tdx",
        )
