from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

from metrobikeatlas.config.models import TDXMetroSettings, TDXSettings
from metrobikeatlas.ingestion.tdx_base import TDXClient
from metrobikeatlas.schemas.core import MetroStation
from metrobikeatlas.utils.cache import JsonFileCache


logger = logging.getLogger(__name__)


class TDXMetroClient:
    def __init__(
        self,
        *,
        tdx: TDXClient,
        settings: TDXSettings,
        metro: TDXMetroSettings,
        cache: Optional[JsonFileCache] = None,
    ) -> None:
        self._tdx = tdx
        self._settings = settings
        self._metro = metro
        self._cache = cache

    def list_stations(self, city: str, *, use_cache: bool = True) -> list[MetroStation]:
        path = self._metro.stations_path_template.format(city=city)
        params = {"$format": "JSON"}

        cache_key = None
        if self._cache is not None and use_cache:
            cache_key = self._cache.make_key(
                "tdx:metro:stations",
                {"base_url": self._settings.base_url, "path": path, "params": params},
            )
            cached = self._cache.get(cache_key)
            if cached is not None:
                return [self.parse_station(item, city=city) for item in cached]

        data = self._tdx.get_json(path, params=params)
        if self._cache is not None and use_cache and cache_key is not None:
            self._cache.set(cache_key, data)

        return [self.parse_station(item, city=city) for item in data]

    def fetch_ridership_timeseries(self, city: str, *, use_cache: bool = True) -> list[Mapping[str, Any]]:
        """
        Station-level ridership is not consistently available via TDX across metro systems.

        If `ridership_path_template` is configured, this method will call it; otherwise it
        returns an empty list (the API/web can fall back to demo data for the MVP).
        """

        if not self._metro.ridership_path_template:
            logger.warning("No metro ridership path configured; returning empty ridership series.")
            return []

        path = self._metro.ridership_path_template.format(city=city)
        params = {"$format": "JSON"}

        cache_key = None
        if self._cache is not None and use_cache:
            cache_key = self._cache.make_key(
                "tdx:metro:ridership",
                {"base_url": self._settings.base_url, "path": path, "params": params},
            )
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        data = self._tdx.get_json(path, params=params)
        if self._cache is not None and use_cache and cache_key is not None:
            self._cache.set(cache_key, data)
        return data

    @staticmethod
    def parse_station(item: Mapping[str, Any], *, city: str) -> MetroStation:
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
            name_en = name_block.get("En")
        else:
            name = str(name_block) if name_block else str(station_id)
            name_en = None

        pos = item.get("StationPosition") or item.get("Position") or {}
        lat = float(pos.get("PositionLat") or pos.get("Lat") or pos.get("latitude"))
        lon = float(pos.get("PositionLon") or pos.get("Lon") or pos.get("longitude"))

        operator = item.get("OperatorID") or item.get("Operator") or "METRO"

        return MetroStation(
            station_id=str(station_id),
            name=str(name),
            name_en=name_en,
            lat=lat,
            lon=lon,
            city=city,
            system=str(operator),
        )
