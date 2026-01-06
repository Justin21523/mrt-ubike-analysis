from __future__ import annotations

# `logging` is used to record ingestion issues without hiding them behind silent failures.
import logging
# We parse availability timestamps into timezone-aware `datetime` values for safe temporal alignment later.
from datetime import datetime
# Typing helpers keep our parsing rules explicit while we still consume raw JSON dicts from TDX.
from typing import Any, Mapping, Optional

# Typed settings tell this client which station/availability endpoints to call for each city.
from metrobikeatlas.config.models import TDXBikeSettings, TDXSettings
# `TDXClient` handles OAuth tokens, retries, and HTTP details so this module stays focused on bike semantics.
from metrobikeatlas.ingestion.tdx_base import TDXClient
# Schemas define our normalized station metadata and availability records (used in Silver and API outputs).
from metrobikeatlas.schemas.core import BikeAvailability, BikeStation
# `JsonFileCache` optionally stores raw JSON responses for faster EDA and reduced API calls.
from metrobikeatlas.utils.cache import JsonFileCache


# Module-level logger is standard for consistent log messages across ingestion scripts.
logger = logging.getLogger(__name__)


# `TDXBikeClient` wraps `TDXClient` with bike-specific endpoints and normalization rules.
# Design goal: make Silver-building code consume stable dataclasses (BikeStation/BikeAvailability) even
# though the upstream TDX JSON fields vary across operators and cities.
class TDXBikeClient:
    def __init__(
        self,
        *,
        tdx: TDXClient,
        settings: TDXSettings,
        bike: TDXBikeSettings,
        cache: Optional[JsonFileCache] = None,
    ) -> None:
        # Store the shared low-level client (session + token cache) so repeated calls are efficient.
        self._tdx = tdx
        # Keep settings so cache keys can include stable request metadata (base_url/path/params).
        self._settings = settings
        # Bike-specific settings include endpoint templates for stations and realtime availability.
        self._bike = bike
        # Cache is optional; in production you may prefer storing raw Bronze files instead of caching in-memory.
        self._cache = cache

    def list_stations(self, city: str, *, use_cache: bool = True) -> list[BikeStation]:
        # Format the endpoint path using the configured template for the requested city.
        path = self._bike.stations_path_template.format(city=city)
        # Force JSON output for consistent parsing.
        params = {"$format": "JSON"}

        # Prepare a cache key only if caching is enabled for this call.
        cache_key = None
        # We gate caching behind both a cache object and the per-call `use_cache` flag.
        if self._cache is not None and use_cache:
            # Cache key includes request metadata so different base URLs or params do not collide.
            cache_key = self._cache.make_key(
                "tdx:bike:stations",
                {"base_url": self._settings.base_url, "path": path, "params": params},
            )
            # Fast path: return cached response parsed into typed `BikeStation` objects.
            cached = self._cache.get(cache_key)
            if cached is not None:
                return [self.parse_station(item, city=city) for item in cached]

        # Fetch raw JSON from TDX; token/retry/timeout is handled inside `TDXClient`.
        data = self._tdx.get_json(path, params=params)
        # Cache the raw JSON so repeated local runs don't spam the API and remain deterministic.
        if self._cache is not None and use_cache and cache_key is not None:
            self._cache.set(cache_key, data)
        # Normalize each raw record into our internal schema so downstream code can rely on stable fields.
        return [self.parse_station(item, city=city) for item in data]

    def fetch_availability_snapshot(self, city: str, *, use_cache: bool = False) -> list[BikeAvailability]:
        """
        Fetch a single availability snapshot (realtime-ish).

        For a time series, run this periodically (cron) and persist to `data/bronze/...`.
        """

        # Availability endpoint is separate from station metadata and changes frequently (realtime-ish).
        path = self._bike.availability_path_template.format(city=city)
        # Force JSON output for consistent parsing.
        params = {"$format": "JSON"}

        # Default `use_cache=False` because availability is time-varying; caching can hide real changes.
        cache_key = None
        if self._cache is not None and use_cache:
            cache_key = self._cache.make_key(
                "tdx:bike:availability",
                {"base_url": self._settings.base_url, "path": path, "params": params},
            )
            cached = self._cache.get(cache_key)
            # Cached availability is still parsed into typed records so callers see the same schema.
            if cached is not None:
                return [self.parse_availability(item) for item in cached]

        # Fetch raw JSON snapshot from TDX.
        data = self._tdx.get_json(path, params=params)
        # If explicitly enabled, store the raw response in cache (useful for debugging without network).
        if self._cache is not None and use_cache and cache_key is not None:
            self._cache.set(cache_key, data)
        # Normalize each raw record into our internal availability schema.
        return [self.parse_availability(item) for item in data]

    @staticmethod
    def parse_station(item: Mapping[str, Any], *, city: str) -> BikeStation:
        # Station identifiers are not consistent across operators; we try the common variants.
        station_id = (
            item.get("StationUID")
            or item.get("StationId")
            or item.get("StationID")
            or item.get("UID")
        )
        # A stable station id is required for joins (metro<->bike links, timeseries aggregation), so fail fast.
        if not station_id:
            raise ValueError(f"Missing station id in record: {item}")

        # Station names may be nested (`{"Zh_tw": "...", "En": "..."}`) or a raw string.
        name_block = item.get("StationName") or {}
        # Prefer Chinese name (`Zh_tw`) because the dashboard is Taiwan-focused.
        if isinstance(name_block, dict):
            name = name_block.get("Zh_tw") or name_block.get("En") or str(station_id)
        else:
            # If the name is not a dict, fall back to a string representation.
            name = str(name_block) if name_block else str(station_id)

        # Station positions are sometimes nested and sometimes flattened; we try common key variants.
        pos = item.get("StationPosition") or item.get("Position") or {}
        # Cast to float so spatial joins (distance computations) work reliably.
        lat = float(pos.get("PositionLat") or pos.get("Lat") or pos.get("latitude"))
        lon = float(pos.get("PositionLon") or pos.get("Lon") or pos.get("longitude"))

        # Operator/system identifiers are useful for labeling and debugging multi-operator datasets.
        operator = item.get("OperatorID") or item.get("Operator") or "BIKE"
        # Capacity may be missing; we keep it optional so the schema remains valid across cities.
        capacity = item.get("BikesCapacity") or item.get("Capacity")
        # Convert capacity to int when present; keep None when absent to avoid fake zeros.
        capacity_value = None if capacity is None else int(capacity)

        # Return a typed dataclass so the Silver layer and API can rely on stable field names.
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
        # Availability records also contain a station id; we normalize to the same field used in stations.
        station_id = (
            item.get("StationUID")
            or item.get("StationId")
            or item.get("StationID")
            or item.get("UID")
        )
        # Without a station id we cannot join the snapshot to station metadata, so fail fast.
        if not station_id:
            raise ValueError(f"Missing station id in availability record: {item}")

        # Update timestamps differ by provider; we try common variants used by TDX feeds.
        ts_raw = item.get("UpdateTime") or item.get("SrcUpdateTime") or item.get("UpdateTimestamp")
        # Time is required to build a time series; a missing timestamp makes the record unusable.
        if not ts_raw:
            raise ValueError(f"Missing update time in availability record: {item}")
        # Parse ISO timestamps into Python datetime; we force "Z" to "+00:00" to ensure timezone-aware UTC.
        ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))

        # Available bikes sometimes appears under different keys; we treat missing as 0 to keep charts stable.
        available_bikes = item.get("AvailableRentBikes") or item.get("AvailableBikes") or 0
        # Available docks may be missing depending on provider; keep None to represent "unknown".
        available_docks = item.get("AvailableReturnBikes") or item.get("AvailableDocks")

        # Return a typed dataclass so temporal alignment and proxy calculations can rely on stable columns.
        return BikeAvailability(
            station_id=str(station_id),
            ts=ts,
            available_bikes=int(available_bikes),
            available_docks=None if available_docks is None else int(available_docks),
            source="tdx",
        )
