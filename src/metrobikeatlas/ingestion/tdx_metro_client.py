from __future__ import annotations

# `logging` is used to emit operational signals (e.g., missing optional endpoints) without crashing the pipeline.
import logging
# Typing helpers keep parsing functions explicit while we still consume raw JSON dicts from TDX.
from typing import Any, Mapping, Optional

# Typed settings tell this client which TDX paths to call and which cities to iterate over.
from metrobikeatlas.config.models import TDXMetroSettings, TDXSettings
# `TDXClient` handles OAuth tokens, retries, and HTTP details so this module stays focused on metro semantics.
from metrobikeatlas.ingestion.tdx_base import TDXClient
# `MetroStation` is our typed schema for normalized station metadata (used in Silver tables and API outputs).
from metrobikeatlas.schemas.core import MetroStation
# `JsonFileCache` optionally stores raw JSON responses to speed up local development and reduce API calls.
from metrobikeatlas.utils.cache import JsonFileCache


# Module-level logger is the standard pattern for structured logs in Python.
# We keep it here so every method can log consistently without passing logger around.
logger = logging.getLogger(__name__)


# `TDXMetroClient` wraps the generic `TDXClient` with metro-specific endpoints and parsing rules.
# Design goal: separate concerns:
# - `TDXClient`: authentication + HTTP reliability
# - `TDXMetroClient`: "which metro endpoints" + "how to normalize metro station JSON"
class TDXMetroClient:
    def __init__(
        self,
        *,
        tdx: TDXClient,
        settings: TDXSettings,
        metro: TDXMetroSettings,
        cache: Optional[JsonFileCache] = None,
    ) -> None:
        # Store the shared low-level client (session + token cache) so repeated calls are efficient.
        self._tdx = tdx
        # Keep settings so cache keys can include stable request metadata (base_url/path/params).
        self._settings = settings
        # Metro-specific settings include endpoint path templates and optional ridership endpoints.
        self._metro = metro
        # Cache is optional because production pipelines may prefer an external cache (or no cache at all).
        self._cache = cache

    def list_stations(self, city: str, *, use_cache: bool = True) -> list[MetroStation]:
        # Format the endpoint path using the configured template (TDX uses city in the URL path).
        path = self._metro.stations_path_template.format(city=city)
        # TDX OData endpoints accept `$format=JSON` to force JSON output (avoids XML defaults).
        params = {"$format": "JSON"}

        # Prepare a cache key only if caching is enabled for this call.
        cache_key = None
        # We gate caching behind both a cache object and the per-call `use_cache` flag.
        if self._cache is not None and use_cache:
            # Cache key includes request metadata so different base URLs or params do not collide.
            cache_key = self._cache.make_key(
                "tdx:metro:stations",
                {"base_url": self._settings.base_url, "path": path, "params": params},
            )
            # Fast path: if we have cached JSON, parse it into typed `MetroStation` objects and return.
            cached = self._cache.get(cache_key)
            if cached is not None:
                return [self.parse_station(item, city=city) for item in cached]

        # Fetch raw JSON from TDX; token/retry/timeout is handled inside `TDXClient`.
        data = self._tdx.get_json(path, params=params)
        # Store the raw JSON response to cache so subsequent runs can skip the network call.
        if self._cache is not None and use_cache and cache_key is not None:
            self._cache.set(cache_key, data)

        # Normalize each raw record into our internal schema so downstream code can rely on stable fields.
        return [self.parse_station(item, city=city) for item in data]

    def fetch_ridership_timeseries(self, city: str, *, use_cache: bool = True) -> list[Mapping[str, Any]]:
        """
        Station-level ridership is not consistently available via TDX across metro systems.

        If `ridership_path_template` is configured, this method will call it; otherwise it
        returns an empty list (the API/web can fall back to demo data for the MVP).
        """

        # Some cities/systems do not expose ridership via TDX; we treat it as an optional dataset.
        if not self._metro.ridership_path_template:
            # Warning is helpful for operators: it explains why the API may fall back to proxies.
            logger.warning("No metro ridership path configured; returning empty ridership series.")
            # Returning an empty list keeps the pipeline resilient (no hard failure for missing optional data).
            return []

        # Build the ridership endpoint path from config for the requested city.
        path = self._metro.ridership_path_template.format(city=city)
        # Force JSON output for consistent parsing.
        params = {"$format": "JSON"}

        # Cache works the same way as for stations: key is derived from request metadata.
        cache_key = None
        if self._cache is not None and use_cache:
            cache_key = self._cache.make_key(
                "tdx:metro:ridership",
                {"base_url": self._settings.base_url, "path": path, "params": params},
            )
            cached = self._cache.get(cache_key)
            # Ridership series is returned as raw dicts because schemas vary across systems.
            if cached is not None:
                return cached

        # Fetch raw series JSON from TDX.
        data = self._tdx.get_json(path, params=params)
        # Cache the raw payload so repeated experiments (EDA) are fast and deterministic.
        if self._cache is not None and use_cache and cache_key is not None:
            self._cache.set(cache_key, data)
        # Return raw records; downstream preprocessing can map to a canonical timeseries schema if needed.
        return data

    @staticmethod
    def parse_station(item: Mapping[str, Any], *, city: str) -> MetroStation:
        # TDX field names differ across systems (UID vs ID, different casing), so we try common variants.
        station_id = (
            item.get("StationUID")
            or item.get("StationId")
            or item.get("StationID")
            or item.get("UID")
        )
        # A stable station id is required for joins (metro<->bike links, timeseries lookups), so fail fast.
        if not station_id:
            raise ValueError(f"Missing station id in record: {item}")

        # Station names are sometimes nested (`{"Zh_tw": "...", "En": "..."}`) and sometimes a string.
        name_block = item.get("StationName") or {}
        # Prefer Chinese name (`Zh_tw`) for UI display in Taiwan; keep English if available.
        if isinstance(name_block, dict):
            name = name_block.get("Zh_tw") or name_block.get("En") or str(station_id)
            name_en = name_block.get("En")
        else:
            # If no dict is provided, treat it as a raw string and keep English unknown.
            name = str(name_block) if name_block else str(station_id)
            name_en = None

        # Positions are also inconsistent; we try standard TDX keys first, then fall back to common alternates.
        pos = item.get("StationPosition") or item.get("Position") or {}
        # We cast to float so downstream geospatial code can compute distances without type issues.
        lat = float(pos.get("PositionLat") or pos.get("Lat") or pos.get("latitude"))
        lon = float(pos.get("PositionLon") or pos.get("Lon") or pos.get("longitude"))

        # Operator/system names differ across cities; we keep a best-effort string for labeling.
        operator = item.get("OperatorID") or item.get("Operator") or "METRO"

        # Return a typed dataclass so the Silver layer and API can rely on stable field names.
        return MetroStation(
            station_id=str(station_id),
            name=str(name),
            name_en=name_en,
            lat=lat,
            lon=lon,
            city=city,
            system=str(operator),
        )
