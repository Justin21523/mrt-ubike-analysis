from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import math
import time
from typing import Any, Iterable, Optional

import requests

from metrobikeatlas.utils.cache import JsonFileCache


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OverpassSettings:
    url: str = "https://overpass-api.de/api/interpreter"
    timeout_s: int = 180
    user_agent: str = "metrobikeatlas/0.1.0"
    sleep_s: float = 1.0


class OverpassError(RuntimeError):
    pass


def build_bbox_from_points(
    *,
    lats: Iterable[float],
    lons: Iterable[float],
    padding_m: float,
) -> tuple[float, float, float, float]:
    """
    Compute a rough bbox around points with meter-based padding.

    Returns (south, west, north, east) in degrees.
    """

    lats_list = list(lats)
    lons_list = list(lons)
    if not lats_list or not lons_list:
        raise ValueError("No points provided for bbox computation")

    south = min(lats_list)
    north = max(lats_list)
    west = min(lons_list)
    east = max(lons_list)

    # Approx: 1 deg lat ~ 111km; lon scales by cos(lat)
    mid_lat = (south + north) / 2.0
    lat_pad = padding_m / 111_000.0
    lon_pad = padding_m / (111_000.0 * max(abs(math.cos(math.radians(mid_lat))), 1e-6))

    return south - lat_pad, west - lon_pad, north + lat_pad, east + lon_pad


def build_overpass_query_for_category(
    *,
    category: str,
    bbox: tuple[float, float, float, float],
    timeout_s: int = 180,
) -> str:
    """
    Build a conservative Overpass QL query for a category within a bbox.

    The mapping below is a pragmatic MVP default and can be refined later.
    """

    south, west, north, east = bbox

    mappings: dict[str, list[str]] = {
        "food": [
            'node["amenity"~"restaurant|cafe|fast_food"]',
            'way["amenity"~"restaurant|cafe|fast_food"]',
            'relation["amenity"~"restaurant|cafe|fast_food"]',
        ],
        "transit": [
            'node["public_transport"]',
            'way["public_transport"]',
            'relation["public_transport"]',
            'node["railway"="station"]',
            'way["railway"="station"]',
        ],
        "education": [
            'node["amenity"~"school|university|college|kindergarten"]',
            'way["amenity"~"school|university|college|kindergarten"]',
            'relation["amenity"~"school|university|college|kindergarten"]',
        ],
        "office": ['node["office"]', 'way["office"]', 'relation["office"]'],
        "park": ['node["leisure"="park"]', 'way["leisure"="park"]', 'relation["leisure"="park"]'],
        "tourism": ['node["tourism"]', 'way["tourism"]', 'relation["tourism"]'],
    }

    selectors = mappings.get(category)
    if not selectors:
        raise ValueError(f"Unsupported category mapping: {category}")

    body = "\n".join([f"  {sel}({south},{west},{north},{east});" for sel in selectors])
    return f"""
[out:json][timeout:{int(timeout_s)}];
(
{body}
);
out center;
""".strip()


class OverpassClient:
    def __init__(
        self,
        *,
        settings: OverpassSettings,
        cache: Optional[JsonFileCache] = None,
    ) -> None:
        self._settings = settings
        self._cache = cache
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": settings.user_agent})

    def query(self, query: str, *, use_cache: bool = True) -> dict[str, Any]:
        cache_key = None
        if self._cache is not None and use_cache:
            cache_key = self._cache.make_key(
                "overpass:query",
                {"url": self._settings.url, "query": query},
            )
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        resp = self._session.post(
            self._settings.url,
            data={"data": query},
            timeout=self._settings.timeout_s,
        )
        if resp.status_code >= 400:
            raise OverpassError(f"Overpass request failed ({resp.status_code}): {resp.text[:500]}")

        data = resp.json()
        if self._cache is not None and use_cache and cache_key is not None:
            self._cache.set(cache_key, data)
        return data

    def polite_sleep(self) -> None:
        time.sleep(float(self._settings.sleep_s))

    def close(self) -> None:
        self._session.close()


def elements_to_poi_rows(elements: list[dict[str, Any]], *, category: str) -> list[dict[str, Any]]:
    rows = []
    for el in elements:
        el_type = el.get("type")
        el_id = el.get("id")
        if not el_type or el_id is None:
            continue

        tags = el.get("tags") or {}
        name = tags.get("name") or tags.get("name:en")

        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            center = el.get("center") or {}
            lat = center.get("lat")
            lon = center.get("lon")
        if lat is None or lon is None:
            continue

        rows.append(
            {
                "source": "osm_overpass",
                "category": category,
                "osm_type": str(el_type),
                "osm_id": str(el_id),
                "name": name,
                "lat": float(lat),
                "lon": float(lon),
                "tags": json.dumps(tags, ensure_ascii=False),
            }
        )
    return rows
