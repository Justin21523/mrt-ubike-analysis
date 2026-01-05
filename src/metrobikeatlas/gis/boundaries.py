from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable, Optional


LonLat = tuple[float, float]


def _ring_bbox(ring: list[LonLat]) -> tuple[float, float, float, float]:
    lons = [p[0] for p in ring]
    lats = [p[1] for p in ring]
    return min(lons), min(lats), max(lons), max(lats)


def _bbox_contains(bbox: tuple[float, float, float, float], lon: float, lat: float) -> bool:
    min_lon, min_lat, max_lon, max_lat = bbox
    return (min_lon <= lon <= max_lon) and (min_lat <= lat <= max_lat)


def _point_in_ring(lon: float, lat: float, ring: list[LonLat]) -> bool:
    """
    Ray casting point-in-polygon for a single ring (GeoJSON lon/lat order).
    """

    if len(ring) < 3:
        return False

    inside = False
    n = len(ring)
    for i in range(n):
        lon1, lat1 = ring[i]
        lon2, lat2 = ring[(i + 1) % n]

        intersects = (lat1 > lat) != (lat2 > lat)
        if not intersects:
            continue

        denom = (lat2 - lat1)
        if denom == 0:
            continue

        lon_at_lat = (lon2 - lon1) * (lat - lat1) / denom + lon1
        if lon < lon_at_lat:
            inside = not inside
    return inside


@dataclass(frozen=True)
class Polygon:
    exterior: list[LonLat]
    holes: list[list[LonLat]]
    bbox: tuple[float, float, float, float]

    @staticmethod
    def from_geojson_coordinates(coords: list[Any]) -> "Polygon":
        if not coords:
            raise ValueError("Empty Polygon coordinates")

        exterior = [(float(x), float(y)) for x, y in coords[0]]
        holes = [[(float(x), float(y)) for x, y in ring] for ring in coords[1:]]
        bbox = _ring_bbox(exterior)
        return Polygon(exterior=exterior, holes=holes, bbox=bbox)

    def contains(self, lon: float, lat: float) -> bool:
        if not _bbox_contains(self.bbox, lon, lat):
            return False
        if not _point_in_ring(lon, lat, self.exterior):
            return False
        for hole in self.holes:
            if _point_in_ring(lon, lat, hole):
                return False
        return True


@dataclass(frozen=True)
class BoundaryFeature:
    name: str
    polygons: list[Polygon]
    bbox: tuple[float, float, float, float]

    def contains(self, lon: float, lat: float) -> bool:
        if not _bbox_contains(self.bbox, lon, lat):
            return False
        return any(p.contains(lon, lat) for p in self.polygons)


class BoundaryIndex:
    """
    Minimal admin boundary index for point -> district lookup.

    Expected input: GeoJSON FeatureCollection with Polygon/MultiPolygon geometries.
    """

    def __init__(self, boundaries: list[BoundaryFeature]) -> None:
        self._boundaries = boundaries

    @staticmethod
    def from_geojson(
        path: Path,
        *,
        name_property: Optional[str] = None,
        name_property_candidates: Iterable[str] = ("district", "town", "TOWNNAME", "NAME", "name"),
    ) -> "BoundaryIndex":
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("type") != "FeatureCollection":
            raise ValueError("GeoJSON must be a FeatureCollection")

        boundaries: list[BoundaryFeature] = []
        features = raw.get("features", [])
        for feature in features:
            props = feature.get("properties") or {}
            name = None
            if name_property and props.get(name_property):
                name = str(props[name_property])
            else:
                for candidate in name_property_candidates:
                    if props.get(candidate):
                        name = str(props[candidate])
                        break
            if not name:
                continue

            geom = feature.get("geometry") or {}
            geom_type = geom.get("type")
            coords = geom.get("coordinates")
            if not geom_type or coords is None:
                continue

            polygons: list[Polygon] = []
            if geom_type == "Polygon":
                polygons.append(Polygon.from_geojson_coordinates(coords))
            elif geom_type == "MultiPolygon":
                for poly_coords in coords:
                    polygons.append(Polygon.from_geojson_coordinates(poly_coords))
            else:
                continue

            min_lon = min(p.bbox[0] for p in polygons)
            min_lat = min(p.bbox[1] for p in polygons)
            max_lon = max(p.bbox[2] for p in polygons)
            max_lat = max(p.bbox[3] for p in polygons)
            bbox = (min_lon, min_lat, max_lon, max_lat)
            boundaries.append(BoundaryFeature(name=name, polygons=polygons, bbox=bbox))

        if not boundaries:
            raise ValueError("No usable boundary features found in GeoJSON")

        return BoundaryIndex(boundaries)

    def lookup(self, *, lat: float, lon: float) -> Optional[str]:
        for boundary in self._boundaries:
            if boundary.contains(lon, lat):
                return boundary.name
        return None

