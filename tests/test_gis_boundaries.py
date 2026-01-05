from __future__ import annotations

import json
from pathlib import Path

from metrobikeatlas.gis.boundaries import BoundaryIndex


def test_boundary_lookup_point_in_polygon(tmp_path: Path) -> None:
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"district": "A"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [0.0, 0.0],
                            [1.0, 0.0],
                            [1.0, 1.0],
                            [0.0, 1.0],
                            [0.0, 0.0],
                        ]
                    ],
                },
            }
        ],
    }
    path = tmp_path / "boundaries.geojson"
    path.write_text(json.dumps(geojson), encoding="utf-8")

    idx = BoundaryIndex.from_geojson(path)
    assert idx.lookup(lat=0.5, lon=0.5) == "A"
    assert idx.lookup(lat=2.0, lon=2.0) is None

