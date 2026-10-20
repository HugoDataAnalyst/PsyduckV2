"""
Synthetic map shared by the fake Koji server and the feeder.

A grid of rectangular polygons over Lisbon, so every area resolves to
Europe/Lisbon and the feeder can place points inside an area without
any geometry library.
"""
import random

ORIGIN_LAT = 38.70
ORIGIN_LON = -9.25
CELL_DEG = 0.02
ROWS = 3
COLS = 4


def build_areas():
    areas = []
    for r in range(ROWS):
        for c in range(COLS):
            lat0 = ORIGIN_LAT + r * CELL_DEG
            lon0 = ORIGIN_LON + c * CELL_DEG
            areas.append({
                "name": f"TestArea{r * COLS + c + 1:02d}",
                "lat0": lat0,
                "lat1": lat0 + CELL_DEG,
                "lon0": lon0,
                "lon1": lon0 + CELL_DEG,
            })
    return areas


AREAS = build_areas()


def koji_feature_collection():
    """Koji's /api/v1/geofence/feature-collection/<project> response shape."""
    features = []
    for a in AREAS:
        ring = [
            [a["lon0"], a["lat0"]],
            [a["lon1"], a["lat0"]],
            [a["lon1"], a["lat1"]],
            [a["lon0"], a["lat1"]],
            [a["lon0"], a["lat0"]],
        ]
        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": {"name": a["name"]},
        })
    return {"data": {"type": "FeatureCollection", "features": features}}


def random_point(rng: random.Random, area=None):
    """A point strictly inside an area (a small margin keeps it off the edges)."""
    a = area or rng.choice(AREAS)
    m = CELL_DEG * 0.02
    return (rng.uniform(a["lat0"] + m, a["lat1"] - m),
            rng.uniform(a["lon0"] + m, a["lon1"] - m),
            a["name"])
