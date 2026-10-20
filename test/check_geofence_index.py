"""
The STRtree geofence lookup must pick exactly the geofence the old linear scan
picked - including overlapping areas (first in list order wins) and points
outside every area - and should be much faster per event.

76 areas of 200-vertex polygons (some overlapping, some concave), 20,000 random
points across and around them.

  python test/check_geofence_index.py
"""
import asyncio
import math
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.abspath(__file__)))   # no repo .env (see check_counter_retrieval.py)
os.environ.setdefault("LOG_FILE", "False")

from shapely.geometry import Point, Polygon  # noqa: E402
from webhook.filter_data import WebhookFilter  # noqa: E402


def make_geofences(n=76, vertices=200, seed=1):
    rng = random.Random(seed)
    fences = []
    for i in range(n):
        cx, cy = 38.5 + (i % 10) * 0.05, -9.4 + (i // 10) * 0.05
        r = 0.03 if i % 7 else 0.06          # every 7th overlaps its neighbours
        pts = []
        for k in range(vertices):
            a = 2 * math.pi * k / vertices
            rr = r * (0.5 if (i % 5 == 0 and k % 20 < 10) else 1.0)   # star-ish concave shapes
            rr *= 1 + rng.uniform(-0.05, 0.05)
            pts.append([cy + rr * math.cos(a), cx + rr * math.sin(a)])    # [lon, lat]
        pts.append(pts[0])
        fences.append({"id": i + 1, "name": f"Area{i + 1:02d}", "offset": 0, "coordinates": [pts]})
    return sorted(fences, key=lambda g: (g["name"], g["id"]))


def old_lookup(fences, lat, lon):
    point = Point(lon, lat)
    for g in fences:
        if point.within(Polygon(g["coordinates"][0])):
            return g["id"]
    return None


async def main() -> int:
    fences = make_geofences()
    wf = WebhookFilter({"pokemon"}, fences)
    rng = random.Random(7)
    points = [(rng.uniform(38.4, 39.05), rng.uniform(-9.5, -8.85)) for _ in range(20000)]

    t0 = time.perf_counter()
    old = [old_lookup(fences, lat, lon) for lat, lon in points[:2000]]
    t_old = (time.perf_counter() - t0) / 2000

    await wf.is_inside_geofence(*points[0])            # build the index once
    t0 = time.perf_counter()
    new_all = [(await wf.is_inside_geofence(lat, lon))[1] for lat, lon in points]
    t_new = (time.perf_counter() - t0) / len(points)

    old_all = old + [old_lookup(fences, lat, lon) for lat, lon in points[2000:]]
    mismatches = sum(1 for a, b in zip(old_all, new_all) if a != b)
    inside = sum(1 for x in old_all if x is not None)
    polygons = [Polygon(g["coordinates"][0]) for g in fences]
    overlapping = sum(1 for lat, lon in points[:2000] if sum(Point(lon, lat).within(p) for p in polygons) > 1)
    print(f"points: {len(points)}, inside some area: {inside}, in 2+ areas (first 2000): {overlapping}, "
          f"mismatches vs linear scan: {mismatches}")
    print(f"per lookup: linear scan {t_old * 1e6:.0f} us -> indexed {t_new * 1e6:.0f} us ({t_old / t_new:.0f}x)")
    ok = mismatches == 0
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
