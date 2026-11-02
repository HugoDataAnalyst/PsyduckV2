"""
Weather sync from Golbat's weather table, against the test stack.

Real S2 level-10 cells (Porto/Algarve, away from the test areas), test areas
9001-9005 drawn from the cells' own corners, Golbat rows in golbat.weather:

  seed     cell A, no stored row, overlaps areas 9001 and 9002   -> 2 rows
  same     cell B, stored with the same condition                -> nothing
  missed   cell C, stored RAINY, Golbat says CLEAR               -> 1 row, after the stored one
  stale    cell D, Golbat last refreshed 3 h ago                 -> nothing

then a second pass must write nothing.

  python test/check_weather_golbat_sync.py      (stack up)
"""
import asyncio
import os
import sys
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))
os.chdir(HERE)   # no repo .env (see check_counter_retrieval.py)
os.environ.update(DB_HOST="127.0.0.1", DB_PORT="13306", DB_USER="psyduck", DB_PASSWORD="psyduck", DB_NAME="PsyduckV2",
                  GOLBAT_DB_HOST="127.0.0.1", GOLBAT_DB_PORT="13306", GOLBAT_DB_USER="psyduck",
                  GOLBAT_DB_PASSWORD="psyduck", GOLBAT_DB_NAME="golbat",
                  REDIS_HOST="127.0.0.1", REDIS_SERVER_PORT="16379", REDIS_PASSWORD="testpass", REDIS_DB="1",
                  LOG_FILE="False", LOG_LEVEL="WARNING")

import harness as h  # noqa: E402
from shapely.geometry import Polygon  # noqa: E402
from utils.s2cell import corners  # noqa: E402

CELL_A, CELL_B, CELL_C, CELL_D = 946992872329052160, 946995071352307712, 944266083492167680, 947364507259240448
AREAS = (9001, 9002, 9003, 9004, 9005)


def ring(cell):
    """Geofence = the cell itself, shrunk a hair so neighbouring cells only touch it."""
    shape = Polygon([(lon, lat) for lat, lon in corners(cell)]).buffer(-1e-6)
    return [list(p) for p in shape.exterior.coords]


def cell_half(cell, side):
    """Geofence covering one half (west/east) of a cell, so the cell overlaps two areas."""
    p = Polygon([(lon, lat) for lat, lon in corners(cell)])
    minx, miny, maxx, maxy = p.bounds
    mid = (minx + maxx) / 2
    box = Polygon([(minx, miny), (mid, miny), (mid, maxy), (minx, maxy)]) if side == "west" else \
        Polygon([(mid, miny), (maxx, miny), (maxx, maxy), (mid, maxy)])
    return [list(c) for c in p.intersection(box).exterior.coords]


GEOFENCES = [
    {"id": 9001, "name": "SyncArea1", "offset": 0, "coordinates": [cell_half(CELL_A, "west")]},
    {"id": 9002, "name": "SyncArea2", "offset": 0, "coordinates": [cell_half(CELL_A, "east")]},
    {"id": 9003, "name": "SyncArea3", "offset": 0, "coordinates": [ring(CELL_B)]},
    {"id": 9004, "name": "SyncArea4", "offset": 0, "coordinates": [ring(CELL_C)]},
    {"id": 9005, "name": "SyncArea5", "offset": 0, "coordinates": [ring(CELL_D)]},
]


def cleanup():
    h.sql("DELETE FROM weather_daily_events WHERE area_id BETWEEN 9001 AND 9005")
    h.sql("DELETE FROM weather WHERE id IN (%s, %s, %s, %s)", (CELL_A, CELL_B, CELL_C, CELL_D), database="golbat")


async def main() -> int:
    import sql.connect_db as db
    from sql.tasks.weather_golbat_sync import sync_once
    cleanup()
    now = int(time.time())
    hour = now - now % 3600
    for cell, cond, updated in ((CELL_A, 3, now - 600), (CELL_B, 1, now - 600),
                                (CELL_C, 1, now - 600), (CELL_D, 4, now - 3 * 3600)):
        lat, lon = map(lambda v: sum(v) / 4, zip(*corners(cell)))
        h.sql("INSERT INTO weather (id, level, latitude, longitude, gameplay_condition, warn_weather, updated) "
              "VALUES (%s, 10, %s, %s, %s, 0, %s)", (cell, lat, lon, cond, updated), database="golbat")
    earlier = datetime.fromtimestamp(hour - 5400, timezone.utc).replace(tzinfo=None)
    for cell, area, cond in ((CELL_B, 9003, 1), (CELL_C, 9004, 2)):
        h.sql("INSERT INTO weather_daily_events VALUES (%s, %s, %s, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, %s, %s)",
              (cell, area, cond, earlier, earlier.date()))

    await db.init_db()
    first = await sync_once(GEOFENCES, 2)
    second = await sync_once(GEOFENCES, 2)
    rows = h.sql("SELECT s2_cell_id, area_id, gameplay_condition, seen_at FROM weather_daily_events "
                 "WHERE area_id BETWEEN 9001 AND 9005 ORDER BY s2_cell_id, area_id, seen_at")
    await db.close_db()

    hour_dt = datetime.fromtimestamp(hour, timezone.utc).replace(tzinfo=None)
    want = sorted([
        (CELL_A, 9001, 3, hour_dt), (CELL_A, 9002, 3, hour_dt),          # seeded in both areas
        (CELL_B, 9003, 1, earlier),                                       # unchanged
        (CELL_C, 9004, 2, earlier), (CELL_C, 9004, 1, hour_dt),           # missed change
    ])
    got = sorted((int(c), int(a), int(g), s) for c, a, g, s in rows)
    print(f"first pass: {first}")
    print(f"second pass: {second}")
    for r in got:
        print("  row", r)
    errors = []
    if got != want:
        errors.append(f"rows {got} != {want}")
    if (first["seeded"], first["missed_changes"]) != (2, 1):
        errors.append(f"first pass {first}")
    if (second["seeded"], second["missed_changes"]) != (0, 0):
        errors.append(f"second pass wrote again {second}")
    cleanup()
    for e in errors:
        print("  FAIL:", e)
    print("RESULT:", "PASS" if not errors else "FAIL")
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
