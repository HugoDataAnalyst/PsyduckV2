"""
Max battles, tappables and the weather API, end to end on the test stack.

1. Feed max battles, tappables and weather - every max battle and tappable is
   sent twice, as Golbat re-sends them - and check the counter endpoints count
   each one exactly once.
2. Weather maths on hand-made rows (area 9999, yesterday) with known answers:
     cell A: CLEAR from 10:00, RAINY from 12:30
     cell B: FOG from the evening before (carried in), CLEAR from 11:00
   window 10:00-14:00 -> CLEAR 5.5, RAINY 1.5, FOG 1.0 cell-hours (2 cells x 4 h)
3. The weather endpoints answer for a real area and refuse 'global' for cells.
4. A weather cell straddling two areas (centre in one, a sliver in the other)
   is stored for both.

  python test/check_maxbattle_tappable_weather.py      (stack up)
"""
import asyncio
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))
os.chdir(HERE)   # no repo .env (see check_counter_retrieval.py)
os.environ.update(DB_HOST="127.0.0.1", DB_PORT="13306", DB_USER="psyduck", DB_PASSWORD="psyduck",
                  DB_NAME="PsyduckV2", LOG_FILE="False", LOG_LEVEL="WARNING")

import harness as h  # noqa: E402
from feeder import Feeder  # noqa: E402

BASE = "http://127.0.0.1:15050"


def get(path, **params):
    url = f"{BASE}{path}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:200]


def per_area(body):
    """Counter endpoints return one result, or {area: result} for several areas."""
    if isinstance(body, dict) and "mode" in body:
        return [body]
    return [v for v in body.values() if isinstance(v, dict) and "data" in v]


async def feed() -> dict:
    f = Feeder(h.WEBHOOK_URL, rate=200, mode="exact", seed=17,
               mix={"pokemon": 0.6, "max_battle": 0.15, "tappable": 0.15, "weather": 0.1})
    await f.run(15)
    await f.drain(60)
    await f.close()
    return f.summary()["expected"]


async def weather_maths() -> list[str]:
    import sql.connect_db as db
    from sql.queries.weather_gets import fetch_weather_summary, fetch_weather_cells
    await db.init_db()
    y = (datetime.utcnow() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = [  # (cell, condition, seen_at)
        (111, 1, y.replace(hour=10)), (111, 2, y.replace(hour=12, minute=30)),
        (222, 7, y - timedelta(hours=4)), (222, 1, y.replace(hour=11)),
    ]
    await db.execute("DELETE FROM weather_daily_events WHERE area_id = 9999")
    for cell, cond, t in rows:
        await db.execute(
            "INSERT INTO weather_daily_events VALUES (%s, 9999, %s, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38.7, -9.1, %s, %s)",
            (cell, cond, t, t.date()))
    start, end = y.replace(hour=10), y.replace(hour=14)
    s = await fetch_weather_summary(area_ids=[9999], offsets={}, start=start, end=end, interval="day", mode="sum")
    g = await fetch_weather_summary(area_ids=[9999], offsets={}, start=start, end=end, interval="day", mode="grouped")
    c = await fetch_weather_cells(area_id=9999, offset=0, start=start, end=end)
    await db.execute("DELETE FROM weather_daily_events WHERE area_id = 9999")
    await db.close_db()

    errors = []
    day = s["data"].get(y.strftime("%Y-%m-%d"), {})
    want = {"CLEAR": 5.5, "RAINY": 1.5, "FOG": 1.0}
    got = {k: v["cell_hours"] for k, v in day.items() if k != "changes"}
    if got != want:
        errors.append(f"sum cell_hours {got} != {want}")
    # A: CLEAR is its first row (no change), RAINY is one; B: FOG -> CLEAR is one
    if day.get("changes") != 2:
        errors.append(f"sum changes {day.get('changes')} != 2")
    if s["cells"] != 2:
        errors.append(f"cells {s['cells']} != 2")
    h12 = g["data"].get(y.replace(hour=12).strftime("%Y-%m-%d %H:00"), {})
    if (h12.get("dominant"), h12.get("CLEAR"), h12.get("RAINY")) != ("CLEAR", 1.5, 0.5):
        errors.append(f"grouped 12:00 {h12}")
    a = c["data"].get("111", {})
    if a.get("hourly", {}).get(y.replace(hour=12).strftime("%Y-%m-%d %H:00")) != {"CLEAR": 0.5, "RAINY": 0.5}:
        errors.append(f"cell 111 hour 12 {a.get('hourly')}")
    if c["data"].get("222", {}).get("summary", {}).get("FOG", {}).get("hours") != 1.0:
        errors.append(f"cell 222 summary {c['data'].get('222', {}).get('summary')}")
    return errors


def send_straddling_cell() -> int:
    from areas import AREAS
    a1 = AREAS[0]
    edge, lat = a1["lon1"], (a1["lat0"] + a1["lat1"]) / 2
    cell = 987654321012345
    body = json.dumps([{"type": "weather", "message": {
        "s2_cell_id": cell, "gameplay_condition": 6, "updated": int(time.time()),
        "latitude": lat, "longitude": edge - 0.002,
        "polygon": [[lat - .001, edge - .005], [lat - .001, edge + .0002],
                    [lat + .001, edge + .0002], [lat + .001, edge - .005]]}}]).encode()
    urllib.request.urlopen(urllib.request.Request(h.WEBHOOK_URL, data=body, headers={"Content-Type": "application/json"}))
    return cell


def main() -> int:
    errors = []
    expected = asyncio.run(feed())
    straddling = send_straddling_cell()
    print(f"fed (each sent twice): {expected['max_battles']} max battles, {expected['tappables']} tappables, "
          f"{expected['weather']} weather changes")
    time.sleep(70)   # weather flush interval is 60 s

    code, mb = get("/api/redis/get_max_battles_counterseries", interval="daily", start_time="2 days", end_time="now")
    mb_total = sum(r["data"].get("total", 0) for r in per_area(mb)) if code == 200 else None
    print(f"max battles counted: {mb_total} (want {expected['max_battles']})")
    if mb_total != expected["max_battles"]:
        errors.append("max battle count")
    code, mbw = get("/api/redis/get_max_battles_counterseries", interval="weekly", mode="grouped",
                    start_time="14 days", end_time="now")
    if code != 200:
        errors.append(f"max battle weekly grouped HTTP {code}")

    code, tp = get("/api/redis/get_tappables_counterseries", interval="hourly", start_time="2 hours", end_time="now")
    tp_total = sum(r["data"].get("total", 0) for r in per_area(tp)) if code == 200 else None
    print(f"tappables counted: {tp_total} (want {expected['tappables']})")
    if tp_total != expected["tappables"]:
        errors.append("tappable count")
    for mode, interval in (("surged", "hourly"), ("grouped", "daily"), ("sum", "weekly")):
        code, _ = get("/api/redis/get_tappables_counterseries", interval=interval, mode=mode,
                      start_time="14 days", end_time="now")
        if code != 200:
            errors.append(f"tappables {mode}/{interval} HTTP {code}")

    areas_of_cell = sorted(r[0] for r in h.sql(
        "SELECT n.name FROM weather_daily_events w JOIN area_names n ON n.id = w.area_id WHERE w.s2_cell_id = %s",
        (straddling,)))
    print(f"straddling cell stored for: {areas_of_cell}")
    if areas_of_cell != ["TestArea01", "TestArea02"]:
        errors.append(f"straddling cell areas {areas_of_cell}")

    rows = int(h.sql_val("SELECT COUNT(*) FROM weather_daily_events WHERE s2_cell_id <> %s", (straddling,)) or 0)
    area = h.sql_val("SELECT n.name FROM weather_daily_events w JOIN area_names n ON n.id = w.area_id LIMIT 1")
    print(f"weather rows: {rows} (want >= {expected['weather']}), sample area {area}")
    if rows < expected["weather"]:
        errors.append("weather rows")
    for mode in ("sum", "grouped"):
        for a in (area, "global"):
            code, body = get("/api/sql/get_weather_data", area=a, start_time="1 day", end_time="now", mode=mode)
            if code != 200 or not body.get("data"):
                errors.append(f"weather_data {mode}/{a} HTTP {code} {str(body)[:120]}")
    code, body = get("/api/sql/get_weather_cells", area=area, start_time="1 day", end_time="now")
    if code != 200 or not body.get("cells"):
        errors.append(f"weather_cells HTTP {code} {str(body)[:120]}")
    code, _ = get("/api/sql/get_weather_cells", area="global", start_time="1 day", end_time="now")
    if code != 400:
        errors.append(f"weather_cells global should be 400, got {code}")

    errors += asyncio.run(weather_maths())
    for e in errors:
        print("  FAIL:", e)
    print("RESULT:", "PASS" if not errors else "FAIL")
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
