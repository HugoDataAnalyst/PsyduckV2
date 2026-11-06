"""
The S2Biomes endpoints return exactly the stored rows, on the test stack.

Inserts synthetic spawns (area 9999, spawnpoints above SP_BASE so nothing else collides) into yesterday's
and today's partitions, plus weather rows for yesterday, then checks:

1. /api/s2biomes/spawns paged with a small limit gives back every inserted row of yesterday, once, in
   primary-key order, with the same values (spawnpoint, pokemon_id, form, seen_at, encounter_id, weather).
2. since / until on today returns exactly the rows inside the window.
3. /api/s2biomes/weather returns yesterday's inserted weather rows.
4. /api/s2biomes/days lists both days.
5. A bad cursor, a limit out of range and an unknown path are refused (400 / 422 / 403).

The inserted rows are deleted again at the end.

  python test/check_s2biomes_endpoints.py      (stack up)
"""
import gzip
import json
import os
import random
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.chdir(HERE)

import harness as h  # noqa: E402
import pymysql  # noqa: E402

BASE = "http://127.0.0.1:15050"
AREA = 9999
SP_BASE = 900_000_000_000_000
SPAWNPOINTS = 2000
PER_SPAWNPOINT = 12
PAGE = 5000
WEATHER_CELLS = 40
SPAWN_COLUMNS = ("spawnpoint", "pokemon_id", "form", "seen_at", "encounter_id", "weather")


def get(path, **params):
    url = f"{BASE}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            body = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            return r.status, body.decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:200]


def tsv_rows(text):
    lines = text.rstrip("\n").split("\n")
    header = lines[0].split("\t")
    return [dict(zip(header, line.split("\t"))) for line in lines[1:]]


def spawn_key(r):
    return (int(r["spawnpoint"]), r["seen_at"])


def make_spawns(day: date, rnd: random.Random) -> list[tuple]:
    rows = []
    for i in range(SPAWNPOINTS):
        sp = SP_BASE + i * 7
        minutes = sorted(rnd.sample(range(24 * 60), PER_SPAWNPOINT))
        for m in minutes:
            seen = datetime.combine(day, datetime.min.time()) + timedelta(minutes=m, seconds=rnd.randrange(60))
            rows.append((sp, rnd.randrange(1, 1026), str(rnd.choice([0, 0, 0, 163, 2985])), rnd.randrange(101),
                         rnd.randrange(1, 36), AREA, seen, day, rnd.getrandbits(63), rnd.choice([None, 0, 1, 2, 3])))
    return rows


def as_api(row: tuple) -> dict:
    sp, pid, form, _iv, _lvl, _area, seen, _day, eid, weather = row
    return {"spawnpoint": str(sp), "pokemon_id": str(pid), "form": form, "seen_at": seen.strftime("%Y-%m-%d %H:%M:%S"),
            "encounter_id": str(eid), "weather": "" if weather is None else str(weather)}


def cleanup(cur, days):
    for d in days:
        cur.execute("DELETE FROM pokemon_iv_daily_events WHERE day_date = %s AND area_id = %s", (d, AREA))
        cur.execute("DELETE FROM weather_daily_events WHERE day_date = %s AND area_id = %s", (d, AREA))


def main() -> int:
    h.banner("S2Biomes endpoints: raw spawn and weather rows")
    rnd = random.Random(42)
    today = date.today()
    yesterday = today - timedelta(days=1)
    conn = pymysql.connect(host="127.0.0.1", port=13306, user="psyduck", password="psyduck",
                           database="PsyduckV2", autocommit=True)
    cur = conn.cursor()
    cleanup(cur, (yesterday, today))
    spawns = {d: make_spawns(d, rnd) for d in (yesterday, today)}
    for rows in spawns.values():
        for i in range(0, len(rows), 5000):
            cur.executemany(
                """INSERT INTO pokemon_iv_daily_events
                   (spawnpoint, pokemon_id, form, iv, level, area_id, seen_at, day_date, encounter_id, weather)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows[i:i + 5000])
    weather = []
    for c in range(WEATHER_CELLS):
        for hour in (3, 11, 19):
            weather.append((5_000_000_000_000_000_000 + c, AREA, rnd.randrange(1, 8), 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            38.7, -9.1, datetime.combine(yesterday, datetime.min.time()) + timedelta(hours=hour), yesterday))
    cur.executemany(
        """INSERT INTO weather_daily_events (s2_cell_id, area_id, gameplay_condition, warn_weather, severity,
           wind_direction, cloud_level, rain_level, wind_level, snow_level, fog_level, special_effect_level,
           latitude, longitude, seen_at, day_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", weather)
    cur.execute("ANALYZE TABLE pokemon_iv_daily_events, weather_daily_events")
    cur.fetchall()
    print(f"inserted {sum(map(len, spawns.values())):,} spawns and {len(weather)} weather rows (area {AREA})")

    errors = []

    # 1. paged day
    got, after, pages = [], None, 0
    while True:
        status, body = get("/api/s2biomes/spawns", day=yesterday, limit=PAGE, **({"after": after} if after else {}))
        if status != 200:
            errors.append(f"spawns page {pages + 1}: HTTP {status} {body}")
            break
        page = tsv_rows(body)
        pages += 1
        got += page
        if len(page) < PAGE:
            break
        after = f"{page[-1]['spawnpoint']},{page[-1]['seen_at']}"
    ours = [r for r in got if int(r["spawnpoint"]) >= SP_BASE]
    want = sorted((as_api(r) for r in spawns[yesterday]), key=spawn_key)
    print(f"spawns {yesterday}: {pages} pages, {len(ours):,} of our rows (want {len(want):,})")
    if [spawn_key(r) for r in got] != sorted(spawn_key(r) for r in got):
        errors.append("spawns are not in primary-key order")
    if len({spawn_key(r) for r in ours}) != len(ours):
        errors.append("a row came back twice across pages")
    if [{c: r[c] for c in SPAWN_COLUMNS} for r in ours] != want:
        errors.append("paged spawns differ from the inserted rows")

    # 2. since / until
    since = datetime.combine(today, datetime.min.time()) + timedelta(hours=10)
    until = since + timedelta(hours=2)
    status, body = get("/api/s2biomes/spawns", day=today, since=since.strftime("%Y-%m-%d %H:%M:%S"),
                       until=until.strftime("%Y-%m-%d %H:%M:%S"))
    window = [r for r in tsv_rows(body) if int(r["spawnpoint"]) >= SP_BASE] if status == 200 else []
    want_window = sorted((as_api(r) for r in spawns[today] if since <= r[6] < until), key=spawn_key)
    print(f"spawns {today} 10:00-12:00: {len(window):,} rows (want {len(want_window):,})")
    if status != 200 or [{c: r[c] for c in SPAWN_COLUMNS} for r in window] != want_window:
        errors.append(f"since/until window differs (HTTP {status})")

    # 3. weather
    status, body = get("/api/s2biomes/weather", day=yesterday)
    w = sorted((r["s2_cell_id"], r["gameplay_condition"], r["seen_at"])
               for r in tsv_rows(body) if status == 200 and r["area_id"] == str(AREA))
    want_w = sorted((str(r[0]), str(r[2]), r[14].strftime("%Y-%m-%d %H:%M:%S")) for r in weather)
    print(f"weather {yesterday}: {len(w)} rows (want {len(want_w)})")
    if w != want_w:
        errors.append(f"weather rows differ (HTTP {status})")

    # 4. days
    status, body = get("/api/s2biomes/days")
    listed = {d["day"] for d in json.loads(body)["spawns"]} if status == 200 else set()
    print(f"days listed: {sorted(listed)[-3:]}")
    if not {str(yesterday), str(today)} <= listed:
        errors.append(f"days endpoint misses {yesterday} or {today}")

    # 5. refusals
    for label, (path, params), want_status in (
        ("bad cursor", ("/api/s2biomes/spawns", {"day": yesterday, "after": "abc"}), 400),
        ("limit too big", ("/api/s2biomes/spawns", {"day": yesterday, "limit": 99_999_999}), 422),
        ("unknown path", ("/api/s2biomes/nope", {}), 403),
    ):
        status, _ = get(path, **params)
        print(f"{label}: HTTP {status} (want {want_status})")
        if status != want_status:
            errors.append(f"{label}: HTTP {status}, want {want_status}")

    cleanup(cur, (yesterday, today))
    conn.close()
    h.write_result("check_s2biomes_endpoints", {"errors": errors, "pages": pages, "rows": len(ours)})
    for e in errors:
        print("  FAIL:", e)
    print("RESULT:", "PASS" if not errors else "FAIL")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
