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
6. Geography through the whole path: encounters with `geography` posted to /webhook land on their spawnpoints
   rows after a flush; re-sending the same values changes nothing; a changed geography writes a
   spawnpoint_geography_changes row (old = stored, new = sent) and sets geo_changed_at.
7. /api/s2biomes/geography returns exactly the stored spawnpoints with geography (synthetic rows force more
   than one keyset page) and /api/s2biomes/geography_changes exactly the stored change rows, also with since.

The inserted rows are deleted again at the end.

  python test/check_s2biomes_endpoints.py      (stack up)
"""
import gzip
import json
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.chdir(HERE)

import harness as h  # noqa: E402
import pymysql  # noqa: E402
from feeder import Feeder  # noqa: E402

BASE = "http://127.0.0.1:15050"
AREA = 9999
SP_BASE = 900_000_000_000_000
SPAWNPOINTS = 2000
PER_SPAWNPOINT = 12
PAGE = 5000
WEATHER_CELLS = 40
SPAWN_COLUMNS = ("spawnpoint", "pokemon_id", "form", "seen_at", "encounter_id", "weather")
GEO = ("landform", "moisture", "landcover", "temperature", "features", "park_status", "settlement")
GEOGRAPHY_COLUMNS = ("spawnpoint", *GEO, "geo_first_seen", "geo_changed_at", "geo_seen_day")
CHANGE_COLUMNS = ("spawnpoint", "changed_at", *(f"old_{c}" for c in GEO), *(f"new_{c}" for c in GEO))
GEO_SP_BASE = SP_BASE + 10_000_000
GEO_SYNTHETIC = 60_000      # above the endpoint's 50,000-row keyset page
GEO_WEBHOOK = 40
GEO_BUFFER = "buffer:spawnpoint_geography"


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


def post_webhook(messages: list[dict]) -> int:
    body = json.dumps([{"type": "pokemon", "message": m} for m in messages]).encode()
    req = urllib.request.Request(h.WEBHOOK_URL, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status


def tsv_field(v) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def db_tsv(cur, sql, params=None) -> list[list[str]]:
    cur.execute(sql, params)
    return [[tsv_field(v) for v in row] for row in cur.fetchall()]


def api_tsv(path, columns, **params) -> tuple[int, list[list[str]]]:
    status, body = get(path, **params)
    if status != 200:
        return status, []
    lines = body.rstrip("\n").split("\n")
    if lines[0].split("\t") != list(columns):
        return -1, []
    return status, [line.split("\t") for line in lines[1:]]


def in_list(values) -> str:
    return ",".join(["%s"] * len(values))


def sent_geo(msg: dict) -> tuple:
    g = msg["geography"]
    return (g["landform"], g["moisture"], g["landcover"], g["temperature"], sum(1 << f for f in g["features"]),
            g["park_status"], g["settlement"])


def wait_geography_flushed(r, timeout=90) -> bool:
    """True once the webhook staging had time to land and no geography batch is buffered or mid-flush."""
    time.sleep(3)
    t0 = time.time()
    while time.time() - t0 < timeout:
        if not list(r.scan_iter(match=GEO_BUFFER + "*", count=1000)):
            return True
        time.sleep(1)
    return False


def geo_rows(cur, sps) -> dict:
    cur.execute(f"SELECT spawnpoint, {', '.join(GEO)}, geo_first_seen, geo_changed_at, geo_seen_day "
                f"FROM spawnpoints WHERE spawnpoint IN ({in_list(sps)})", list(sps))
    return {row[0]: row[1:] for row in cur.fetchall()}


def webhook_spawnpoints() -> tuple[list[dict], list[int]]:
    feeder = Feeder(seed=7)
    msgs = [feeder.pokemon("encounter") for _ in range(GEO_WEBHOOK)]
    return msgs, [int(m["spawnpoint_id"], 16) for m in msgs]


def check_geography(cur, errors) -> None:
    r = h.rds()
    msgs, sps = webhook_spawnpoints()
    want = {sp: sent_geo(m) for sp, m in zip(sps, msgs)}
    try:
        # 6a. first sighting
        if post_webhook(msgs) != 200 or not wait_geography_flushed(r):
            errors.append("geography: webhook POST or flush failed")
            return
        first = geo_rows(cur, sps)
        got = {sp: row[:7] for sp, row in first.items()}
        print(f"geography via webhook: {sum(got.get(sp) == want[sp] for sp in sps)}/{len(sps)} spawnpoints stored")
        if got != want:
            errors.append("geography: stored values differ from the sent ones")
        if any(row[7] is None or row[8] is not None or row[9] is None for row in first.values()):
            errors.append("geography: geo_first_seen / geo_seen_day missing or geo_changed_at set on first sighting")
        cur.execute(f"SELECT COUNT(*) FROM spawnpoint_geography_changes WHERE spawnpoint IN ({in_list(sps)})", sps)
        if cur.fetchone()[0]:
            errors.append("geography: change rows written on first sighting")

        # 6b. same values again: nothing changes
        time.sleep(1.1)
        if post_webhook(msgs) != 200 or not wait_geography_flushed(r):
            errors.append("geography: re-send POST or flush failed")
            return
        again = geo_rows(cur, sps)
        print(f"same geography re-sent: {sum(again[sp] == first[sp] for sp in sps)}/{len(sps)} rows unchanged")
        if again != first:
            errors.append("geography: re-sending the same values changed rows")

        # 6c. one spawnpoint changes
        g0 = msgs[0]["geography"]
        changed = dict(msgs[0], geography=dict(g0, temperature=(g0["temperature"] + 1) % 7, features=[2, 5]))
        time.sleep(1.1)
        if post_webhook([changed]) != 200 or not wait_geography_flushed(r):
            errors.append("geography: change POST or flush failed")
            return
        after = geo_rows(cur, sps)
        cur.execute(f"SELECT {', '.join(CHANGE_COLUMNS)} FROM spawnpoint_geography_changes "
                    f"WHERE spawnpoint IN ({in_list(sps)})", sps)
        change_rows = cur.fetchall()
        sp0 = sps[0]
        print(f"changed geography: {len(change_rows)} change row(s) (want 1)")
        if [row[0] for row in change_rows] != [sp0]:
            errors.append(f"geography: want exactly one change row for {sp0}, got {change_rows}")
        else:
            row = change_rows[0]
            if tuple(row[2:9]) != want[sp0] or tuple(row[9:16]) != sent_geo(changed):
                errors.append(f"geography: change row old/new values wrong: {row}")
            if after[sp0][:7] != sent_geo(changed) or after[sp0][8] != row[1] or after[sp0][7] != first[sp0][7]:
                errors.append(f"geography: spawnpoint row after change wrong: {after[sp0]}")
        if any(after[sp] != first[sp] for sp in sps[1:]):
            errors.append("geography: unchanged spawnpoints were rewritten")
    finally:
        r.close()


def check_geography_endpoints(cur, errors) -> None:
    rnd = random.Random(11)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    rows, changes = [], []
    for i in range(GEO_SYNTHETIC):
        sp = GEO_SP_BASE + i
        if i % 10 == 9:      # no geography: must not be exported
            rows.append((sp, 38.7, -9.1, *([None] * 7), None, None, None))
            continue
        first = now - timedelta(days=rnd.randrange(1, 30), seconds=rnd.randrange(86400))
        changed = first + timedelta(hours=rnd.randrange(1, 48)) if i % 7 == 0 else None
        rows.append((sp, 38.7, -9.1, rnd.randrange(5), rnd.randrange(4), rnd.randrange(9), rnd.randrange(7),
                     rnd.randrange(128), rnd.randrange(3), rnd.randrange(3), first, changed, now.date()))
        if changed and len(changes) < 30:
            changes.append((sp, changed, *(rnd.randrange(5) for _ in GEO), *rows[-1][3:10]))
    for i in range(0, len(rows), 5000):
        cur.executemany(f"INSERT INTO spawnpoints (spawnpoint, latitude, longitude, {', '.join(GEO)}, geo_first_seen, "
                        f"geo_changed_at, geo_seen_day) VALUES ({in_list(range(13))})", rows[i:i + 5000])
    cur.executemany(f"INSERT INTO spawnpoint_geography_changes ({', '.join(CHANGE_COLUMNS)}) "
                    f"VALUES ({in_list(CHANGE_COLUMNS)})", changes)
    print(f"inserted {len(rows):,} synthetic spawnpoints and {len(changes)} change rows")

    status, api = api_tsv("/api/s2biomes/geography", GEOGRAPHY_COLUMNS)
    db = db_tsv(cur, f"SELECT {', '.join(GEOGRAPHY_COLUMNS)} FROM spawnpoints WHERE temperature IS NOT NULL "
                     "ORDER BY spawnpoint")
    ours = sum(1 for row in api if int(row[0]) >= GEO_SP_BASE)
    print(f"geography endpoint: {len(api):,} rows, {ours:,} synthetic (want {len(db):,} / "
          f"{sum(1 for r in rows if r[6] is not None):,})")
    if status != 200 or api != db:
        errors.append(f"geography endpoint differs from the stored rows (HTTP {status})")

    status, api = api_tsv("/api/s2biomes/geography_changes", CHANGE_COLUMNS)
    db = db_tsv(cur, f"SELECT {', '.join(CHANGE_COLUMNS)} FROM spawnpoint_geography_changes "
                     "ORDER BY changed_at, spawnpoint")
    print(f"geography_changes endpoint: {len(api)} rows (want {len(db)})")
    if status != 200 or api != db:
        errors.append(f"geography_changes endpoint differs from the stored rows (HTTP {status})")

    since = sorted(c[1] for c in changes)[len(changes) // 2]
    status, api = api_tsv("/api/s2biomes/geography_changes", CHANGE_COLUMNS, since=since.strftime("%Y-%m-%d %H:%M:%S"))
    db = db_tsv(cur, f"SELECT {', '.join(CHANGE_COLUMNS)} FROM spawnpoint_geography_changes WHERE changed_at >= %s "
                     "ORDER BY changed_at, spawnpoint", (since,))
    print(f"geography_changes since {since}: {len(api)} rows (want {len(db)})")
    if status != 200 or api != db or not db or db[0][1] != tsv_field(since):
        errors.append(f"geography_changes since= differs from the stored rows (HTTP {status})")


def cleanup_geography(cur):
    _, sps = webhook_spawnpoints()
    for table in ("spawnpoints", "spawnpoint_geography_changes"):
        cur.execute(f"DELETE FROM {table} WHERE spawnpoint >= %s", (GEO_SP_BASE,))
        cur.execute(f"DELETE FROM {table} WHERE spawnpoint IN ({in_list(sps)})", sps)


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
    cleanup_geography(cur)
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

    # 6. geography through the webhook, 7. geography endpoints
    check_geography(cur, errors)
    check_geography_endpoints(cur, errors)

    cleanup(cur, (yesterday, today))
    cleanup_geography(cur)
    conn.close()
    h.write_result("check_s2biomes_endpoints", {"errors": errors, "pages": pages, "rows": len(ours)})
    for e in errors:
        print("  FAIL:", e)
    print("RESULT:", "PASS" if not errors else "FAIL")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
