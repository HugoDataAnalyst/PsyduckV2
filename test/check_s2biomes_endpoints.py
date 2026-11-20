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
6. A spawnpoint's S2 level-16 cell: utils.s2cell.spawnpoint_l16 gives a pair known from s2sphere, and the SQL
   formula of the migration gives the same as Python on every face.
   Geography through the whole path: encounters with `geography` posted to /webhook land in cell_geography after
   a flush, the spawnpoints of one cell as one row; re-sending the same values changes nothing; a changed
   geography (from another spawnpoint of the cell) writes a cell_geography_changes row (old = stored,
   new = sent) and sets geo_changed_at.
7. /api/s2biomes/geography returns exactly the stored cells with geography (synthetic rows on both sides of
   2**63 force more than one keyset page) and /api/s2biomes/geography_changes exactly the stored change rows,
   also with since.
8. The cell_geography migration carries per-spawnpoint geography over: downgrade one step, insert old-shape
   spawnpoints rows and spawnpoint_geography_changes rows, upgrade, compare cell_geography and
   cell_geography_changes with the expected rows per cell; the old columns and table are gone.

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
sys.path.insert(1, os.path.dirname(HERE))
os.chdir(HERE)

import harness as h  # noqa: E402
import pymysql  # noqa: E402
from feeder import Feeder  # noqa: E402
from utils.s2cell import spawnpoint_l16  # noqa: E402

BASE = "http://127.0.0.1:15050"
AREA = 9999
SP_BASE = 900_000_000_000_000
SPAWNPOINTS = 2000
PER_SPAWNPOINT = 12
PAGE = 5000
WEATHER_CELLS = 40
SPAWN_COLUMNS = ("spawnpoint", "pokemon_id", "form", "seen_at", "encounter_id", "weather")
GEO = ("landform", "moisture", "landcover", "temperature", "features", "park_status", "settlement")
GEOGRAPHY_COLUMNS = ("l16", *GEO, "geo_first_seen", "geo_changed_at", "geo_seen_day")
CHANGE_COLUMNS = ("l16", "changed_at", *(f"old_{c}" for c in GEO), *(f"new_{c}" for c in GEO))
GEO_BUFFER = "buffer:cell_geography"
# Test level-16 cells: position bits (l16 bits 29-60) from these values on, apart from anything else
SYNTHETIC_CELL0 = 0xA000_0000
WEBHOOK_CELL0 = 0xB000_0000
MIGRATION_CELL0 = 0xB100_0000
GEO_SYNTHETIC = 60_000      # above the endpoint's 50,000-row keyset page
WEBHOOK_CELLS, PER_CELL = 10, 4
# spawnpoint 0d19338497b (Lisbon) -> level-16 cell 0d1933849, from s2sphere CellId.parent(16)
KNOWN_PAIR = (900118104443, 943842241355448320)
L16_SQL = "((%s << 20) & ~((1 << 28) - 1)) | (1 << 28)"  # as in the cell_geography migration


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


def cell_rows(cur, cells) -> dict:
    cur.execute(f"SELECT l16, {', '.join(GEO)}, geo_first_seen, geo_changed_at, geo_seen_day "
                f"FROM cell_geography WHERE l16 IN ({in_list(cells)})", list(cells))
    return {row[0]: row[1:] for row in cur.fetchall()}


def change_rows(cur, cells) -> list[tuple]:
    cur.execute(f"SELECT {', '.join(CHANGE_COLUMNS)} FROM cell_geography_changes WHERE l16 IN ({in_list(cells)}) "
                "ORDER BY changed_at, l16", list(cells))
    return [tuple(row) for row in cur.fetchall()]


def test_spawnpoint(face: int, cell: int, k: int) -> int:
    """Spawnpoint k of a test level-16 cell: spawnpoint bits 9+ (face, position) name the cell, bits 0-8 the
    level-20 cell inside it (lsb set)."""
    return (face << 41) | (cell << 9) | (k << 1) | 1


def webhook_messages() -> list[dict]:
    """PER_CELL encounters in each of WEBHOOK_CELLS level-16 cells (face 5: l16 above 2**63), each carrying its
    cell's geography."""
    feeder = Feeder(seed=7)
    msgs = []
    for c in range(WEBHOOK_CELLS):
        for k in range(PER_CELL):
            sp = f"{test_spawnpoint(5, WEBHOOK_CELL0 + c, k):011x}"
            msgs.append(dict(feeder.pokemon("encounter"), spawnpoint_id=sp, geography=Feeder.geography(sp)))
    return msgs


def check_l16(cur, msgs, errors) -> None:
    sp, l16 = KNOWN_PAIR
    print(f"l16 of spawnpoint {sp:011x}: {spawnpoint_l16(sp)} (s2sphere: {l16})")
    if spawnpoint_l16(sp) != l16:
        errors.append(f"spawnpoint_l16({sp}) = {spawnpoint_l16(sp)}, want {l16}")
    rnd = random.Random(5)
    sample = [sp, *(int(m["spawnpoint_id"], 16) for m in msgs),
              *(test_spawnpoint(f, rnd.getrandbits(32), rnd.getrandbits(7)) for f in range(6) for _ in range(5))]
    cur.execute("SELECT " + ", ".join([L16_SQL] * len(sample)), sample)
    got = list(cur.fetchone())
    want = [spawnpoint_l16(s) for s in sample]
    print(f"l16 in SQL = Python: {sum(g == w for g, w in zip(got, want))}/{len(sample)}")
    if got != want:
        errors.append("l16 computed in SQL differs from utils.s2cell.spawnpoint_l16")


def check_geography(cur, errors) -> None:
    r = h.rds()
    msgs = webhook_messages()
    check_l16(cur, msgs, errors)
    want = {spawnpoint_l16(int(m["spawnpoint_id"], 16)): sent_geo(m) for m in msgs}
    cells = list(want)
    try:
        # 6a. first sighting: the spawnpoints of a cell collapse to one row
        if post_webhook(msgs) != 200 or not wait_geography_flushed(r):
            errors.append("geography: webhook POST or flush failed")
            return
        first = cell_rows(cur, cells)
        got = {l16: row[:7] for l16, row in first.items()}
        print(f"geography via webhook: {len(msgs)} spawnpoints -> {len(first)} cell rows (want {len(cells)}), "
              f"{sum(got.get(c) == want[c] for c in cells)}/{len(cells)} with the sent values")
        if got != want:
            errors.append("geography: stored cells differ from the sent ones")
        if any(row[7] is None or row[8] is not None or row[9] is None for row in first.values()):
            errors.append("geography: geo_first_seen / geo_seen_day missing or geo_changed_at set on first sighting")
        if change_rows(cur, cells):
            errors.append("geography: change rows written on first sighting")

        # 6b. same values again: nothing changes
        time.sleep(1.1)
        if post_webhook(msgs) != 200 or not wait_geography_flushed(r):
            errors.append("geography: re-send POST or flush failed")
            return
        again = cell_rows(cur, cells)
        print(f"same geography re-sent: {sum(again[c] == first[c] for c in cells)}/{len(cells)} cells unchanged")
        if again != first:
            errors.append("geography: re-sending the same values changed rows")

        # 6c. one cell changes, seen from a spawnpoint of it not sent before
        c0 = cells[0]
        g0 = msgs[0]["geography"]
        sp = f"{test_spawnpoint(5, WEBHOOK_CELL0, PER_CELL):011x}"
        changed = dict(msgs[0], spawnpoint_id=sp,
                       geography=dict(g0, temperature=(g0["temperature"] + 1) % 7, features=[2, 5]))
        time.sleep(1.1)
        if post_webhook([changed]) != 200 or not wait_geography_flushed(r):
            errors.append("geography: change POST or flush failed")
            return
        after = cell_rows(cur, cells)
        changes = change_rows(cur, cells)
        print(f"changed geography: {len(changes)} change row(s) (want 1)")
        if [row[0] for row in changes] != [c0]:
            errors.append(f"geography: want exactly one change row for cell {c0}, got {changes}")
        else:
            row = changes[0]
            if row[2:9] != want[c0] or row[9:16] != sent_geo(changed):
                errors.append(f"geography: change row old/new values wrong: {row}")
            if after[c0][:7] != sent_geo(changed) or after[c0][8] != row[1] or after[c0][7] != first[c0][7]:
                errors.append(f"geography: cell row after change wrong: {after[c0]}")
        if any(after[c] != first[c] for c in cells[1:]):
            errors.append("geography: unchanged cells were rewritten")
    finally:
        r.close()


def synthetic_cells() -> list[int]:
    """GEO_SYNTHETIC level-16 ids, half on face 3 (below 2**63) and half on face 4 (above)."""
    return [((3 if i < GEO_SYNTHETIC // 2 else 4) << 61) | ((SYNTHETIC_CELL0 + i) << 29) | (1 << 28)
            for i in range(GEO_SYNTHETIC)]


def check_geography_endpoints(cur, errors) -> None:
    rnd = random.Random(11)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    cells = synthetic_cells()
    rows, changes = [], []
    for i, l16 in enumerate(cells):
        if i % 10 == 9:      # no temperature: must not be exported
            rows.append((l16, *([None] * 7), now, None, now.date()))
            continue
        first = now - timedelta(days=rnd.randrange(1, 30), seconds=rnd.randrange(86400))
        changed = first + timedelta(hours=rnd.randrange(1, 48)) if i % 7 == 0 else None
        rows.append((l16, rnd.randrange(5), rnd.randrange(4), rnd.randrange(9), rnd.randrange(7),
                     rnd.randrange(128), rnd.randrange(3), rnd.randrange(3), first, changed, now.date()))
        if changed and (len(changes) < 30 or i >= GEO_SYNTHETIC // 2 and len(changes) < 60):
            changes.append((l16, changed, *(rnd.randrange(5) for _ in GEO), *rows[-1][1:8]))
    for i in range(0, len(rows), 5000):
        cur.executemany(f"INSERT INTO cell_geography (l16, {', '.join(GEO)}, geo_first_seen, geo_changed_at, "
                        f"geo_seen_day) VALUES ({in_list(range(11))})", rows[i:i + 5000])
    cur.executemany(f"INSERT INTO cell_geography_changes ({', '.join(CHANGE_COLUMNS)}) "
                    f"VALUES ({in_list(CHANGE_COLUMNS)})", changes)
    print(f"inserted {len(rows):,} synthetic cells and {len(changes)} change rows")

    status, api = api_tsv("/api/s2biomes/geography", GEOGRAPHY_COLUMNS)
    db = db_tsv(cur, f"SELECT {', '.join(GEOGRAPHY_COLUMNS)} FROM cell_geography WHERE temperature IS NOT NULL "
                     "ORDER BY l16")
    synthetic = set(cells)
    print(f"geography endpoint: {len(api):,} rows, {sum(int(row[0]) in synthetic for row in api):,} synthetic "
          f"(want {len(db):,} / {sum(1 for r in rows if r[4] is not None):,})")
    if status != 200 or api != db:
        errors.append(f"geography endpoint differs from the stored rows (HTTP {status})")

    status, api = api_tsv("/api/s2biomes/geography_changes", CHANGE_COLUMNS)
    db = db_tsv(cur, f"SELECT {', '.join(CHANGE_COLUMNS)} FROM cell_geography_changes ORDER BY changed_at, l16")
    print(f"geography_changes endpoint: {len(api)} rows (want {len(db)})")
    if status != 200 or api != db:
        errors.append(f"geography_changes endpoint differs from the stored rows (HTTP {status})")

    since = sorted(c[1] for c in changes)[len(changes) // 2]
    status, api = api_tsv("/api/s2biomes/geography_changes", CHANGE_COLUMNS, since=since.strftime("%Y-%m-%d %H:%M:%S"))
    db = db_tsv(cur, f"SELECT {', '.join(CHANGE_COLUMNS)} FROM cell_geography_changes WHERE changed_at >= %s "
                     "ORDER BY changed_at, l16", (since,))
    print(f"geography_changes since {since}: {len(api)} rows (want {len(db)})")
    if status != 200 or api != db or not db or db[0][1] != tsv_field(since):
        errors.append(f"geography_changes since= differs from the stored rows (HTTP {status})")


def alembic(*args) -> tuple[int, str]:
    p = h.compose("exec", "-T", "app", "alembic", *args, capture=True, check=False)
    return p.returncode, (p.stdout + p.stderr).strip()


def has_column(cur, table, column) -> bool:
    cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() "
                "AND table_name = %s AND column_name = %s", (table, column))
    return bool(cur.fetchone()[0])


def has_table(cur, table) -> bool:
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() "
                "AND table_name = %s", (table,))
    return bool(cur.fetchone()[0])


def migration_cells() -> dict:
    """Old-shape spawnpoints rows (spawnpoint, 7 geography values, geo_first_seen, geo_seen_day, geo_changed_at)
    per test cell, and the cell_geography row each must become after the upgrade (None: no row)."""
    d = datetime
    a, b, c, e = (test_spawnpoint(f, MIGRATION_CELL0 + i, 0) - 1 for i, f in enumerate((2, 0, 1, 5)))
    va0, va1, va2, vb, ve0, ve1 = ((i % 5, i % 4, i % 9, i % 7, i * 3 % 128, i % 3, (i + 1) % 3) for i in range(1, 7))
    return {
        "A": ([(a | 1, *va0, d(2026, 11, 1, 10), date(2026, 11, 10), None),
               (a | 3, *va1, d(2026, 10, 28, 8), date(2026, 11, 15), d(2026, 11, 12, 9)),   # seen last: its values
               (a | 5, *va2, d(2026, 11, 5), date(2026, 11, 12), d(2026, 11, 14, 7))],
              (*va1, d(2026, 10, 28, 8), d(2026, 11, 14, 7), date(2026, 11, 15))),
        "B": ([(b | 1, *vb, d(2026, 11, 2, 12), date(2026, 11, 3), None)],
              (*vb, d(2026, 11, 2, 12), None, date(2026, 11, 3))),
        "C": ([(c | 1, *([None] * 7), None, None, None)], None),                            # no geography
        "E": ([(e | 1, *ve0, d(2026, 11, 1), date(2026, 11, 9), None),
               (e | 3, *ve1, d(2026, 11, 3), date(2026, 11, 9), None)],                    # same day: later first seen
              (*ve1, d(2026, 11, 1), None, date(2026, 11, 9))),
    }


def check_migration(cur, errors) -> None:
    """Downgrade one step, write per-spawnpoint geography in the old shape, upgrade: cell_geography and
    cell_geography_changes hold it per cell, the old columns and table are gone."""
    code, out = alembic("downgrade", "-1")
    print(f"alembic downgrade -1: exit {code}")
    if code or not has_column(cur, "spawnpoints", "temperature") or not has_table(cur, "spawnpoint_geography_changes") \
            or has_table(cur, "cell_geography"):
        errors.append(f"migration: downgrade did not restore the old shape: {out[-1500:]}")
        return
    cells = migration_cells()
    sp_rows = [row for rows, _ in cells.values() for row in rows]
    cur.executemany(f"INSERT INTO spawnpoints (spawnpoint, latitude, longitude, {', '.join(GEO)}, geo_first_seen, "
                    f"geo_seen_day, geo_changed_at) VALUES (%s, 38.7, -9.1, {in_list(range(10))})", sp_rows)
    (a0, a1, a2), (b0,) = [r[0] for r in cells["A"][0]], [r[0] for r in cells["B"][0]]
    v = [tuple(range(i, i + 7)) for i in range(4)]
    old_changes = [(a0, datetime(2026, 11, 12, 9), *v[0], *v[1]),
                   (a1, datetime(2026, 11, 12, 9), *v[2], *v[3]),   # same cell and time as a0's: only a0's is kept
                   (a2, datetime(2026, 11, 14, 7), *v[1], *v[2]),
                   (b0, datetime(2026, 11, 2, 12), *v[3], *v[0])]
    cur.executemany("INSERT INTO spawnpoint_geography_changes (spawnpoint, changed_at, "
                    f"{', '.join(f'old_{c}' for c in GEO)}, {', '.join(f'new_{c}' for c in GEO)}) "
                    f"VALUES ({in_list(range(16))})", old_changes)
    print(f"old shape: {len(sp_rows)} spawnpoints rows in {len(cells)} cells, {len(old_changes)} change rows")

    code, out = alembic("upgrade", "head")
    print(f"alembic upgrade head: exit {code}")
    if code:
        errors.append(f"migration: upgrade failed: {out[-1500:]}")
        return
    l16s = {name: spawnpoint_l16(rows[0][0]) for name, (rows, _) in cells.items()}
    got = cell_rows(cur, list(l16s.values()))
    want = {l16s[name]: row for name, (_, row) in cells.items() if row}
    print(f"carried over: {len(got)} cell rows (want {len(want)}), "
          f"{sum(got.get(c) == w for c, w in want.items())}/{len(want)} exact")
    if got != want:
        errors.append(f"migration: cell_geography after upgrade {got}, want {want}")
    got_changes = change_rows(cur, list(l16s.values()))
    want_changes = sorted(((spawnpoint_l16(c[0]), *c[1:]) for i, c in enumerate(old_changes) if i != 1),
                          key=lambda c: (c[1], c[0]))
    print(f"carried over: {len(got_changes)} change rows (want {len(want_changes)})")
    if got_changes != want_changes:
        errors.append(f"migration: cell_geography_changes after upgrade {got_changes}, want {want_changes}")
    gone = not has_column(cur, "spawnpoints", "temperature") and not has_column(cur, "spawnpoints", "geo_seen_day") \
        and not has_table(cur, "spawnpoint_geography_changes")
    print(f"spawnpoints geography columns and spawnpoint_geography_changes dropped: {gone}")
    if not gone:
        errors.append("migration: old geography columns or spawnpoint_geography_changes left behind")
    cur.execute(f"DELETE FROM spawnpoints WHERE spawnpoint IN ({in_list(sp_rows)})", [r[0] for r in sp_rows])


def cleanup_geography(cur):
    cells = synthetic_cells()
    cells += [spawnpoint_l16(test_spawnpoint(5, WEBHOOK_CELL0 + c, 0)) for c in range(WEBHOOK_CELLS)]
    cells += [spawnpoint_l16(rows[0][0]) for rows, _ in migration_cells().values()]
    for i in range(0, len(cells), 5000):
        chunk = cells[i:i + 5000]
        for table in ("cell_geography", "cell_geography_changes"):
            cur.execute(f"DELETE FROM {table} WHERE l16 IN ({in_list(chunk)})", chunk)
    sps = [int(m["spawnpoint_id"], 16) for m in webhook_messages()] + [test_spawnpoint(5, WEBHOOK_CELL0, PER_CELL)]
    cur.execute(f"DELETE FROM spawnpoints WHERE spawnpoint IN ({in_list(sps)})", sps)


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

    # 6. l16 and geography through the webhook, 7. geography endpoints, 8. migration carry-over
    check_geography(cur, errors)
    check_geography_endpoints(cur, errors)
    cleanup_geography(cur)
    check_migration(cur, errors)

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
