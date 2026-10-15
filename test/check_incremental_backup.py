"""
Incremental Redis -> MySQL backup must leave MySQL identical to Redis.

Seeds a separate Redis db (2) with ~20k keys for areas at UTC+10, UTC-10,
UTC+0 and one area missing from the geofences: hourly/daily/weekly/monthly
counters and timeseries series (some active, some idle). Then:

  1. full backup (since=None)                       -> MySQL == Redis
  2. live traffic: write to every bucket that is still open in each area's
     local time (current hour/day/week/month, active series)
  3. incremental backup (since = start of step 1)   -> MySQL == Redis again,
     with far fewer rows written

The UTC-10 area is the trap: its current local hour looks long closed if bucket
times are compared to UTC without the area's offset.

  python test/check_incremental_backup.py         (stops the test app while it runs)
"""
import asyncio
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.abspath(__file__)))   # no repo .env (see check_counter_retrieval.py)
os.environ.update(REDIS_HOST="127.0.0.1", REDIS_SERVER_PORT="16379", REDIS_PASSWORD="testpass", REDIS_DB="2",
                  DB_HOST="127.0.0.1", DB_PORT="13306", DB_USER="psyduck", DB_PASSWORD="psyduck",
                  DB_NAME="PsyduckV2", LOG_FILE="False", LOG_LEVEL="WARNING")

import harness as h  # noqa: E402
import sql.connect_db as db  # noqa: E402
from my_redis.connect_redis import RedisManager  # noqa: E402
from my_redis.utils.mysql_backup import MySQLBackup, _is_counter_key_recent  # noqa: E402
from server_fastapi import global_state  # noqa: E402

AREAS = {"IncrAreaPlus": 10, "IncrAreaMinus": -10, "IncrAreaZero": 0, "IncrAreaUnknown": None}
COUNTER_FAMILIES_H = ["pokemon_hourly", "raid_hourly"]
COUNTER_FAMILIES_D = ["pokemon_daily", "raid_daily"]
COUNTER_FAMILIES_W = ["pokemon_total", "raid_total"]


def local_now(area: str) -> datetime:
    off = AREAS[area] or 0
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=off)


def monday(d: datetime) -> datetime:
    return (d - timedelta(days=d.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)


def keys_for(area: str):
    now = local_now(area)
    for fam in COUNTER_FAMILIES_H:
        for hh in range(0, 24 * 40):
            yield f"counter:{fam}:{area}:{(now - timedelta(hours=hh)).strftime('%Y%m%d%H')}"
    for fam in COUNTER_FAMILIES_D:
        for dd in range(0, 200):
            yield f"counter:{fam}:{area}:{(now - timedelta(days=dd)).strftime('%Y%m%d')}"
    for fam in COUNTER_FAMILIES_W:
        for ww in range(0, 30):
            yield f"counter:{fam}:{area}:{(monday(now) - timedelta(weeks=ww)).strftime('%Y%m%d')}"
    for mm in range(0, 6):
        m = (now.replace(day=1) - timedelta(days=31 * mm)).strftime("%Y%m")
        for boost in ("0", "1"):
            yield f"counter:pokemon_weather_iv:{area}:{m}:{boost}"


def open_keys_for(area: str):
    """Keys live traffic writes to right now (current local bucket of each family)."""
    now = local_now(area)
    for fam in COUNTER_FAMILIES_H:
        yield f"counter:{fam}:{area}:{now.strftime('%Y%m%d%H')}"
    for fam in COUNTER_FAMILIES_D:
        yield f"counter:{fam}:{area}:{now.strftime('%Y%m%d')}"
    for fam in COUNTER_FAMILIES_W:
        yield f"counter:{fam}:{area}:{monday(now).strftime('%Y%m%d')}"
    yield f"counter:pokemon_weather_iv:{area}:{now.strftime('%Y%m')}:1"


def ts_series(area: str, n: int):
    return [f"ts:pokemon:total:{area}:{pid}:0" for pid in range(1, n + 1)]


async def seed(client, rng):
    await client.flushdb()
    n = 0
    async with client.pipeline(transaction=False) as pipe:
        for area in AREAS:
            for key in keys_for(area):
                pipe.hset(key, mapping={f"{rng.randint(1, 50)}:0:total": rng.randint(1, 99)})
                n += 1
            local_epoch = int(local_now(area).replace(tzinfo=timezone.utc).timestamp())
            for i, key in enumerate(ts_series(area, 3000)):
                # half the series went quiet 6-40h ago, half are active up to now
                newest = local_epoch - (rng.randint(6, 40) * 3600 if i % 2 else rng.randint(0, 600))
                fields = {str((newest - 60 * k) // 60 * 60): rng.randint(1, 5) for k in range(rng.randint(1, 30))}
                pipe.hset(key, mapping=fields)
                n += 1
        await pipe.execute()
    return n


async def live_traffic(client, rng):
    """What the webhook path does between two backups: only open buckets / active series change."""
    async with client.pipeline(transaction=False) as pipe:
        for area in AREAS:
            for key in open_keys_for(area):
                pipe.hincrby(key, "999:0:total", 7)
            minute = int(local_now(area).replace(tzinfo=timezone.utc).timestamp()) // 60 * 60
            for key in ts_series(area, 3000)[::2]:          # active ones
                pipe.hincrby(key, str(minute), 1)
        await pipe.execute()


async def mismatches(client) -> int:
    rows = {}
    for table in ("redis_counter_backup", "redis_timeseries_backup"):
        async for chunk in db.stream_rows(f"SELECT redis_key, hash_data FROM {table} WHERE redis_key LIKE %s",
                                          ("%IncrArea%",), batch=5000):
            for r in chunk:
                rows[r["redis_key"]] = json.loads(r["hash_data"])
    bad = 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async for key in client.scan_iter(match="*IncrArea*", count=2000):
        if key.startswith("counter:") and not _is_counter_key_recent(key, now):
            continue                      # outside retention: never backed up, by design
        if rows.get(key) != await client.hgetall(key):
            bad += 1
    return bad


async def purge_rows():
    for table in ("redis_counter_backup", "redis_timeseries_backup"):
        await db.execute(f"DELETE FROM {table} WHERE redis_key LIKE %s", ("%IncrArea%",))


async def main() -> int:
    h.stop("app")
    try:
        global_state.geofences = [{"name": a, "offset": o, "id": i, "coordinates": [[]]}
                                  for i, (a, o) in enumerate(AREAS.items(), 1) if o is not None]
        await db.init_db()
        rm = RedisManager()
        await rm.init_redis()
        client = await rm.check_redis_connection()
        rng = random.Random(3)
        await purge_rows()
        n = await seed(client, rng)
        print(f"seeded {n} keys over {len(AREAS)} areas (UTC+10, UTC-10, UTC+0, unknown)")

        since = time.time()
        t0 = time.perf_counter()
        c_full = await MySQLBackup.counters(client)
        t_full = await MySQLBackup.timeseries(client)
        d_full = time.perf_counter() - t0
        bad_full = await mismatches(client)
        print(f"full backup: {c_full} counter + {t_full} timeseries rows in {d_full:.1f}s, mismatches {bad_full}")

        await live_traffic(client, rng)
        t0 = time.perf_counter()
        c_inc = await MySQLBackup.counters(client, since_utc=since)
        t_inc = await MySQLBackup.timeseries(client, since_utc=since)
        d_inc = time.perf_counter() - t0
        bad_inc = await mismatches(client)
        print(f"incremental: {c_inc} counter + {t_inc} timeseries rows in {d_inc:.1f}s, mismatches {bad_inc}")

        await purge_rows()
        await client.flushdb()
        await rm.close_redis()
        await db.close_db()
        # counters are where the volume is (550k rows in production); timeseries
        # only skip idle series, so about half here
        ok = bad_full == 0 and bad_inc == 0 and c_inc * 10 < c_full and t_inc < t_full
        print(f"rows written: full {c_full + t_full} -> incremental {c_inc + t_inc}; time {d_full:.1f}s -> {d_inc:.1f}s")
        print("RESULT:", "PASS" if ok else "FAIL")
        return 0 if ok else 1
    finally:
        h.start("app")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
